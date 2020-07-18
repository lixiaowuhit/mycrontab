package worker

import (
	"fmt"
	"mycrontab/common"
	"time"
)

type Scheduler struct {
	jobEventChan      chan *common.JobEvent              // etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan // 任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  //  任务执行状态表
	jobResultChan     chan *common.JobExecuteResult
}

var (
	G_scheduler *Scheduler
)

// 把任务变化推送给Scheduler
func (s *Scheduler) PushJobEvent(j *common.JobEvent) {
	s.jobEventChan <- j
}

// 处理任务变化的handle
func (s *Scheduler) handleJobEvent(j *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		err             error
		jobExecuteInfo  *common.JobExecuteInfo
		//jobExisted      bool
		jobExecuting bool
	)

	switch j.EventType {
	case common.JOB_EVENT_SAVE: // 保存任务事件
		if jobSchedulePlan, err = common.BuildJobSchedulePlan(j.Job); err != nil {
			fmt.Println("BuildJobSchedulePlan failed, err:", err)
			return
		}
		s.jobPlanTable[j.Job.Name] = jobSchedulePlan
	case common.JOB_EVENT_DELETE: // 删除任务事件
		delete(s.jobPlanTable, j.Job.Name)
	case common.JOB_EVENT_KILL: // 强杀任务事件
		// 取消Command的执行,先判断任务是否在执行中
		if jobExecuteInfo, jobExecuting = s.jobExecutingTable[j.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc()
		}
	}
}

// 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobSchedulePlan *common.JobSchedulePlan) {
	// 调度和执行是2件事情,调度是找出到期了要执行的任务,执行是真的执行
	// 假设任务每分钟执行60次,但有一次执行用了2分钟,则执行期间不会被重复调度
	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	// 如果任务正在执行, 跳过,本次不执行
	if _, jobExecuting = scheduler.jobExecutingTable[jobSchedulePlan.Job.Name]; jobExecuting {
		fmt.Println("任务正在执行,跳过执行", jobSchedulePlan.Job.Name)
		return
	}

	// 构建执行状态信息 并且保存
	jobExecuteInfo = common.BuildJobExecuteInfo(jobSchedulePlan)
	scheduler.jobExecutingTable[jobSchedulePlan.Job.Name] = jobExecuteInfo

	// 执行任务
	fmt.Println("执行任务:", jobExecuteInfo.Job.Name,
		jobExecuteInfo.PlanTime.Format("04:05"),
		jobExecuteInfo.RealTime.Format("04:05"))
	G_executor.ExecuteJob(jobExecuteInfo)
}

// 调度任务并计算出下次调度的时间间隔
func (scheduler *Scheduler) TrySchedule() (scheduleAfter time.Duration) {
	var (
		now             time.Time
		jobSchedulePlan *common.JobSchedulePlan
		nearNext        *time.Time
	)

	if len(scheduler.jobPlanTable) == 0 {
		scheduleAfter = 1 * time.Second
		return
	}
	// 当前时间
	now = time.Now()

	// 遍历所有任务
	for _, jobSchedulePlan = range scheduler.jobPlanTable {
		if jobSchedulePlan.NextTime.Before(now) || jobSchedulePlan.NextTime.Equal(now) {
			// 尝试执行任务
			//fmt.Println("执行的任务是:", jobSchedulePlan.Job.Name)
			scheduler.TryStartJob(jobSchedulePlan)
			//刷新下次执行时间
			jobSchedulePlan.NextTime = jobSchedulePlan.Expr.Next(now)
		}
		// 统计最近一个要调度的任务的时间
		if nearNext == nil || jobSchedulePlan.NextTime.Before(*nearNext) {
			nearNext = &jobSchedulePlan.NextTime
		}
	}
	// 下次要调度的间隔
	scheduleAfter = (*nearNext).Sub(now)
	return
}

// 处理任务结果
func (s *Scheduler) handleJobResult(result *common.JobExecuteResult) {
	var (
		jobLog *common.JobLog
	)
	// 删除执行状态
	delete(s.jobExecutingTable, result.ExecuteInfo.Job.Name)
	// 处理执行结果
	//fmt.Println("任务完成",result.ExecuteInfo.Job.Name, string(result.Output),result.Err)
	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog = &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixNano() / 1000 / 1000,
			ScheduleTime: result.ExecuteInfo.RealTime.UnixNano() / 1000 / 1000,
			StartTime:    result.StartTime.UnixNano() / 1000 / 1000,
			EndTIme:      result.EndTime.UnixNano() / 1000 / 1000,
		}
		if result.Err == nil {
			jobLog.Err = ""
		} else {
			jobLog.Err = result.Err.Error()
		}
		//  把日志存到mongoDB
		G_logSink.Append(jobLog)
	}
}

// 调度协程
func (s *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)
	// 初始化一次, scheduleAfter设置为1秒
	scheduleAfter = s.TrySchedule()

	// 调度的延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务common.Job
	for {
		select {
		case jobEvent = <-s.jobEventChan: // 监听任务变化时间
			// 对内存中维护中的任务列表进行增删改查
			s.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 最近的任务到期需要执行了了
		case jobResult = <-s.jobResultChan: // 监听任务执行结果
			s.handleJobResult(jobResult)
		}
		// 上边无论是监听到任务变化了,还是有任务到期要执行了,都再调度依次任务
		scheduleAfter = s.TrySchedule()
		scheduleTimer = time.NewTimer(scheduleAfter)
	}
}

func InitScheduler() (err error) {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),        // 任务事件管道
		jobPlanTable:      make(map[string]*common.JobSchedulePlan), // 任务计划表
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}
	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return
}

// 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(jobResult *common.JobExecuteResult) {
	scheduler.jobResultChan <- jobResult
}
