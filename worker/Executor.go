package worker

import (
	"math/rand"
	"mycrontab/common"
	"os/exec"
	"time"
)

type Executor struct {
}

var G_executor *Executor

// 执行一个任务

func (e *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		var (
			cmd     *exec.Cmd
			err     error
			output  []byte
			result  *common.JobExecuteResult
			jobLock *JobLock
		)
		//
		// 任务结果
		result = &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化分布式锁
		jobLock = G_jobMgr.CreateJobLock(info.Job.Name)
		// 为了防止因为时间同步的不是很精确导致总是一个worker抢到所有任务,所以随机sleep 0到100毫秒
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		// 上锁
		err = jobLock.TryLock()
		defer jobLock.UnLock()
		if err != nil { // 上锁失败
			result.StartTime = time.Now()
			result.Err = err
			result.EndTime = time.Now()
		} else {
			result.StartTime = time.Now()
			// 生成要执行的shell命令对象
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)

			// 执行并且捕获输出
			output, err = cmd.CombinedOutput()
			result.Err = err
			result.Output = output
			result.EndTime = time.Now()
		}
		// 把任务完成后,把执行的结果返回给scheduler, scheduler会从executingTable中删除执行记录
		G_scheduler.PushJobResult(result)
	}()
}

func InitExecutor() (err error) {
	G_executor = &Executor{}
	return
}
