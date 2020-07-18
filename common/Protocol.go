package common

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

type Job struct {
	Name     string `json:"name"`
	Command  string `json:"command"`
	CronExpr string `json:"cronExpr"`
}

// http应答响应结构体
type Response struct {
	Errno int         `json:"errno"`
	Msg   string      `json:"msg"`
	Data  interface{} `json:"data"`
}

func BuildResponse(errno int, msg string, data interface{}) (resp []byte, err error) {
	var response Response
	response.Errno = errno
	response.Msg = msg
	response.Data = data
	resp, err = json.Marshal(response)
	return
}

// 从json反序列化Job
func UnpackJob(value []byte) (job *Job, err error) {
	job = &Job{}
	err = json.Unmarshal(value, job)
	return
}

// job变化的事件
type JobEvent struct {
	EventType int // save 1, delete 2
	Job       *Job
}

//任务事件变化有两种, 1, 更新添加任务, 2 删除任务
func BuildJobEvent(eventType int, job *Job) (jobEvent *JobEvent) {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// 任务执行结果
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo
	Output      []byte
	Err         error
	StartTime   time.Time
	EndTime     time.Time
}

// 从etcd的key中提取任务名 /cron/jobs/job1 -> job1
func ExtractJobName(key string) string {
	return strings.TrimPrefix(key, JOB_SAVE_DIR)
}

// etcd中的key中提取要killer的任务名 /corn/killer/job1 ->job1
func ExtractKillerName(key string) string {
	return strings.TrimPrefix(key, JOB_KILLER_DIR)
}

// 任务的计划表
type JobSchedulePlan struct {
	Job      *Job                 // 要调度的任务
	Expr     *cronexpr.Expression // 解析后的任务的cronExpr表达式
	NextTime time.Time
}

// 构造任务执行计划
func BuildJobSchedulePlan(job *Job) (jobSchedulePlan *JobSchedulePlan, err error) {
	var expr *cronexpr.Expression

	// 解析任务的cron表达式
	if expr, err = cronexpr.Parse(job.CronExpr); err != nil {
		fmt.Println("Parse cronExpr err:", err)
		return
	}
	// 生成 任务调度计划表,返回
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

type JobExecuteInfo struct {
	Job        *Job
	PlanTime   time.Time
	RealTime   time.Time
	CancelCtx  context.Context    // 用于取消任务执行的context上下文
	CancelFunc context.CancelFunc // 用于取消任务上下文context的func
}

func BuildJobExecuteInfo(plan *JobSchedulePlan) (jobExecuteInfo *JobExecuteInfo) {
	jobExecuteInfo = &JobExecuteInfo{
		Job:      plan.Job,
		PlanTime: plan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 任务执行日志
type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"` // 任务名
	Command      string `json:"command" bson:"command"`
	Err          string `json:"err" bson:"err"`
	Output       string `json:"output" bson:"output"`
	PlanTime     int64  `json:"planTime" bson:"planTime"`         // 计划开始时间
	ScheduleTime int64  `json:"scheduleTime" bson:"scheduleTime"` // 实际调度时间, 如何和PlanTime差距大,schedule繁忙
	StartTime    int64  `json:"startTime" bson:"startTime"`       // 开始执行事件
	EndTIme      int64  `json:"endTime" bson:"endTime"`           // 任务执行结束事件
}

type LogBatch struct {
	Logs []interface{} // 多条日志
}

// 任务日志查询过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

// 任务日志排序规则
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` // 按startTime: -1
}
