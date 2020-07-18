package worker

import (
	"context"
	"mycrontab/common"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/coreos/etcd/clientv3"
)

type JobMgr struct {
	client  *clientv3.Client
	kv      clientv3.KV
	lease   clientv3.Lease
	watcher clientv3.Watcher
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		client  *clientv3.Client
		config  clientv3.Config
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}
	if client, err = clientv3.New(config); err != nil {
		return
	}
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单列
	G_jobMgr = &JobMgr{
		client:  client,
		kv:      kv,
		lease:   lease,
		watcher: watcher,
	}

	// 启动任务监听
	if err = G_jobMgr.watchJobs(); err != nil {
		return
	}

	// 启动监听任务keiller
	if err = G_jobMgr.watchKiller(); err != nil {
		return
	}

	return
}

// 监视任务变化
func (j *JobMgr) watchJobs() (err error) {
	var (
		getResp            *clientv3.GetResponse
		kvpare             *mvccpb.KeyValue
		job                *common.Job
		jobEvent           *common.JobEvent
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		event              *clientv3.Event
		jobName            string
	)
	// 1, 首先get /cron/jobs/目录下的所有任务, 并且取到当前集群的revision
	if getResp, err = G_jobMgr.kv.Get(
		context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix()); err != nil {
		return
	}
	// 循环取出所有单个任务
	for _, kvpare = range getResp.Kvs {
		if job, err = common.UnpackJob(kvpare.Value); err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			// 同步给调度协程
			//fmt.Println(*jobEvent, jobEvent.Job)
			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 2, 获取已经存在的job后,继续监听后续job变化, 因为是阻塞的,所以新开一个协程
	go func() {
		// 从get时刻的后续版本开始监听变化
		watchStartRevision = getResp.Header.Revision + 1
		watchChan = j.watcher.Watch(context.TODO(),
			common.JOB_SAVE_DIR, clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 从watchChan 中获取监听事件,处理
		for watchResp = range watchChan {
			for _, event = range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT: // 新建或者修改任务
					// 拿到job
					if job, err = common.UnpackJob(event.Kv.Value); err != nil {
						continue
					}
					// 构建一个更新的event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
				case mvccpb.DELETE:
					jobName = common.ExtractJobName(string(event.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}
				// 把jobEvent变化推给scheduler
				//fmt.Println(*jobEvent, jobEvent.Job)
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return
}

func (j *JobMgr) watchKiller() (err error) {
	// 监听/cron/killer/目录
	var (
		watchChan clientv3.WatchChan
		watchResp clientv3.WatchResponse
		event     *clientv3.Event
		jobName   string
		jobEvent  *common.JobEvent
		job       *common.Job
	)
	go func() {

		watchChan = j.watcher.Watch(context.TODO(),
			common.JOB_KILLER_DIR, clientv3.WithPrefix())
		// 从watchChan 中获取监听事件,处理
		for watchResp = range watchChan {
			for _, event = range watchResp.Events {
				switch event.Type {
				case mvccpb.PUT: // 杀死某个任务的事件
					jobName = common.ExtractKillerName(string(event.Kv.Key))
					job = &common.Job{Name: jobName}
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_KILL, job)
					G_scheduler.PushJobEvent(jobEvent)
				case mvccpb.DELETE: // killer标记自动过期,不关心
				}

			}
		}
	}()
	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(jobName string) (jobLock *JobLock) {
	// 返回一把锁
	jobLock = InitJobLock(jobName, jobMgr.client, jobMgr.lease)
	return
}
