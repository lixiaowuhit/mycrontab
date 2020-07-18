package master

import (
	"context"
	"encoding/json"
	"fmt"
	"mycrontab/common"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"

	"github.com/coreos/etcd/clientv3"
)

type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_jobMgr *JobMgr
)

func InitJobMgr() (err error) {
	var (
		client *clientv3.Client
		config clientv3.Config
		kv     clientv3.KV
		lease  clientv3.Lease
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
	G_jobMgr = &JobMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return
}

func (j *JobMgr) SaveJob(job *common.Job) (oldJob *common.Job, err error) {
	// 把任务保存到etcd的/cron/jobs/任务名 value是json
	var (
		jobKey    string
		jobValue  []byte
		putResp   *clientv3.PutResponse
		oldJobObj common.Job
	)
	jobKey = common.JOB_SAVE_DIR + job.Name

	if jobValue, err = json.Marshal(job); err != nil {
		return
	}
	if putResp, err = j.kv.Put(context.TODO(), jobKey, string(jobValue),
		clientv3.WithPrevKV()); err != nil {
		fmt.Printf("kv Put err:%v\n", err.Error())
		return
	}
	fmt.Println("KV put success:", putResp.Header.String())
	if putResp.PrevKv != nil {
		if err = json.Unmarshal(putResp.PrevKv.Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

func (j *JobMgr) DeleteJob(name string) (oldJob *common.Job, err error) {
	var (
		jobKey    string
		delResp   *clientv3.DeleteResponse
		oldJobObj common.Job
	)
	// etcd中保存任务的key
	jobKey = common.JOB_SAVE_DIR + name
	if delResp, err = j.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV()); err != nil {
		return
	}

	if len(delResp.PrevKvs) != 0 {
		if err = json.Unmarshal(delResp.PrevKvs[0].Value, &oldJobObj); err != nil {
			err = nil
			return
		}
		oldJob = &oldJobObj
	}

	return
}

func (j *JobMgr) ListJobs() (jobList []*common.Job, err error) {
	var (
		dirKey  string
		getResp *clientv3.GetResponse
		kvPair  *mvccpb.KeyValue
		job     *common.Job
	)
	jobList = make([]*common.Job, 0)
	dirKey = common.JOB_SAVE_DIR
	if getResp, err = j.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix()); err != nil {
		return
	}
	for _, kvPair = range getResp.Kvs {
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			fmt.Println(err)
			err = nil
			continue
		}
		jobList = append(jobList, job)
	}
	return
}

func (j *JobMgr) KillJob(name string) (err error) {
	// 往etcd中添加/cron/killer/任务名, 所有worker实时监听
	var (
		killKey        string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseID        clientv3.LeaseID
	)
	// 往etcd中添加的key
	killKey = common.JOB_KILLER_DIR + name
	// 创建一个1秒自动过期的租约,以便worker监听到强杀任务后能自动删除
	if leaseGrantResp, err = G_jobMgr.lease.Grant(context.TODO(), 1); err != nil {
		return
	}

	leaseID = leaseGrantResp.ID
	// 真正往etcd中putkiller标记
	if _, err = G_jobMgr.kv.Put(
		context.TODO(), killKey, "", clientv3.WithLease(leaseID)); err != nil {
		return
	}
	return
}
