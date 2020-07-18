package worker

import (
	"context"
	"mycrontab/common"

	"github.com/coreos/etcd/clientv3"
)

type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string
	cancelFunc context.CancelFunc
	leaseID    clientv3.LeaseID
	isLocked   bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (jobLock *JobLock) {
	jobLock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}

// 尝试上锁
func (jobLock *JobLock) TryLock() (err error) {
	// 1, 申请一个5秒的租约
	grantResponse, err := jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return
	}
	// 用于取消自动续租的cantext对
	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	// 租约id
	leaseID := grantResponse.ID
	// 2, 自动续租
	keepAliveChan, err := jobLock.lease.KeepAlive(cancelCtx, leaseID)
	if err != nil {
		cancelFunc()                                  // 取消自动续租
		jobLock.lease.Revoke(context.TODO(), leaseID) // 立即释放租约
		return
	}

	// 3, 新起一个协程来处理自动续租事务
	go func() {
		for {
			select {
			case keepResp := <-keepAliveChan:
				if keepResp == nil {
					return
				}
			}
		}
	}()

	// 4, 事务抢锁
	txn := jobLock.kv.Txn(context.TODO())
	lockKey := common.JOB_LOCK_DIR + jobLock.jobName // 锁路径
	// 事务抢锁 如果创建Revison=0,也就是说没有被创建,则创建,否则读取
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))

	txnResp, err := txn.Commit()
	if err != nil {
		cancelFunc()                                  // 取消自动续租
		jobLock.lease.Revoke(context.TODO(), leaseID) // 立即释放租约
		return
	}
	if !txnResp.Succeeded { // 锁被占用
		err = common.ERR_LOCK_ALREADY_REQUIRED
		cancelFunc()                                  // 取消自动续租
		jobLock.lease.Revoke(context.TODO(), leaseID) // 立即释放租约
		return
	}
	// 抢锁成功
	jobLock.leaseID = leaseID
	jobLock.cancelFunc = cancelFunc
	jobLock.isLocked = true
	return
}

func (jobLock *JobLock) UnLock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseID)
	}
}
