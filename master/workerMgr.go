package master

import (
	"context"
	"fmt"
	"mycrontab/common"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var G_workerMgr *WorkerMgr

// 获取在线worker列表
func (w *WorkerMgr) ListWorkers() (workerArr []string, err error) {
	// 确保返回的不是nil
	workerArr = make([]string, 0)

	getResp, err := w.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix())
	if err != nil {
		fmt.Println("Kv Get workerArr err:", err)
		return
	}

	// 解析每个节点的ip
	for _, kv := range getResp.Kvs {
		// kv.Key: /cron/workers/ip_addr
		workerArr = append(workerArr, strings.TrimPrefix(string(kv.Key), common.JOB_WORKER_DIR))
	}
	return
}
func InitWorkerMgr() (err error) {
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

	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}

	return
}
