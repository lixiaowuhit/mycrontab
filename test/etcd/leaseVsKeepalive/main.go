package main

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
)

func main() {
	timeFmt := "04:05"
	config := clientv3.Config{
		Endpoints:   []string{"192.168.100.8:2379"},
		DialTimeout: time.Second,
	}
	client, err := clientv3.New(config)
	if err != nil {
		fmt.Println("client创建错误,err:", err)
		return
	}
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	leaseGrantResponse, err := lease.Grant(context.TODO(), 2)
	if err != nil {
		fmt.Println("租约申请错误,err:", err)
		return
	}
	leaseID := leaseGrantResponse.ID
	timeoutCtx, _ := context.WithTimeout(context.TODO(), 10*time.Second)
	keepAliveChan, err := lease.KeepAlive(timeoutCtx, leaseID)
	if err != nil {
		fmt.Println("自动续租错误, err:", err)
		return
	}
	fmt.Println(keepAliveChan)
	//go func() {
	//	for {
	//		select {
	//		case keepAliveResp := <-keepAliveChan:
	//			if keepAliveResp == nil {
	//				fmt.Println(time.Now().Format(timeFmt)+":", "自动续租已经失效了")
	//				return
	//			} else {
	//				fmt.Println(time.Now().Format(timeFmt)+":", "收到自动续租应答:", keepAliveResp.ID)
	//			}
	//		}
	//	}
	//}()

	putResponse, err := kv.Put(context.TODO(),
		"/cron/lock/job10", "", clientv3.WithLease(leaseID))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(time.Now().Format(timeFmt)+":", "写入成功:", putResponse.Header.Revision)

	for {
		getResp, err := kv.Get(context.TODO(), "/cron/lock/job10")
		if err != nil {
			fmt.Println(err)
			return
		}
		if getResp.Count == 0 {
			fmt.Println(time.Now().Format(timeFmt)+":", "kv过期了")
			return
		}
		fmt.Println(time.Now().Format(timeFmt)+":", "还没过期:", getResp.Kvs)
		time.Sleep(2 * time.Second)
	}

}
