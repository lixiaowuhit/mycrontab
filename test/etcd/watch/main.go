package main

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
)

func main() {
	//timeFmt := "04:05"
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
	go func() {
		for {
			time.Sleep(time.Second)
			_, err2 := kv.Put(context.TODO(), "/cron/jobs/job100", "I am job100")
			if err2 != nil {
				fmt.Println("put err:", err)
			}
			_, err2 = kv.Delete(context.TODO(), "/cron/jobs/job100")
			if err2 != nil {
				fmt.Println("put err:", err)
			}
			time.Sleep(time.Second)
		}
	}()
	getResponse, err := kv.Get(context.TODO(), "/cron/jobs/job100")
	if err != nil {
		fmt.Println(err)
		return
	}
	if getResponse.Count != 0 {
		fmt.Println("当前值是:", string(getResponse.Kvs[0].Value))
	}
	revision := getResponse.Header.Revision
	fmt.Println("当前Revision是:", revision)
	watcher := clientv3.NewWatcher(client)
	watchChan := watcher.Watch(
		context.TODO(), "/cron/jobs/job100", clientv3.WithRev(revision+1))
	for watchResponse := range watchChan {
		for _, event := range watchResponse.Events {
			fmt.Println(event)
			fmt.Println(event.Type.String())
		}
	}
}
