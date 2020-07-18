package worker

import (
	"context"
	"fmt"
	"mycrontab/common"
	"net"
	"time"

	"github.com/coreos/etcd/clientv3"
)

type Register struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease

	localIP string
}

var G_register *Register

// 获取第一个本地ipv4的非回环地址
func getLocalIP() (ipv4 string, err error) {
	// 获取所有网卡信息
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("InterfaceAddrs err:", err)
		return
	}

	// 取第一个非lo的IPV4网卡
	for _, addr := range addrs {
		// addr是一个接口,需要断言反解看是不是IP地址,有可能是unix sock
		ipNet, isIPNet := addr.(*net.IPNet)
		// 如果是IP地址,并且不是本地回环,并且是ipV4的接口,就符合条件,我们取第一个
		if isIPNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return
}

// 往etcd put /cron/workers/ip_addr 来注册,并且自动续租
func (r *Register) keepOnline() {
	var (
		err            error
		leaseGrantResp *clientv3.LeaseGrantResponse
		keepAliveChan  <-chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp  *clientv3.LeaseKeepAliveResponse
		putResp        *clientv3.PutResponse
		cancelCtx      context.Context
		cancelFunc     context.CancelFunc
	)
	// 注册的key
	regKey := common.JOB_WORKER_DIR + r.localIP + "  regTime:" + time.Now().
		Format("2006-01-02 15:04:05")

	// 注册过程中出错就重试,不停的注册,除非程序崩溃掉,所以用for 循环
	for {
		//cancelFunc = nil
		//fmt.Println("重新开始注册")
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())
		// 申请一个租约
		if leaseGrantResp, err = r.lease.Grant(cancelCtx, 5); err != nil {
			goto RETRY
		}
		// 自动续租
		if keepAliveChan, err = r.lease.KeepAlive(cancelCtx, leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		//cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		if putResp, err = r.kv.Put(cancelCtx, regKey, "",
			clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			fmt.Println("Register put err:", putResp.Header.String())
			goto RETRY
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveChan:
				//fmt.Println("keepAliveResp:", keepAliveResp.String())
				if keepAliveResp == nil {
					goto RETRY
				}
			}
		}
	RETRY:
		fmt.Println("注册错误:", err)
		if cancelFunc != nil {
			cancelFunc()
		}
		time.Sleep(time.Second)
	}
}

func InitRegister() (err error) {
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
	localIP, err := getLocalIP()
	if err != nil {
		fmt.Println("GetLocalIP err:", err)
		return
	}

	// 赋值单列
	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIP,
	}

	go G_register.keepOnline()
	return
}
