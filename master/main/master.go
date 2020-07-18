package main

import (
	"flag"
	"fmt"
	"mycrontab/master"
	"runtime"
	"sync"
)

var (
	conFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	flag.StringVar(
		&conFile, "config", "./master.json", "指定json格式配置文件路径")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	numCpu := runtime.NumCPU()
	runtime.GOMAXPROCS(numCpu)
}

func main() {
	var (
		err error
		wg  sync.WaitGroup
	)
	// 初始化命令行参数, 获取到conFile,即用什么
	initArgs()

	//  初始化线程数
	initEnv()

	// 根据initArgs获取到的配置文件目录加载配置文件
	if err = master.InitConfig(conFile); err != nil {
		goto ERR
	}

	// 初始化日志管理器
	if err = master.InitLogMgr(); err != nil {
		goto ERR
	}
	// 初始化任务管理器
	if err = master.InitJobMgr(); err != nil {
		goto ERR
	}
	// 初始化worker节点管理器, 服务发现
	if err = master.InitWorkerMgr(); err != nil {
		goto ERR
	}

	// 初始化ApiServer
	wg.Add(1)
	if err = master.InitApiServer(); err != nil {
		goto ERR
	}

	// 循环不退出程序
	//for {
	//	time.Sleep(1 * time.Second)
	//}
	wg.Wait()
	return
ERR:
	fmt.Println(err)
}
