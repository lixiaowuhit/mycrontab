package main

import (
	"flag"
	"fmt"
	"mycrontab/worker"
	"runtime"
	"time"
)

var (
	conFile string // 配置文件路径
)

// 解析命令行参数
func initArgs() {
	flag.StringVar(
		&conFile, "config", "./worker.json", "指定json格式配置文件路径")
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
	)
	// 初始化命令行参数, 获取到conFile,即用什么配置文件启动程序
	initArgs()
	// 初始化线程数量
	initEnv()

	// 初始化配置文件
	if err = worker.InitConfig(conFile); err != nil {
		goto ERR
	}

	// 启动日志协程
	if err = worker.InitLogSink(); err != nil {
		fmt.Println("initLogSink err")
		goto ERR
	}

	// 初始化任务执行器
	if err = worker.InitExecutor(); err != nil {
		goto ERR
	}
	// 初始化并执行调度器
	if err = worker.InitScheduler(); err != nil {
		goto ERR
	}
	// 初始化任务管理器
	if err = worker.InitJobMgr(); err != nil {
		goto ERR
	}
	// 服务注册
	if err = worker.InitRegister(); err != nil {
		fmt.Println("InitRegister err:", err)
		goto ERR
	}

	for {
		time.Sleep(1 * time.Second)
	}

	return
ERR:
	fmt.Println(err)
}
