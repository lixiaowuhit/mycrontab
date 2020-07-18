package worker

import (
	"context"
	"fmt"
	"mycrontab/common"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogSink struct {
	client         *mongo.Client
	logCollection  *mongo.Collection
	logChan        chan *common.JobLog
	autoCommitChan chan *common.LogBatch
}

var G_logSink *LogSink

func (logSink *LogSink) saveLogs(batch *common.LogBatch) {
	_, err := logSink.logCollection.InsertMany(context.TODO(), batch.Logs)
	if err != nil {
		fmt.Println("insertMany err:", err)
		return
	}
}

// 日志处理协程
func (logSink *LogSink) writeLoop() {
	var (
		log          *common.JobLog
		logBatch     *common.LogBatch
		commitTimer  *time.Timer
		timeoutBatch *common.LogBatch
	)
	for {
		select {
		case log = <-logSink.logChan:
			// 每次插入需要等待mongodb的一次请求往返,耗时较长,所以定义一个批次窗口
			// 如果logBatch为空,初始化一个新的Batch,让这个批次定时未到阈值也提交
			if logBatch == nil {
				logBatch = &common.LogBatch{}
				// 启动一个定时器
				commitTimer = time.AfterFunc(
					time.Duration(G_config.JobLogCommitTimeout)*time.Millisecond,
					//func() {
					//	// 发出超时通知,不要直接提交这个logbatch,防止多个协程并发问题
					//	logSink.autoCommitChan <- logBatch
					//},
					func(batch *common.LogBatch) func() {
						return func() {
							logSink.autoCommitChan <- batch
						}
					}(logBatch),
				)
			}
			// 把新日志追加到批次中
			logBatch.Logs = append(logBatch.Logs, log)
			// 如果批次满了,就立即发送日志,并且清除batch
			if len(logBatch.Logs) >= G_config.JobLogBatchSize {
				logSink.saveLogs(logBatch) // 发送日志
				logBatch = nil             // 批次清空
				commitTimer.Stop()         // 定时器关闭
			}
		case timeoutBatch = <-logSink.autoCommitChan:
			// 判断过期批次是不是当前批次,有可能timer触发发送timeoutBatch到管道,但下一个select还是
			// 走的上一个case,导致批次满了,在上一个case中把logBatch保存后置为nil了,这时候就不能重复提交
			if timeoutBatch == logBatch {
				logSink.saveLogs(logBatch)
				logBatch = nil
			}
		}

	}
}

func InitLogSink() (err error) {
	var (
		clientOptions *options.ClientOptions
		client        *mongo.Client
	)
	// mongodb连接配置
	clientOptions = options.Client().ApplyURI(G_config.MongoDBURI).
		SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout) * time.Millisecond)

	// 建立连接,生成client
	if client, err = mongo.Connect(context.TODO(), clientOptions); err != nil {
		return
	}
	// 初始化单例
	G_logSink = &LogSink{
		client:         client,
		logCollection:  client.Database("cron").Collection("log"), // collection(表)
		logChan:        make(chan *common.JobLog, 1000),
		autoCommitChan: make(chan *common.LogBatch, 1000),
	}
	// 启动一个协程来处理日志存储
	go G_logSink.writeLoop()
	return
}

// 发送日志
func (logSink *LogSink) Append(jobLog *common.JobLog) {
	// 因为只有一个线程往mongodb存日志,存在日志产生的多,存储的慢的可能,会导致logChan缓存不够,程序阻塞,所以
	select {
	case logSink.logChan <- jobLog:
	default: // 如果logChan满了,会走default丢弃日志,保证主线程不阻塞
	}
}
