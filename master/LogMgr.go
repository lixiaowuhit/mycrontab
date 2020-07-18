package master

import (
	"context"
	"fmt"
	"mycrontab/common"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var G_logMgr *LogMgr

func InitLogMgr() (err error) {
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
	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"), // collection(表)
	}
	return
}

func (logMgr *LogMgr) ListLog(name string, skip, limit int) (
	logArr []*common.JobLog, err error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		jobLog  *common.JobLog
	)

	// 为了防止获取到0个数据后导致返回的logArr为nil,对logArr初始化为len为0的分配过地址的切片
	logArr = make([]*common.JobLog, 0)
	// 过滤条件
	filter = &common.JobLogFilter{JobName: name}
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	//limitOps := options.Find().SetLimit(int64(limit))
	//skipOps := options.Find().SetSkip(int64(skip))
	//sortOps := options.Find().SetSort(logSort)
	findOps := options.Find().SetLimit(int64(limit)).SetSkip(int64(skip)).SetSort(logSort)
	cursor, err = logMgr.logCollection.Find(
		context.TODO(), filter, findOps)

	//cursor, err = logMgr.logCollection.Find(
	//	context.TODO(), struct{}{})

	if err != nil {
		fmt.Println(err)
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog = &common.JobLog{}
		if err = cursor.Decode(jobLog); err != nil {
			fmt.Println(err)
			continue
		}

		logArr = append(logArr, jobLog)
	}
	return
}
