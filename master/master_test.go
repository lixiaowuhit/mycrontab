package master

import (
	"context"
	"fmt"
	"mycrontab/common"
	"net"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type person struct {
	Name string
	Age  int
}

type personBatch struct {
	pers []*person
}

func showTime() {
	fmt.Println(time.Now().Format("04:05"))
}

func TestMaster(t *testing.T) {
	sendChan := make(chan *person, 100)
	recvChan := make(chan *personBatch, 100)
	var pB *personBatch
	var pB2 *personBatch
	var afterFunc *time.Timer

	// 开启一个协程每秒往sendChan里发送一个person对象
	go func(c chan *person) {
		for i := 0; i < 100; i++ {
			c <- &person{Age: i}
			time.Sleep(time.Second)
		}
	}(sendChan)

	// 从sendChan里取出person对象添加到personSlice里,
	for i := 0; i <= 3; i++ {
		select {
		case p := <-sendChan:
			if pB == nil {
				pB = &personBatch{}

				// 5秒后往recvChan 里发送包含5个person数组
				afterFunc = time.AfterFunc(2*time.Second,
					//func(pb *personBatch) func() {
					//	return func() {
					//		//showTime()
					//		fmt.Printf("pb: %p | %v\n", pb, pb.pers)
					//		recvChan <- pb
					//		//fmt.Printf("pb: %p | %v\n", pb, pb.pers)
					//	}
					//}(pB),
					func() {
						fmt.Printf("pb: %p | %v\n", pB, pB.pers)
						recvChan <- pB
						//fmt.Printf("pb: %p | %v\n", pB, pB.pers)
					},
				)
			}
			pB.pers = append(pB.pers, p)
			if len(pB.pers) >= 3 {
				fmt.Println("提交任务~~~~~~", pB.pers)
				pB = nil
				afterFunc.Stop()
			}
		case pB2 = <-recvChan:
			showTime()
			if pB2 == pB {
				fmt.Println("提交任务", pB2.pers)
				pB = nil
			} else {
				fmt.Println("pB2 != pB, 已经提交")
			}
			fmt.Printf("pB2: %p | %v\n", pB2, pB2.pers)
		}
	}
	showTime()
	fmt.Printf("pB: %p | %v\n", pB, pB)
	fmt.Printf("pB2: %p | %v\n", pB2, pB2.pers)
	//if afterFunc != nil {
	//	afterFunc.Stop()
	//}
}

func Test2(t *testing.T) {
	var (
		clientOptions *options.ClientOptions
		client        *mongo.Client
		err           error
	)
	// mongodb连接配置
	clientOptions = options.Client().ApplyURI("mongodb://192.168.100.8:27017").
		SetConnectTimeout(1000 * time.Millisecond)
	// 建立连接,生成client
	if client, err = mongo.Connect(context.TODO(), clientOptions); err != nil {
		fmt.Println("mongo connect err:", err)
		return
	}
	collection := client.Database("cron").Collection("log")
	person := &struct {
		NaMe string `bson:"NaMe"`
		AGE  int    `bson:"AGE"`
	}{"tom", 99}

	insertOneResult, err := collection.InsertOne(context.TODO(), person)
	if err != nil {
		fmt.Println("insertOne failed, err:", err)
		return
	}
	fmt.Println(insertOneResult.InsertedID)
	//deleteResult, err := collection.DeleteMany(context.TODO(), struct{}{})
	//if err != nil {
	//	fmt.Println("删除错误:", err)
	//	return
	//}
	//fmt.Println("删除数目:", deleteResult.DeletedCount)
	cursor, err := collection.Find(context.TODO(), struct{}{})
	if err != nil {
		fmt.Println("cursor find err:", err)
		return
	}
	defer cursor.Close(context.TODO())
	for cursor.Next(context.TODO()) {
		log := &common.JobLog{}
		if err = cursor.Decode(log); err != nil {
			fmt.Println("cursor decode err:", err)
			return
		}
		fmt.Printf("%#v\n", *log)
	}
}

func Test3(t *testing.T) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println("InterfaceAddrs err:", err)
		return
	}
	fmt.Println("addrs have", len(addrs), "addr")
	for _, addr := range addrs {
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				fmt.Println(ipNet.IP.String(), ipNet.Network(), ipNet.String())
			}
		}
	}
}

// test4
func Test4(t *testing.T) {
	var sum int
	//time.AfterFunc(2*time.Second, func(n int) func() {
	//	return func() {
	//		n = 100
	//	}
	//}(sum))

	adder := func() func(int) int {
		return func(i int) int {
			sum += i
			return sum
		}
	}
	a := adder()
	for i := 0; i < 4; i++ {
		fmt.Println(a(i))
		time.Sleep(time.Second)
	}
}

func Test5(t *testing.T) {
	a := new(int)
	fmt.Println(*a)
	p := new(person)
	fmt.Println(p, *p)
	//s := new([]int)
	var s *[]int

	fmt.Printf("%p  %#v\n", *s, *s)
}

func Test6(t *testing.T) {
	for i := 0; i <= 10; i++ {
		fmt.Println("hahaha", i)
	}
}
