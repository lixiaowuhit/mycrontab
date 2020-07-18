package master

import (
	"encoding/json"
	"fmt"
	"mycrontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

func handleJobSave(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	if err = r.ParseForm(); err != nil { // 1, 解析post表单
		goto ERR
	}

	postJob = r.PostForm.Get("job") // 2, 拿到表单中的job字段
	// 3, 反序列化拿到的job
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}
	// 4, 保存到etcd
	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		fmt.Println(err)
		goto ERR
	}
	// 5, 返回正常应答
	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
		return
	}

ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

// 删除任务接口 POST /job/delete name=job1
func handleJobDelete(w http.ResponseWriter, r *http.Request) {
	var (
		err    error
		name   string
		bytes  []byte
		oldJob *common.Job
	)
	err = r.ParseForm()
	if err != nil {
		goto ERR
	}
	name = r.PostForm.Get("name")

	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", oldJob); err == nil {
		w.Write(bytes)
		return
	}
ERR:
	if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func InitApiServer() (err error) {
	var (
		listener      net.Listener
		httpServer    *http.Server
		mux           *http.ServeMux
		staticDir     http.Dir
		staticHandler http.Handler
	)

	// 定义动态api路由handler
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handlerWorkerList)
	// 静态文件目录
	staticDir = http.Dir(G_config.Webroot)
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	if listener, err = net.Listen(
		"tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		Handler:      mux,
	}
	G_apiServer = &ApiServer{httpServer: httpServer}

	go httpServer.Serve(listener)
	return
}

// 获取健康节点列表
func handlerWorkerList(w http.ResponseWriter, r *http.Request) {
	workerArr, err := G_workerMgr.ListWorkers()
	if err != nil {
		fmt.Println(err)
		if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
			w.Write(bytes)
		}
		return
	}
	if bytes, err := common.BuildResponse(0, "success", workerArr); err == nil {
		w.Write(bytes)
		return
	}
}

// 处理job日志
func handleJobLog(w http.ResponseWriter, r *http.Request) {
	var (
		err        error
		bytes      []byte
		name       string // 任务名
		skipParam  string //从第几条开始
		limitParam string // 返回多少条
		skip       int
		limit      int
		logArr     []*common.JobLog
	)
	if err = r.ParseForm(); err != nil {
		goto ERR
	}
	// 获取请求参数get  /job/log?name=job1&skip=0&limit=10
	name = r.Form.Get("name")
	skipParam = r.Form.Get("skip")
	limitParam = r.Form.Get("limit")
	if skip, err = strconv.Atoi(skipParam); err != nil {
		//goto ERR
		skip = 0
	}
	if limit, err = strconv.Atoi(limitParam); err != nil {
		//goto ERR
		limit = 20
	}
	if logArr, err = G_logMgr.ListLog(name, skip, limit); err != nil {
		goto ERR
	}

	if bytes, err = common.BuildResponse(0, "success", logArr); err == nil {
		w.Write(bytes)
		return
	}
ERR:
	fmt.Println(err)
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}

func handleJobKill(writer http.ResponseWriter, request *http.Request) {
	var (
		err   error
		bytes []byte
		name  string
	)
	if err = request.ParseForm(); err != nil {
		goto ERR
	}
	name = request.PostForm.Get("name")
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", nil); err == nil {
		writer.Write(bytes)
		return
	}
ERR:
	if bytes, err = common.BuildResponse(-1, err.Error(), nil); err == nil {
		writer.Write(bytes)
	}
}

// 列举所有crontab
func handleJobList(w http.ResponseWriter, r *http.Request) {
	var (
		err     error
		jobList []*common.Job
		bytes   []byte
	)
	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildResponse(0, "success", jobList); err == nil {
		w.Write(bytes)
		return
	}

ERR:
	if bytes, err := common.BuildResponse(-1, err.Error(), nil); err == nil {
		w.Write(bytes)
	}
}
