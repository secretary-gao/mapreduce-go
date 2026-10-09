package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState struct {
	status    int       // 0 空闲, 1 进行中, 2 已完成
	startTime time.Time // 任务派出去的时间
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []TaskState
	reduceTasks []TaskState
}

func allDone(tasks []TaskState) bool {
	for _, t := range tasks {
		if t.status != 2 {
			return false
		}
	}
	return true
}
func pick(tasks []TaskState) int {
	for i := range tasks {
		t := &tasks[i]
		if t.status == 0 || (t.status == 1 && time.Since(t.startTime) > 10*time.Second) {
			t.status = 1
			t.startTime = time.Now()
			return i
		}
	}
	return -1
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = len(c.files)

	if !allDone(c.mapTasks) {
		if i := pick(c.mapTasks); i >= 0 {
			reply.TaskType = "map"
			reply.TaskId = i
			reply.FileName = c.files[i]
		} else {
			reply.TaskType = "wait"
		}
		return nil
	}

	if !allDone(c.reduceTasks) {
		if i := pick(c.reduceTasks); i >= 0 {
			reply.TaskType = "reduce"
			reply.TaskId = i
		} else {
			reply.TaskType = "wait"
		}
		return nil
	}

	reply.TaskType = "exit"
	return nil
}

func (c *Coordinator) ReportTask(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == "map" {
		c.mapTasks[args.TaskId].status = 2
	} else if args.TaskType == "reduce" {
		c.reduceTasks[args.TaskId].status = 2
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return allDone(c.reduceTasks)

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.mapTasks = make([]TaskState, len(files))
	c.reduceTasks = make([]TaskState, nReduce)

	c.server(sockname)
	return &c
}
