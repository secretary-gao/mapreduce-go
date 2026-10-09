package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskArgs struct {
}

type TaskReply struct {
	TaskType string // "map" / "reduce" / "wait" / "exit"
	TaskId   int    // 任务编号
	FileName string // map 任务要读的文件
	NReduce  int    // 分成几个桶
	NMap     int    // map 任务总数
}

type ReportArgs struct {
	TaskType string // 做完的是 map 还是 reduce
	TaskId   int    // 做完的任务编号
}

type ReportReply struct {
}
