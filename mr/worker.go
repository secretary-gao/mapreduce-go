package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		reply := TaskReply{}
		ok := call("Coordinator.GetTask", &TaskArgs{}, &reply)
		if !ok {
			return
		}

		switch reply.TaskType {
		case "map":
			doMap(mapf, reply)
			report("map", reply.TaskId)
		case "reduce":
			doReduce(reducef, reply)
			report("reduce", reply.TaskId)
		case "wait":
			time.Sleep(time.Second)
		case "exit":
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}
func doMap(mapf func(string, string) []KeyValue, task TaskReply) {
	// 读文件、调用 mapf
	content, err := os.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	kva := mapf(task.FileName, string(content))
	// 分桶
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReduce
		buckets[r] = append(buckets[r], kv)
	}
	// 每个桶写成临时文件,再改名
	for r := 0; r < task.NReduce; r++ {
		tmp, err := os.CreateTemp(".", "mr-tmp-")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range buckets[r] {
			enc.Encode(&kv)
		}
		tmp.Close()
		os.Rename(tmp.Name(), fmt.Sprintf("mr-%d-%d", task.TaskId, r))
	}
}

func doReduce(reducef func(string, []string) string, task TaskReply) {
	// 读所有中间文件
	var kva []KeyValue
	for i := 0; i < task.NMap; i++ {
		f, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.TaskId))
		if err != nil {
			log.Fatalf("cannot open mr-%d-%d", i, task.TaskId)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		f.Close()
	}
	//排序
	sort.Slice(kva, func(a, b int) bool { return kva[a].Key < kva[b].Key })
	//分组、调用 reducef、写文件
	tmp, err := os.CreateTemp(".", "mr-tmp-")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	i := 0
	for i < len(kva) {
		j := i
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tmp.Close()
	os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", task.TaskId))
}

func report(taskType string, taskId int) {
	args := ReportArgs{TaskType: taskType, TaskId: taskId}
	reply := ReportReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
