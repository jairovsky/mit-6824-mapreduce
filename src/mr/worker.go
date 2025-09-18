package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"math/rand"
	"strconv"
	"time"
	"io/ioutil"
	"os"
	"encoding/json"
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

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,) {

	workerId := strconv.Itoa(rand.Int())
	
	for {
		task := askForWork(workerId)
		fmt.Printf("got task %v\n", task)
		
		if task == nil || task.TaskType == TaskTypeWait {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if task.TaskType == TaskTypeMap {
			runMapTask(workerId, task, mapf)
		}

		if task.TaskType == TaskTypeReduce {
			
		}
	}
}

func runMapTask(workerId string,
	            task *AssignTaskReply,
				mapf func(string, string) []KeyValue,) {

	outputFiles := createTaskOutputTempFiles(task.R)
	fmt.Printf("temp files created %v \n", outputFiles)

	mapFuncResults := make([]KeyValue, 0)
	for _, split := range task.Splits {
		mapFuncResults = append(
			mapFuncResults,
			mapf(split.Path, readFile(split.Path))...,
		)
	}

	fmt.Printf("generated %d keys\n", len(mapFuncResults))

	for _,r := range mapFuncResults {
		p := choosePartitionRegion(r.Key, task.R)
		
		payload, err := json.Marshal(r)
		payload = append(payload, '\n')
		if err != nil {
			log.Fatalf("can't encode json")
		}
		
		_, err = outputFiles[p].Write(payload)
		if err != nil {
			log.Fatalf("can't write json to file")
		}
	}

	splits := make([]Split, task.R)
	for i, f := range outputFiles {
		err := f.Close()
		if err != nil {
			log.Fatalf("can't close file")
		}
		
		finalFileName := fmt.Sprintf("mr-map-%d-%d", task.TaskId, i)
		os.Rename(f.Name(), finalFileName)
		splits[i].Path = finalFileName
	}
}

func createTaskOutputTempFiles(nFiles int) []*os.File {
	tmpfiles := make([]*os.File, nFiles)
	for i := 0; i < nFiles; i++ {
		tmpf, err := ioutil.TempFile("", "mr-map-output")
		if err != nil {
			log.Fatalf("can't create temp file")
		}
		tmpfiles[i] = tmpf
	}

	return tmpfiles
}

func readFile(path string) string {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", path)
	}
	file.Close()
	return string(content)
}

func choosePartitionRegion(key string, R int) int {
	return ihash(key) % R
}

func askForWork(workerId string) *AssignTaskReply {

	args := AssignTaskArgs{}
	args.WorkerId = workerId
	
	reply := AssignTaskReply{}

	success := call("Master.AssignTask", &args, &reply)

	if success {
		return &reply
	}
	
	return nil
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
