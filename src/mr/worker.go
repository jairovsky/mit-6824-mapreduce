package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	reducef func(string, []string) string) {

	workerId := strconv.Itoa(rand.Int())

	for {
		task := askForWork(workerId)
		log.Printf("got task %v", task)

		if task == nil {
			log.Printf("received a nil task. Exiting...")
			os.Exit(0)
		}

		if task.TaskType == TaskTypeWait {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if task.TaskType == TaskTypeMap {
			runMapTask(workerId, task, mapf)
		}

		if task.TaskType == TaskTypeReduce {
			runReduceTask(workerId, task, reducef)
		}
	}
}

func runMapTask(workerId string,
	task *AssignTaskReply,
	mapf func(string, string) []KeyValue) {

	outputFiles := createTaskOutputTempFiles(task.R)

	mapFuncResults := make([]KeyValue, 0)
	for _, split := range task.Splits {
		mapFuncResults = append(
			mapFuncResults,
			mapf(split.Path, readFile(split.Path))...,
		)
	}

	fmt.Printf("generated %d keys\n", len(mapFuncResults))

	for _, r := range mapFuncResults {
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
		err = os.Rename(f.Name(), finalFileName)
		if err != nil {
			log.Fatalf("can't rename file")
		}
		splits[i].Path = finalFileName
	}

	completeTask(workerId, task.TaskId, task.TaskType, splits)
}

func runReduceTask(workerId string,
	task *AssignTaskReply,
	reducef func(string, []string) string) {

	outputFile := createTaskOutputTempFile()

	mapResultsByKey := make(map[string][]string)
	for _, split := range task.Splits {
		keyValues := readKeyValues(split.Path)
		for _, kv := range keyValues {
			value, exists := mapResultsByKey[kv.Key]
			if !exists {
				value = make([]string, 0)
			}
			mapResultsByKey[kv.Key] = append(value, kv.Value)
		}
	}

	reduceResults := make([]KeyValue, 0)
	for k, v := range mapResultsByKey {
		kv := KeyValue{}
		kv.Key = k
		kv.Value = reducef(k, v)
		reduceResults = append(reduceResults, kv)

	}
	sort.Sort(ByKey(reduceResults))
	for _, rr := range reduceResults {
		outputFile.WriteString(fmt.Sprintf("%s %s\n", rr.Key, rr.Value))
	}
	err := outputFile.Close()
	if err != nil {
		log.Fatal("can't close file")
	}
	os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%d", task.TaskId))

	splits := make([]Split, 0)

	completeTask(workerId, task.TaskId, task.TaskType, splits)
}

func createTaskOutputTempFiles(nFiles int) []*os.File {
	tmpfiles := make([]*os.File, nFiles)
	for i := 0; i < nFiles; i++ {
		tmpfiles[i] = createTaskOutputTempFile()
	}

	return tmpfiles
}

func createTaskOutputTempFile() *os.File {
	tmpf, err := ioutil.TempFile("", "mr-temp")
	if err != nil {
		log.Fatalf("can't create temp file")
	}
	return tmpf
}

func readFile(path string) string {
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", path)
	}
	return string(content)
}

func readKeyValues(path string) []KeyValue {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("cannot open %v", path)
	}

	keyValues := make([]KeyValue, 0)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Bytes()
		var kv KeyValue
		json.Unmarshal(line, &kv)
		keyValues = append(keyValues, kv)
	}
	return keyValues
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

func completeTask(
	workerId string,
	taskId int,
	taskType TaskType,
	splits []Split,
) bool {

	args := CompleteTaskArgs{
		WorkerId: workerId,
		TaskType: taskType,
		TaskId:   taskId,
		Splits:   splits,
	}

	reply := new(interface{})

	completed := call("Master.CompleteTask", &args, &reply)
	if !completed {
		log.Println("Error while trying to send CompleteTask")
	}

	return completed
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
