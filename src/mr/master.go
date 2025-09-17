package mr

import (
//	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"fmt"
)

type TaskStatus int
type WorkerStatus int
type TaskType int

const (
	Unassigned TaskStatus = iota
	Running
	Finished
)

const (
	WorkerIdle WorkerStatus = iota
	WorkerRunning
)

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
)

type Task struct {
	TaskId int
	Type   TaskType
	Status TaskStatus
	Splits []Split
	Worker EnrolledWorker
}

type EnrolledWorker struct {
	Id string
	Status WorkerStatus
	LastInteraction time.Time
}

type Split struct {
	Path  string
}

type Master struct {
	mutex       sync.Mutex
	mapTasks    []*Task
	reduceTasks []*Task
	workers     map[string]EnrolledWorker
	R           int
}

var waitTask = Task {
	TaskId: -1,
	Type: TaskTypeWait,
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (m *Master) findTask() (*Task, bool) {

	for _, t := range m.mapTasks {
		if t.Status == Unassigned {
			return t, true
		}
	}

	return nil, false
}


func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	m.onRPC(args.WorkerId)

	m.mutex.Lock()
	t, found := m.findTask()

	if !found {
		t = &waitTask
	} else {
		t.Status = Running
		t.Worker = m.workers[args.WorkerId]
	}

	m.mutex.Unlock()

	reply.TaskId = t.TaskId
	reply.TaskType = t.Type
	reply.Splits = t.Splits
	reply.R = m.R

	return nil
}

func (m *Master) onRPC(workerId string) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	w, exists := m.workers[workerId]
	if !exists {
		fmt.Printf("registering worker %s\n", workerId)
		m.workers[workerId] = EnrolledWorker{
			Id:     workerId,
			Status: WorkerIdle,
			LastInteraction: time.Now(),
		}
	} else {
		w.LastInteraction = time.Now()
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	fmt.Printf("files to process: %v \n", files)
	m.workers = make(map[string]EnrolledWorker)
	
	mapTasks := make([]*Task, len(files))
	for i, f := range files {
		mapTasks[i] = &Task{
			TaskId: i,
			Type:   TaskTypeMap,
			Status: Unassigned,
			Splits: []Split{{
				Path: f,
			}},
		}
	}
	m.mapTasks = mapTasks

	m.reduceTasks = make([]*Task, nReduce)

	m.R = nReduce

	m.server()
	return &m
}
