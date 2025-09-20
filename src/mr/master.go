package mr

import (
	//	"io"
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int
type WorkerStatus int
type TaskType int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
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
	TaskId          int
	Type            TaskType
	Status          TaskStatus
	Splits          []Split
	LastInteraction time.Time
	WorkerId        string
}

type EnrolledWorker struct {
	Id              string
	Status          WorkerStatus
	LastInteraction time.Time
}

type Split struct {
	Path string
}

type Master struct {
	// R is, in the paper's terminology, the number of concurrent reduce tasks
	R                    int
	mapTasks             []*Task
	mapTasksCompleted    int
	reduceTasks          []*Task
	reduceTasksCompleted int
	workers              map[string]*EnrolledWorker
	mutex                sync.Mutex
}

var waitTask = Task{
	TaskId: -1,
	Type:   TaskTypeWait,
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

func (m *Master) findTask(workerId string) (*Task, bool) {

	if !m.allTasksCompleted(TaskTypeMap) {
		return findIdleTask(workerId, m.mapTasks)
	}

	if !m.allTasksCompleted(TaskTypeReduce) {
		return findIdleTask(workerId, m.reduceTasks)
	}

	return nil, false
}

func findIdleTask(workerId string, tasks []*Task) (*Task, bool) {

	for _, t := range tasks {
		if t.WorkerId == workerId {
			return nil, false
		}
	}

	for _, t := range tasks {
		if t.Status == Idle {
			return t, true
		}
	}

	return nil, false
}

func (m *Master) allTasksCompleted(taskType TaskType) bool {

	if taskType == TaskTypeMap {
		return len(m.mapTasks) == m.mapTasksCompleted
	}

	if taskType == TaskTypeReduce {
		return len(m.reduceTasks) == m.reduceTasksCompleted
	}

	return false
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	m.onRPC(args.WorkerId)

	m.mutex.Lock()
	defer m.mutex.Unlock()
	t, found := m.findTask(args.WorkerId)

	if !found {
		t = &waitTask
	} else {
		t.Status = InProgress
		t.LastInteraction = time.Now()
		t.WorkerId = args.WorkerId
	}

	reply.TaskId = t.TaskId
	reply.TaskType = t.Type
	reply.Splits = t.Splits
	reply.R = m.R

	return nil
}

func (m *Master) CompleteTask(args *CompleteTaskArgs, reply *interface{}) error {

	m.onRPC(args.WorkerId)

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if args.TaskType == TaskTypeMap {

		updateCompleteTask(m.mapTasks[args.TaskId])

		for i, s := range args.Splits {
			m.reduceTasks[i].Splits = append(m.reduceTasks[i].Splits, s)
		}

		m.mapTasksCompleted += 1
		log.Printf("# of map tasks completed: %d", m.mapTasksCompleted)
	}

	if args.TaskType == TaskTypeReduce {

		updateCompleteTask(m.reduceTasks[args.TaskId])

		m.reduceTasksCompleted += 1
		log.Printf("# of reduce tasks completed: %d", m.reduceTasksCompleted)
	}

	return nil
}

func updateCompleteTask(t *Task) {
	t.LastInteraction = time.Now()
	t.Status = Completed
	t.WorkerId = ""
}

func (m *Master) onRPC(workerId string) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	w, exists := m.workers[workerId]
	if !exists {
		log.Printf("registering worker %s", workerId)
		m.workers[workerId] = &EnrolledWorker{
			Id:              workerId,
			Status:          WorkerIdle,
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

	log.Printf("files to process: %v", files)
	m.workers = make(map[string]*EnrolledWorker)

	mapTasks := make([]*Task, len(files))
	for i, f := range files {
		mapTasks[i] = &Task{
			TaskId: i,
			Type:   TaskTypeMap,
			Status: Idle,
			Splits: []Split{{
				Path: f,
			}},
		}
	}
	m.mapTasks = mapTasks
	m.mapTasksCompleted = 0

	m.reduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = &Task{
			TaskId: i,
			Type:   TaskTypeReduce,
			Status: Idle,
			Splits: []Split{},
		}
	}
	m.reduceTasksCompleted = 0

	m.R = nReduce

	m.server()
	return &m
}
