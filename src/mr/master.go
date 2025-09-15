package mr

import (
	"io"
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

const (
	Created TaskStatus = iota
	Running
	Failed
	Finished
)

const (
	WorkerIdle WorkerStatus = iota
	WorkerRunning
)

type Task struct {
	Status TaskStatus
	Split  FileSplit
	LastInteraction time.Time
}

type EnrolledWorker struct {
	Id string
	Status WorkerStatus
}

type FileSplit struct {
	Path  string
	Start int
	Size  int64
}

type Master struct {
	mutex    sync.Mutex
	mapTasks []Task
	workers  map[string]EnrolledWorker
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {

	m.onRPC(args.WorkerId)

	reply.Id = 123
	reply.TaskType = "wait"
	reply.Split = FileSplit{}

	return nil
}

func (m *Master) onRPC(workerId string) {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	_, exists := m.workers[workerId]
	if !exists {
		fmt.Printf("registering worker %s\n", workerId)
		m.workers[workerId] = EnrolledWorker{
			Id:     workerId,
			Status: WorkerIdle,
		}
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	fmt.Printf("files to process: %v \n", files)
	m.workers = make(map[string]EnrolledWorker)

	splits := splitFiles(files)
	
	mapTasks := make([]Task, len(splits))
	for i, s := range splits {
		mapTasks[i] = Task{
			Status: Created,
			Split:  s,
		}
	}
	m.mapTasks = mapTasks

	m.server()
	return &m
}

func splitFiles(files []string) []FileSplit {
	chunks := make([]FileSplit, 0)
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			panic("can't open file")
		}
		fileInfo, err := f.Stat()
		if err != nil {
			panic("cant stat file")
		}
		chunks = append(chunks, chunkFile(f, path, fileInfo.Size())...)
	}

	return chunks
}

// chunkFile subdivides a file in M parts. These are the "splits" in MapReduce terminology.
// Each split contains a group of lines from the file and
// has a size <= 64KiB

// NOTE the current implementation just returns 1 split
// representing the entire file
func chunkFile(r io.Reader, path string, size int64) []FileSplit {

	chunks := make([]FileSplit, 1)
	chunks[0] = FileSplit{
		Path:  path,
		Start: 0,
		Size:  size,
	}

	return chunks
}

/*
func chunkFiles(files []string) []FileChunk {

	chunks := make([]FileChunk, 0)
	for _, path := range files {

		f, err := os.Open(path)
		if err != nil {
			panic("can't read file")
		}

		buf := bufio.NewReader(f)
		buf.ReadByte()

		chunk := FileChunk{
			Path:  path,
			Start: 0,
			Size:  0,
		}
		pos := 0
		for {
			ch, err := buf.ReadByte()
			if err == io.EOF {

				break
			}

			pos += 1
			if ch == '\n' {
				newSize := pos - chunk.Start
				if newSize <= MaxChunkSize {
					chunk.Size = pos - chunk.Start
				} else {
					chunks = append(chunks, chunk)
					chunk = FileChunk{
						Path:  path,
						Start: pos,
						Size:  0,
					}
				}
			}
		}

	}

	return chunks
}
*/
