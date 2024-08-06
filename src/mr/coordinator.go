package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TODO:
//  1. spawn workers
//  2. assigns workers to either map or reduce tasks
//
// NOTE:
// There are M map tasks and R reduce
// tasks to assign. The master picks idle workers and
// assigns each one a map task or a reduce task.
//
// NOTE:
// "The master keeps several data structures. For each map
// task and reduce task, it stores the state (idle, in-progress,
// or completed), and the identity of the worker machine
// (for non-idle tasks)."
// "The master is the conduit through which the location
// of intermediate file regions is propagated from map tasks
// to reduce tasks. Therefore, for each completed map task,
// the master stores the locations and sizes of the R intermediate
// file regions produced by the map task. Updates
// to this location and size information are received as map
// tasks are completed. The information is pushed incrementally to
// workers that have in-progress reduce tasks."
type Coordinator struct {
	// Your definitions here.
	NumReduce     int
	MapTasks      []*MapTask
	ReduceTasks   map[int]*ReduceTask
	m             *sync.Mutex
	nextID        int
	leaseDuration time.Duration
}

type Status string

const (
	unknown   Status = ""
	idle      Status = "idle"
	inPrgress Status = "inProgress"
	completed Status = "completed"
)

type MapTask struct {
	Status     Status
	Worker     int
	Split      string
	NReduce    int
	UpdateTime time.Time
	ExpireTime time.Time
}

type ReduceTask struct {
	Status     Status
	IFiles     []IntermediateFile
	NumReduce  int
	UpdateTime time.Time
	ExpireTime time.Time
}

// DoMap returns map work if available
func (c *Coordinator) DoMap(args *DoMapReq, reply *DoMapResp) error {
	c.m.Lock()
	defer c.m.Unlock()
	// 1. Find "idle" tasks
	// 2. Assign to worker
	for _, v := range c.MapTasks {
		// idle, or
		// inprogress but expired (unreachable)
		if v.Status == idle || (v.Status == inPrgress && time.Now().After(v.ExpireTime)) {
			v.Status = inPrgress
			v.UpdateTime = time.Now()
			v.ExpireTime = time.Now().Add(c.leaseDuration)

			v.Worker = c.nextID
			reply.Status = inPrgress
			reply.Task = *v

			// auto-increment
			c.nextID += 1
			return nil
		}
	}

	// Conditions
	// 1. Other map steps in progress
	// 2. Map steps are all done
	nMDone := 0
	for _, v := range c.MapTasks {
		if v.Status == completed {
			nMDone += 1
		}
	}
	if nMDone == len(c.MapTasks) {
		reply.Status = completed
	} else {
		reply.Status = inPrgress
	}
	return nil
}

func (c *Coordinator) DoneMap(args *DoneMapReq, reply *DoneMapResp) error {
	c.m.Lock()
	defer c.m.Unlock()

	// Find the task and change the status to complete
	for _, v := range c.MapTasks {
		if v.Split == args.Task.Split {
			if v.Worker != args.Task.Worker {
				panic(fmt.Errorf(
					"doneMap(). split expected to be worked on by %d. received %d",
					v.Worker, args.Task.Worker,
				))
			}
			if v.Status != inPrgress {
				panic(fmt.Errorf(
					"doneMap(). split expected to be inProgress. received %s",
					v.Status,
				))
			}

			v.Status = completed
			v.UpdateTime = time.Now()

		}
	}

	// Add all intermediate files.
	// ReduceNum is the reduce task ID
	if c.ReduceTasks == nil {
		c.ReduceTasks = map[int]*ReduceTask{}
	}

	fmt.Printf("Coordinator: files %#v\b", args.IFiles)
	for _, v := range args.IFiles {
		if _, ok := c.ReduceTasks[v.ReduceNum]; !ok {
			t := ReduceTask{
				Status:     idle,
				IFiles:     []IntermediateFile{},
				NumReduce:  v.ReduceNum,
				UpdateTime: time.Now(),
				ExpireTime: time.Time{},
			}
			c.ReduceTasks[v.ReduceNum] = &t
		}

		c.ReduceTasks[v.ReduceNum].IFiles = append(c.ReduceTasks[v.ReduceNum].IFiles, v)
	}

	return nil
}

func (c *Coordinator) DoReduce(args *DoReduceReq, reply *DoReduceResp) error {
	c.m.Lock()
	defer c.m.Unlock()

	for _, v := range c.ReduceTasks {
		if v.Status == idle {
			v.Status = inPrgress
			v.ExpireTime = time.Now().Add(c.leaseDuration)

			reply.Task = *v
			reply.Status = inPrgress
			break
		}
	}

	nrDone := 0
	for _, v := range c.ReduceTasks {
		if v.Status == completed {
			nrDone += 1
		}
	}

	if nrDone == len(c.ReduceTasks) {
		reply.Status = completed
	}

	return nil
}

func (c *Coordinator) DoneReduce(args *DoneReduceReq, reply *DoneReduceResp) error {
	c.m.Lock()
	defer c.m.Unlock()

	b, _ := json.Marshal(args)
	fmt.Printf("DoneReduce: %s\n", string(b))

	t := c.ReduceTasks[args.Task.NumReduce]
	t.Status = completed
	t.UpdateTime = time.Now()

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Coordinator is done when all tasks are finished
	nMDone, nRDone := 0, 0
	for _, v := range c.MapTasks {
		if v.Status == completed {
			nMDone += 1
		}
	}
	for _, v := range c.ReduceTasks {
		if v.Status == completed {
			nRDone += 1
		}
	}
	return nMDone == len(c.MapTasks) && nRDone == len(c.ReduceTasks)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
// TODO:
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:     nReduce,
		m:             &sync.Mutex{},
		leaseDuration: 1 * time.Minute,
	}

	// go func() {
	// 	tker := time.NewTicker(time.Second)
	// 	defer tker.Stop()
	// 	for true {
	// 		select {
	// 		case <-tker.C:
	// 			b, _ := json.Marshal(c)
	// 			fmt.Printf("Coordinator %s\n", string(b))
	// 		}
	// 	}
	// }()

	for _, v := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{
			Status:     idle,
			NReduce:    c.NumReduce,
			UpdateTime: time.Now(),
			Split:      v,
		})
	}

	c.server()
	return &c
}
