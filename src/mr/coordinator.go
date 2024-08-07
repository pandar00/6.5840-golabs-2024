package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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
	NumReduce int

	// key=split
	MapTasks map[string]*MapTask
	// key=reduce num
	ReduceTasks map[int]*ReduceTask

	// Every RPC operation is locked to guarantee mutual exclusiveness
	m *sync.Mutex

	// Duration until coordinate assumes the worker is dead
	leaseDuration time.Duration
}

func (c *Coordinator) LeaseTask(req *LeaseTaskReq, resp *LeaseTaskResp) error {
	c.m.Lock()
	defer c.m.Unlock()

	// TODO: If a worker dies after lease it can die in either map or reduce phase.
	//       In the case worker dies in reduce phase, the coordinator needs to
	//       reset any ongoing resduce task and redo the map phase of the dead worker
	//       because, strictly speaking, intermediate files are local to the worker.
	//       However, the code still works because we're running everything locally
	//       which preserves the intermeidate result even after the worker dies.

	nmDone := 0
	// Iterate map or reduce tasks
	for _, t := range c.MapTasks {
		if t.Status == Idle || t.IsExpired() {
			t.Status = InProgress
			t.Leasee = req.WorkerID
			t.UpdateTime = time.Now()
			t.ExpireTime = time.Now().Add(c.leaseDuration)

			resp.Task = t
			resp.HasTask = true
			return nil
		}

		if t.Status == Done {
			nmDone += 1
		}
	}

	// Wait handing out reduce tasks until map tasks are all finished
	if nmDone != len(c.MapTasks) {
		return nil
	}

	for _, t := range c.ReduceTasks {
		if t.Status == Idle || t.IsExpired() {
			t.Status = InProgress
			t.Leasee = req.WorkerID
			t.UpdateTime = time.Now()
			t.ExpireTime = time.Now().Add(c.leaseDuration)

			resp.Task = t
			resp.HasTask = true
			return nil
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(req *TaskDoneReq, resp *TaskDoneResp) error {
	c.m.Lock()
	defer c.m.Unlock()

	switch rt := req.Task.(type) {
	case MapTask:
		t := c.MapTasks[rt.Split]

		// Ignore if the rogue worker is detected. Coordinator will reassign.
		if t.Status != InProgress || t.IsExpired() {
			return nil
		}

		// Mark map task as done and update other fields
		t.Status = Done
		t.UpdateTime = time.Now()
		t.IFiles = rt.IFiles

		// Create reduce tasks
		for _, v := range t.IFiles {
			if reduceT, ok := c.ReduceTasks[v.ReduceNum]; ok {
				// Existing reduce task may already have ifiles for a given worker
				// if worker is reassigned to a map task so only add if ifiles are new
				found := false
				for _, ifile := range reduceT.IFiles {
					if ifile.Name == v.Name {
						found = true
					}
				}
				if !found {
					reduceT.IFiles = append(reduceT.IFiles, v)
				}
			} else {
				// No reduce task found. Create a new one.
				reduceT := ReduceTask{
					NumReduce:  v.ReduceNum,
					Status:     Idle,
					IFiles:     []IFile{v},
					UpdateTime: time.Now(),
				}
				c.ReduceTasks[v.ReduceNum] = &reduceT
			}
		}
	case ReduceTask:
		t := c.ReduceTasks[rt.NumReduce]

		// Ignore if the rogue worker is detected. Coordinator will reassign.
		if t.Status != InProgress || t.IsExpired() {
			return nil
		}

		// Mark reduce task as done and update other fields
		t.Status = Done
		t.UpdateTime = time.Now()

	default:
		panic(fmt.Errorf("unknown type %T", req.Task))
	}

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
	c.m.Lock()
	defer c.m.Unlock()
	// Coordinator is done when all tasks are finished
	nMDone, nRDone := 0, 0
	for _, v := range c.MapTasks {
		if v.Status == Done {
			nMDone += 1
		}
	}
	for _, v := range c.ReduceTasks {
		if v.Status == Done {
			nRDone += 1
		}
	}
	return nMDone == len(c.MapTasks) && nRDone == len(c.ReduceTasks)
}

func (c *Coordinator) IsDone(req *IsDoneReq, resp *IsDoneResp) error {
	resp.Value = c.Done()
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce: nReduce,

		MapTasks:    map[string]*MapTask{},
		ReduceTasks: map[int]*ReduceTask{},

		m:             &sync.Mutex{},
		leaseDuration: 10 * time.Second,
	}

	for _, v := range files {
		c.MapTasks[v] = &MapTask{
			Status:     Idle,
			NReduce:    c.NumReduce,
			UpdateTime: time.Now(),
			Split:      v,
		}
	}

	c.server()
	return &c
}
