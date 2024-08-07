package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
	"strconv"
	"time"
)

// Some field types are interface{}, so registration is required for gob.
func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
}

type LeaseTaskReq struct {
	WorkerID string
}

type LeaseTaskResp struct {
	// Task may be empty if coordinator was unable to allocate a task.
	Task    interface{}
	HasTask bool
}

type TaskDoneReq struct {
	Task     interface{}
	WorkerID string
}

type TaskDoneResp struct{}

type IsDoneReq struct{}

type IsDoneResp struct {
	// true=done, false=not done
	Value bool
}

type Status string

const (
	// Idle is the initial state
	Idle       Status = "idle"
	InProgress Status = "in_progress"
	Done       Status = "done"
)

// IFile is the intermediate files
type IFile struct {
	ReduceNum int
	Name      string
}

type MapTask struct {
	// Split is the work to be done
	// Effectively ID of the map task
	Split  string
	Status Status
	Leasee string

	// IFiles is the resulting intermediate files
	// Set only after map task is completed
	IFiles []IFile

	// Number of reduce to be done
	NReduce int

	UpdateTime time.Time
	ExpireTime time.Time
}

func (t *MapTask) IsExpired() bool {
	return t.Status == InProgress && time.Now().After(t.ExpireTime)
}

type ReduceTask struct {
	// Reduce task number. Effectively the ID
	NumReduce int
	Status    Status
	Leasee    string

	// Intermediate files to reduce
	IFiles []IFile

	UpdateTime time.Time
	ExpireTime time.Time
}

func (t *ReduceTask) IsExpired() bool {
	return t.Status == InProgress && time.Now().After(t.ExpireTime)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
