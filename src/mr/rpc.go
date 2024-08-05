package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type DoMapReq struct{}

type DoMapResp struct {
	Task   MapTask
	Status Status
}

type IntermediateFile struct {
	Worker    int
	ReduceNum int
	Filename  string
}

type DoneMapReq struct {
	Task   MapTask
	IFiles []IntermediateFile
}
type DoneMapResp struct{}

type DoReduceReq struct{}

type DoReduceResp struct {
	Task   ReduceTask
	Status Status
}

type DoneReduceReq struct {
	Task ReduceTask
}

type DoneReduceResp struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
