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

type EmptyArgs struct{}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	FinishedPhase
)

type WorkerID int
type MapTaskDone int
type MapTaskID string
type ReduceTaskID int

type WorkerInfo struct {
	ID           WorkerID
	NReduceTasks int
}

type MapTaskResult struct {
	Filename    MapTaskID
	ID          WorkerID
	FileCounter int
}

type ReduceTaskDesc struct {
	Files  []string
	TaskID ReduceTaskID
}

type ReduceTaskResult struct {
	TaskID ReduceTaskID
	ID     WorkerID
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
