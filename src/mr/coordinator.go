package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                   sync.Mutex
	inputFiles           []string
	nReduceTasks         int
	currentPhase         Phase
	workerIdGen          WorkerID
	registeredHeartBeats map[WorkerID]bool
	activeWorkers        []WorkerID // Current active workers
	// Map Phase Variables
	validMapFiles            [][]string
	remainingMapTasksQueue   []MapTaskID // Tasks which have not been assigned to workers
	mapTaskCompletedBy       map[MapTaskID]WorkerID
	mapTaskAssignmentMapping map[WorkerID]MapTaskID // file assigned to each task

	remainingReduceTasksQueue   []ReduceTaskID // Tasks which have not been assigned to workers
	reduceTaskCompletedBy       map[ReduceTaskID]WorkerID
	reduceTaskAssignmentMapping map[WorkerID]ReduceTaskID // number assigned to each task
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) MarkMapTaskAsFinished(request *MapTaskResult, _ *EmptyArgs) error {
	c.mu.Lock()
	c.mapTaskCompletedBy[request.Filename] = request.ID
	for i := 0; i < c.nReduceTasks; i++ {
		c.validMapFiles[i] = append(c.validMapFiles[i], fmt.Sprintf("mr-map-out-%v-%v-%v", request.ID, request.FileCounter, i))
	}
	c.mu.Unlock()
	log.Printf("%s file has been mapped by worker %d", request.Filename, request.ID)
	return nil
}

func (c *Coordinator) MarkReduceTaskAsFinished(request *ReduceTaskResult, _ *EmptyArgs) error {
	c.mu.Lock()
	c.reduceTaskCompletedBy[request.TaskID] = request.ID
	c.mu.Unlock()
	log.Printf("%d reduce task has been finished by worker %d", request.TaskID, request.ID)
	return nil
}

func find[T comparable](elems []T, v T) int {
	for idx, s := range elems {
		if v == s {
			return idx
		}
	}
	return -1
}

func (c *Coordinator) GetMapTask(id *WorkerID, mapTask *MapTaskID) error {
	c.mu.Lock()
	if c.currentPhase != MapPhase {
		c.mu.Unlock()
		return errors.New("Wrong Phase")
	} else if find(c.activeWorkers, *id) == -1 {
		c.mu.Unlock()
		return errors.New("Worker is considered dead by the coordinator")
	} else if len(c.remainingMapTasksQueue) == 0 {
		mapTask = nil
	} else {
		*mapTask = c.remainingMapTasksQueue[0]
		c.remainingMapTasksQueue = c.remainingMapTasksQueue[1:]
		c.mapTaskAssignmentMapping[*id] = *mapTask
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) GetReduceTask(id *WorkerID, reduceTask *ReduceTaskDesc) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if find(c.activeWorkers, *id) == -1 {
		return errors.New("Worker is considered dead by the coordinator")
	} else if len(c.remainingReduceTasksQueue) == 0 {
		reduceTask.TaskID = -1
	} else {
		reduceTaskId := c.remainingReduceTasksQueue[0]
		reduceTask.TaskID = reduceTaskId
		reduceTask.Files = c.validMapFiles[reduceTaskId]
		c.remainingReduceTasksQueue = c.remainingReduceTasksQueue[1:]

		c.reduceTaskAssignmentMapping[*id] = reduceTaskId
	}
	return nil
}

func (c *Coordinator) RegisterHeartBeat(workerId *WorkerID, _ *EmptyArgs) error {
	c.mu.Lock()
	c.registeredHeartBeats[*workerId] = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) RegisterWorker(_, info *WorkerInfo) error {
	info.NReduceTasks = c.nReduceTasks
	c.mu.Lock()
	info.ID = c.workerIdGen
	c.workerIdGen++
	c.activeWorkers = append(c.activeWorkers, info.ID)
	c.mu.Unlock()
	log.Printf("Worker %d has been registered", info.ID)

	return nil
}

func (c *Coordinator) manageHeartBeats() {
	for !c.Done() {
		c.mu.Lock()
		newActiveWorkers := []WorkerID{}
		deadWorkers := []WorkerID{}
		for _, worker := range c.activeWorkers {
			_, exists := c.registeredHeartBeats[worker]
			if exists {
				newActiveWorkers = append(newActiveWorkers, worker)
			} else {
				deadWorkers = append(deadWorkers, worker)
				log.Printf("Worker %d is dead.", worker)
			}
		}

		for _, worker := range deadWorkers {
			if c.currentPhase == MapPhase {
				assignedMapTask, exists := c.mapTaskAssignmentMapping[worker]
				if exists {
					c.remainingMapTasksQueue = append(c.remainingMapTasksQueue, assignedMapTask)
					log.Printf("Map task %v originally assigned to worker %v has been returned to queue", assignedMapTask, worker)
					delete(c.mapTaskAssignmentMapping, worker)
				}
			} else if c.currentPhase == ReducePhase {
				assignedReduceTask, exists := c.reduceTaskAssignmentMapping[worker]
				if exists {
					c.remainingReduceTasksQueue = append(c.remainingReduceTasksQueue, assignedReduceTask)
					log.Printf("Map task %v originally assigned to worker %v has been returned to queue", assignedReduceTask, worker)
					delete(c.reduceTaskAssignmentMapping, worker)
				}
			}
		}
		c.activeWorkers = newActiveWorkers
		c.registeredHeartBeats = map[WorkerID]bool{}
		c.mu.Unlock()

		time.Sleep(2 * time.Second)
	}
}

// func removeMapPhaseSupuriousFiles(validMapFiles [][]string) {
// 	files, _ := ioutil.ReadDir("./")

// 	for _, file := range files {
// 		name := file.Name()
// 		if !file.IsDir() && strings.HasPrefix(name, "mr-map-out-") {
// 			parts := strings.Split(name, "-")
// 			reduceIdx :=
// 		}
// 	}
// }

func (c *Coordinator) managePhaseTransition() {
	for !c.Done() {
		c.mu.Lock()
		if c.currentPhase == MapPhase {
			mappedFileCount := 0
			for _, file := range c.inputFiles {
				_, exists := c.mapTaskCompletedBy[MapTaskID(file)]
				if exists {
					mappedFileCount++
				}
			}

			if mappedFileCount == len(c.inputFiles) {
				c.currentPhase = ReducePhase
				// removeSupuriousFiles(mapTaskCompletionWorkerID)
				log.Printf("SWITCHED TO REDUCE PHASE")
			}
		} else if c.currentPhase == ReducePhase {
			reduceTasksCount := 0
			for i := 0; i < c.nReduceTasks; i++ {
				_, exists := c.reduceTaskCompletedBy[ReduceTaskID(i)]
				if exists {
					reduceTasksCount++
				}
			}
			if reduceTasksCount == c.nReduceTasks {
				c.currentPhase = FinishedPhase
				log.Printf("SWITCHED TO FINISHED PHASE")
			}
		} else {
			c.mu.Unlock()
			break
		}
		c.mu.Unlock()

		time.Sleep(4 * time.Second)
	}
}

func (c *Coordinator) GetPhase(_ *EmptyArgs, phase *Phase) error {
	c.mu.Lock()
	*phase = c.currentPhase
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.manageHeartBeats()
	go c.managePhaseTransition()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	phase := c.currentPhase
	c.mu.Unlock()

	return phase == FinishedPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.inputFiles = files
	c.nReduceTasks = nReduce
	c.currentPhase = MapPhase
	c.remainingMapTasksQueue = []MapTaskID{}
	c.remainingReduceTasksQueue = []ReduceTaskID{}

	c.mapTaskCompletedBy = map[MapTaskID]WorkerID{}
	c.reduceTaskCompletedBy = map[ReduceTaskID]WorkerID{}

	c.mapTaskAssignmentMapping = map[WorkerID]MapTaskID{}
	c.reduceTaskAssignmentMapping = map[WorkerID]ReduceTaskID{}

	for _, file := range files {
		// c.mapTaskCompletionWorkerID[MapTaskID(file)] = -1
		c.remainingMapTasksQueue = append(c.remainingMapTasksQueue, MapTaskID(file))
	}

	for i := 0; i < nReduce; i++ {
		c.validMapFiles = append(c.validMapFiles, []string{})
		c.remainingReduceTasksQueue = append(c.remainingReduceTasksQueue, ReduceTaskID(i))
		// c.reduceTaskCompletionWorkerID[ReduceTaskID(i)] = -1
	}

	c.server()
	return &c
}
