package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func initializeWorker() WorkerInfo {
	args := EmptyArgs{}
	info := WorkerInfo{}

	ok := call("Coordinator.RegisterWorker", &args, &info)

	if !ok {
		log.Fatal("Error getting worker info")
	}
	return info
}

func getMapTask(id WorkerID) MapTaskID {
	var fileToMap MapTaskID
	ok := call("Coordinator.GetMapTask", &id, &fileToMap)

	if !ok {
		log.Fatal("Error getting new file to map")
	}
	return fileToMap
}

func getReduceTask(id WorkerID) ReduceTaskDesc {
	var reduceTaskDesc ReduceTaskDesc
	ok := call("Coordinator.GetReduceTask", &id, &reduceTaskDesc)

	if !ok {
		log.Fatal("Error getting new reduce task")
	}
	return reduceTaskDesc
}

func (w *WorkerInfo) StartMapFileJob(filename MapTaskID, mappedFileCounter int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(string(filename))
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	keyValuePairs := mapf(string(filename), string(content))

	intermediateOutputFiles := []*os.File{}

	for i := 0; i < w.NReduceTasks; i++ {
		mapOutputFilename := fmt.Sprintf("mr-map-out-%v-%v-%v", w.ID, mappedFileCounter, i)
		ofile, _ := os.Create(mapOutputFilename)
		intermediateOutputFiles = append(intermediateOutputFiles, ofile)
	}

	for _, pair := range keyValuePairs {
		idx := ihash(pair.Key) % w.NReduceTasks
		strToWrite := fmt.Sprintf("%v %v\n", pair.Key, pair.Value)
		n, err := intermediateOutputFiles[idx].WriteString(strToWrite)

		if n != len(strToWrite) || err != nil {
			log.Printf("Failed to write string: (%s), to file %s", strToWrite, intermediateOutputFiles[idx].Name())
		}
	}

	for _, file := range intermediateOutputFiles {
		file.Close()
	}
}

func (w *WorkerInfo) StartReduceTaskJob(reduceTaskDesc ReduceTaskDesc, reducef func(string, []string) string) {
	reduceMap := map[string][]string{}

	for _, filename := range reduceTaskDesc.Files {
		file, err := os.Open(string(filename))
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		contentLines := strings.Split(string(content), "\n")

		for _, line := range contentLines {
			KVPair := strings.Split(line, " ")
			// log.Fatalf("\t\t\t\tt\t\t\t\t\t\t\tLENGTH: %v", len(KVPair))
			if len(KVPair) != 2 {
				continue
			}
			_, exists := reduceMap[KVPair[0]]
			if !exists {
				reduceMap[KVPair[0]] = []string{}
			}
			reduceMap[KVPair[0]] = append(reduceMap[KVPair[0]], KVPair[1])
		}
	}

	outputFileName := fmt.Sprintf("mr-out-%d", reduceTaskDesc.TaskID)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", outputFileName)
	}

	for key, values := range reduceMap {
		reducedValue := reducef(key, values)
		strToWrite := fmt.Sprintf("%v %v\n", key, reducedValue)
		n, err := outputFile.WriteString(strToWrite)

		if n != len(strToWrite) || err != nil {
			log.Fatalf("Failed to write %v to file %v", strToWrite, outputFileName)
		}
	}

	outputFile.Close()
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerInfo := initializeWorker()
	log.Printf("Worker %d has been initialized", workerInfo.ID)
	go sendHeartBeat(workerInfo.ID)

	// Your worker implementation here.
	mappedFileCounter := 0
	for GetPhase() == MapPhase {
		fileToMap := getMapTask(workerInfo.ID)

		if len(fileToMap) == 0 {
			time.Sleep(2 * time.Second)
		} else {
			log.Printf("Worker %d has been assigned map task %s", workerInfo.ID, fileToMap)
			workerInfo.StartMapFileJob(fileToMap, mappedFileCounter, mapf)
			MarkMapTaskAsFinished(fileToMap, mappedFileCounter, workerInfo.ID)
			mappedFileCounter++
		}
	}

	for GetPhase() == ReducePhase {
		reduceTaskDesc := getReduceTask(workerInfo.ID)

		if reduceTaskDesc.TaskID == -1 {
			time.Sleep(2 * time.Second)
		} else {
			log.Printf("Reduce Phase: Worker ID: %d, Task id: %d, number of files %d", workerInfo.ID, reduceTaskDesc.TaskID, len(reduceTaskDesc.Files))
			workerInfo.StartReduceTaskJob(reduceTaskDesc, reducef)
			MarkReduceTaskAsFinished(reduceTaskDesc.TaskID, workerInfo.ID)
		}
	}
}

func MarkMapTaskAsFinished(mapTaskId MapTaskID, mappedFileCounter int, workerId WorkerID) {
	result := MapTaskResult{
		Filename:    mapTaskId,
		FileCounter: mappedFileCounter,
		ID:          workerId,
	}
	args := EmptyArgs{}
	ok := call("Coordinator.MarkMapTaskAsFinished", &result, &args)

	if !ok {
		log.Fatalf("Could not submit completion state of map task: %s, worker ID: %d", mapTaskId, workerId)
	}

}

func MarkReduceTaskAsFinished(reduceTaskId ReduceTaskID, workerId WorkerID) {
	result := ReduceTaskResult{
		ID:     workerId,
		TaskID: reduceTaskId,
	}
	args := EmptyArgs{}
	ok := call("Coordinator.MarkReduceTaskAsFinished", &result, &args)

	if !ok {
		log.Fatalf("Could not submit completion state of reduce task: %d, worker ID: %d", reduceTaskId, workerId)
	}

}

func GetPhase() Phase {
	var currentPhase Phase
	args := EmptyArgs{}
	ok := call("Coordinator.GetPhase", &args, &currentPhase)

	if !ok {
		log.Fatal("Error getting phase")
	}
	return currentPhase
}

func sendHeartBeat(id WorkerID) {
	for {
		args := EmptyArgs{}
		ok := call("Coordinator.RegisterHeartBeat", &id, &args)

		if !ok {
			log.Fatal("Error getting phase")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	retries := 5

	c, err := rpc.DialHTTP("unix", sockname)
	for retries > 0 {
		if err != nil {
			log.Print("dialing:", err)
		} else {
			break
		}
		c, err = rpc.DialHTTP("unix", sockname)
		retries--
		time.Sleep(500 * time.Millisecond)
	}

	if retries == 0 {
		log.Fatal("Could not contact coordinator:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
