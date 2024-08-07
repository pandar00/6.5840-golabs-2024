package mr

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"math/big"
	"net/rpc"
	"os"
	"strings"
	"time"
)

var debug = false

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

// GetID generates random 8 char ID.
// Hacky implementation to avoid importing external library.
func GetID() string {
	// Get random integer
	maxi := big.NewInt(math.MaxInt64)
	i, err := rand.Int(rand.Reader, maxi)
	if err != nil {
		log.Fatal(fmt.Errorf("GetID; %w", err))
	}
	// Hash it and get first 8 chars to get random string
	h := sha256.New()
	h.Write([]byte(i.String()))
	return fmt.Sprintf("%x", h.Sum(nil))[:8]
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Idea: Worker just do what the coordinator tells it to do
	//       until the coordiator says it's done which then the Worker
	//       terminates the program
	// Event loop
	// LeaseTask
	//     Task is either
	//         map task    - single task  => split
	//         reduce task - singlet task => reduce number
	//     Conditions:
	//     1. Available task assigned
	//     2. No available task assigned. Poll isDone
	// Do Task
	// Emit Task Done

	// The worker should put intermediate Map output in files in the
	// current directory, where your worker can later read them as
	// input to Reduce tasks.

	// The worker implementation should put the output of the X'th
	// reduce task in the file mr-out-X.

	// A mr-out-X file should contain one line per Reduce function
	// output. The line should be generated with the Go "%v %v"
	// format, called with the key and value. Have a look in
	// main/mrsequential.go for the line commented "this is the
	// correct format". The test script will fail if your implementation
	// deviates too much from this format.

	ID := GetID()
	for true {
		if IsDone() {
			break
		}

		task, hasTask := LeaseTask(ID)
		if !hasTask {
			time.Sleep(time.Second)
			continue
		}

		switch t := task.(type) {
		case MapTask:
			DoMap(&t, mapf)
			TaskDone(ID, t)
		case ReduceTask:
			DoReduce(&t, reducef)
			TaskDone(ID, t)
		default:
			log.Fatal(fmt.Errorf("unknown type %T", task))
		}
	}
}

func DoMap(t *MapTask, f func(string, string) []KeyValue) {
	filename := t.Split
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("DoMap(); cannot open %v; %s", filename, err.Error())
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("DoMap(); cannot read %v, %s", filename, err.Error())
	}

	var iFiles []IFile
	files := make([]*os.File, t.NReduce)
	// Iterate list of key values
	for _, kv := range f(filename, string(content)) {
		nReduceTask := ihash(kv.Key) % t.NReduce
		ifname := fmt.Sprintf("mr-%s-%d", t.Leasee, nReduceTask)

		if files[nReduceTask] == nil {
			// Worker can work on more than one file. Append if the file already exists
			newF, err := os.OpenFile(ifname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("DoMap(); failed to open file %s; %s", ifname, err.Error())
			}
			defer newF.Close()

			files[nReduceTask] = newF

			iFiles = append(iFiles, IFile{
				ReduceNum: nReduceTask,
				Name:      ifname,
			})
		}

		fmt.Fprintf(files[nReduceTask], "%s %s\n", kv.Key, kv.Value)
	}
	t.IFiles = iFiles
}

func DoReduce(t *ReduceTask, f func(string, []string) string) {
	kv := map[string][]string{}

	// Read all intermidiate for a given reduce task
	for _, v := range t.IFiles {
		f, err := os.Open(v.Name)
		if err != nil {
			log.Fatalf("DoReduce(); failed to open file %s; %s", v.Name, err.Error())
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			tokens := strings.Split(line, " ")
			if _, ok := kv[tokens[0]]; !ok {
				kv[tokens[0]] = []string{}
			}
			kv[tokens[0]] = append(kv[tokens[0]], tokens[1])
		}
	}

	// Call reduce for each of the key and write to reduce file
	filename := fmt.Sprintf("mr-out-%d", t.NumReduce)
	outF, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("DoReduce(); failed to open file %s; %s", filename, err.Error())
	}
	defer outF.Close()

	for k, v := range kv {
		rVal := f(k, v)
		fmt.Fprintf(outF, "%v %v\n", k, rVal)
	}
}

func LeaseTask(workerID string) (interface{}, bool) {
	args := LeaseTaskReq{
		WorkerID: workerID,
	}
	reply := LeaseTaskResp{}
	ok := call("Coordinator.LeaseTask", &args, &reply)
	if !ok {
		fmt.Println("LeaseTask() call failed!")
	}
	return reply.Task, reply.HasTask
}

func TaskDone(workerID string, task interface{}) {
	args := TaskDoneReq{
		WorkerID: workerID,
		Task:     task,
	}
	reply := TaskDoneResp{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if !ok {
		fmt.Println("TaskDone() call failed!")
	}
}

func IsDone() bool {
	args := IsDoneReq{}
	reply := IsDoneResp{}
	ok := call("Coordinator.IsDone", &args, &reply)
	if !ok {
		fmt.Println("IsDone() call failed!")
	}
	return reply.Value
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if debug {
		b, _ := json.MarshalIndent(args, "", "    ")
		fmt.Printf("Call %s; Args: %s\n", rpcname, string(b))
	}

	err = c.Call(rpcname, args, reply)

	if debug {
		b, _ := json.MarshalIndent(reply, "", "    ")
		fmt.Printf("Call %s; Reply: %s\n", rpcname, string(b))
	}

	if err == nil {
		return true
	}

	fmt.Printf("Call %s failed; %s\n", rpcname, err.Error())

	return false
}
