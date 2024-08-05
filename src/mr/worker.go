package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"reflect"
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

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//
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

MapEventLoop:
	for true {
		resp := DoMap()
		switch resp.Status {
		case completed:
			break MapEventLoop
		case inPrgress:
			// Read contents
			filename := resp.Task.Split
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// Write contents to mr-W-R where W=worker_task_num, R=num_reduce
			var iFiles []IntermediateFile
			files := make([]*os.File, resp.Task.NReduce)
			for _, v := range mapf(filename, string(content)) {
				nReduceTask := ihash(v.Key) % resp.Task.NReduce
				ifilename := fmt.Sprintf("mr-%d-%d", resp.Task.Worker, nReduceTask)

				iFiles = append(iFiles, IntermediateFile{
					Worker:    resp.Task.Worker,
					ReduceNum: nReduceTask,
					Filename:  ifilename,
				})

				if files[nReduceTask] == nil {
					newF, err := os.OpenFile(ifilename, os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Fatalf("failed to open file %s; %s", ifilename, err.Error())
					}
					files[nReduceTask] = newF
				}

				f := files[nReduceTask]
				fmt.Fprintf(f, "%v %v\n", v.Key, v.Value)
			}

			// Flush
			for _, f := range files {
				f.Close()
			}
			DoneMap(resp.Task, iFiles)

			// Map phase is done. Continue the loop to poll for more work
		case idle, unknown:
			fallthrough
		default:
			time.Sleep(1 * time.Second)
		}
	}
ReduceEventLoop:
	for true {
		resp := DoReduce()
		switch resp.Status {
		case completed:
			break ReduceEventLoop
		case inPrgress:
			kv := map[string][]string{}

			// Read all intermidiate for a given reduce task
			for _, v := range resp.Task.IFiles {
				f, err := os.Open(v.Filename)
				if err != nil {
					log.Fatalf("failed to open file %s; %s", v.Filename, err.Error())
				}
				scanner := bufio.NewScanner(f)
				for scanner.Scan() {
					t := scanner.Text()
					tokens := strings.Split(t, " ")
					if _, ok := kv[tokens[0]]; !ok {
						kv[tokens[0]] = []string{}
					}

					kv[tokens[0]] = append(kv[tokens[0]], tokens[1])
				}
				f.Close()
			}

			// Call reduce for each of the key and write to reduce file
			oFilename := fmt.Sprintf("mr-out-%d", resp.Task.NumReduce)
			outF, err := os.OpenFile(oFilename, os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatalf("failed to open file %s; %s", oFilename, err.Error())
			}
			for k, v := range kv {
				rVal := reducef(k, v)
				fmt.Fprintf(outF, "%v %v\n", k, rVal)
			}
			outF.Close()
			DoneReduce(resp.Task)
		case idle, unknown:
			fallthrough
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func DoMap() DoMapResp {
	args := DoMapReq{}
	reply := DoMapResp{}

	ok := call("Coordinator.DoMap", &args, &reply)
	if ok {
		b, _ := json.Marshal(&reply)
		fmt.Printf(string(b))
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoneMap(t MapTask, iFiles []IntermediateFile) DoneMapResp {
	args := DoneMapReq{
		Task:   t,
		IFiles: iFiles,
	}
	reply := DoneMapResp{}

	ok := call("Coordinator.DoneMap", &args, &reply)
	if ok {
		b, _ := json.Marshal(&reply)
		fmt.Printf(string(b))
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func DoReduce() DoReduceResp {
	args := DoReduceReq{}
	reply := DoReduceResp{}

	ok := call("Coordinator.DoReduce", &args, &reply)
	if ok {
		b, _ := json.Marshal(&reply)
		fmt.Printf(string(b))
	} else {
		fmt.Print("call failed!\n")
	}
	return reply
}

func DoneReduce(t ReduceTask) DoneReduceResp {
	args := DoneReduceReq{}
	reply := DoneReduceResp{}

	ok := call("Coordinator.DoneReduce", &args, &reply)
	if ok {
		b, _ := json.Marshal(&reply)
		fmt.Println(string(b))
	} else {
		fmt.Print("call failed!\n")
	}
	return reply
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

	fmt.Printf("calling %s, args %s, reply %s\n", rpcname,
		reflect.TypeOf(args).Name(),
		reflect.TypeOf(reply).Name())
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
