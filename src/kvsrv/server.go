package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if v, ok := kv.store[args.Key]; ok {
		reply.Value = v
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldValue := kv.store[args.Key]
	kv.store[args.Key] = oldValue + args.Value
	reply.Value = oldValue
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = map[string]string{}
	return kv
}
