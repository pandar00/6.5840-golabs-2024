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

type LogEntry struct {
	SeqNum int64
	// Value after the operation
	Value string
}
type KVServer struct {
	mu        sync.Mutex
	store     map[string]string
	clientLog map[int64]*LogEntry
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seen, value := kv.deduplicate(args.ClientID, args.SeqNum); seen {
		reply.Value = value
		return
	}

	reply.Value = kv.store[args.Key]

	kv.clientLog[args.ClientID] = &LogEntry{
		SeqNum: args.SeqNum,
		Value:  reply.Value,
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seen, value := kv.deduplicate(args.ClientID, args.SeqNum); seen {
		reply.Value = value
		return
	}

	kv.store[args.Key] = args.Value

	kv.clientLog[args.ClientID] = &LogEntry{
		SeqNum: args.SeqNum,
		Value:  reply.Value,
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if seen, value := kv.deduplicate(args.ClientID, args.SeqNum); seen {
		reply.Value = value
		return
	}

	oldValue := kv.store[args.Key]
	kv.store[args.Key] = oldValue + args.Value
	reply.Value = oldValue

	kv.clientLog[args.ClientID] = &LogEntry{
		SeqNum: args.SeqNum,
		Value:  reply.Value,
	}
}

func (kv *KVServer) deduplicate(clientID, seqNum int64) (bool, string) {
	if log, ok := kv.clientLog[clientID]; ok && log.SeqNum == seqNum {
		return true, log.Value
	}
	return false, ""
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.store = map[string]string{}
	kv.clientLog = map[int64]*LogEntry{}
	return kv
}
