// Package kv implements a distributed key-value store using the Raft consensus algorithm.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"

	"github.com/goyalg325/kvstore/raft"
)

// Command represents an operation to be performed on the key-value store
type Command struct {
	Op    string // "Put", "Get", or "Delete"
	Key   string
	Value string
}

// Result represents the result of a command execution
type Result struct {
	Value string
	Err   error
}

// Clerk represents a client of the key-value service
type Clerk struct {
	servers []string // List of server addresses
	leader  int      // Last known leader, to avoid trying all servers
	mu      sync.Mutex
}

// NewClerk creates a new client for the key-value service
func NewClerk(servers []string) *Clerk {
	return &Clerk{
		servers: servers,
		leader:  0,
	}
}

// Get retrieves the value for a key
func (ck *Clerk) Get(key string) (string, error) {
	cmd := Command{
		Op:  "Get",
		Key: key,
	}
	return ck.execute(cmd)
}

// Put sets a key-value pair
func (ck *Clerk) Put(key, value string) error {
	cmd := Command{
		Op:    "Put",
		Key:   key,
		Value: value,
	}
	_, err := ck.execute(cmd)
	return err
}

// Delete removes a key
func (ck *Clerk) Delete(key string) error {
	cmd := Command{
		Op:  "Delete",
		Key: key,
	}
	_, err := ck.execute(cmd)
	return err
}

// execute sends a command to the servers and returns the result
func (ck *Clerk) execute(cmd Command) (string, error) {
	ck.mu.Lock()
	leader := ck.leader
	ck.mu.Unlock()

	// Try each server, starting with the last known leader
	for i := 0; i < len(ck.servers); i++ {
		serverIndex := (leader + i) % len(ck.servers)
		server := ck.servers[serverIndex]

		args := &CommandArgs{Cmd: cmd}
		reply := &CommandReply{}

		// Create RPC client
		client, err := rpc.DialHTTP("tcp", server)
		if err != nil {
			continue
		}
		
		// Make the call with timeout
		call := client.Go("KVServer.Command", args, reply, nil)
		var callErr error
		
		select {
		case <-call.Done:
			callErr = call.Error
		case <-time.After(500 * time.Millisecond):
			callErr = fmt.Errorf("RPC call timed out")
		}
		
		client.Close()
		
		if callErr == nil && !reply.WrongLeader {
			// Update the leader for future requests
			ck.mu.Lock()
			ck.leader = serverIndex
			ck.mu.Unlock()
			
			if reply.Err != "" {
				return "", fmt.Errorf(reply.Err)
			}
			return reply.Value, nil
		}
	}

	return "", fmt.Errorf("all servers failed")
}

// CommandArgs contains the arguments for a command
type CommandArgs struct {
	Cmd Command
}

// CommandReply contains the result of a command
type CommandReply struct {
	WrongLeader bool
	Err         string
	Value       string
}

// KVServer is a server that stores key-value pairs
type KVServer struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32
	data         map[string]string
	lastApplied  map[int]CommandReply
	notifyCh     map[int]chan CommandReply
	persister    *KVPersister
	logger       *log.Logger
	maxraftstate int // snapshot if log grows this big
	lastIndex    int // last applied index
}

// NewKVServer creates a new key-value server
func NewKVServer(servers []raft.RaftPeer, me int, persister raft.Persister, maxraftstate int) *KVServer {
	// Register command types for gob
	gob.Register(Command{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		me:           me,
		rf:           raft.NewRaft(servers, me, persister, applyCh),
		applyCh:      applyCh,
		data:         make(map[string]string),
		lastApplied:  make(map[int]CommandReply),
		notifyCh:     make(map[int]chan CommandReply),
		persister:    NewKVPersister(fmt.Sprintf("kv-data-%d", me)),
		logger:       log.New(os.Stderr, fmt.Sprintf("[KV %d] ", me), log.Ltime|log.Lmicroseconds),
		maxraftstate: maxraftstate,
	}

	// Load snapshot if available
	kv.loadSnapshot(persister.ReadSnapshot())

	// Start applying log entries
	go kv.applier()

	kv.logger.Printf("initialized server with %d peers", len(servers))
	return kv
}

// Command handles client commands
func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) error {
	// Check if this is a read-only Get command
	if args.Cmd.Op == "Get" {
		// For read operations, we can fast-path by checking if we're the leader
		// and reading directly from the state without going through Raft
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.WrongLeader = true
			return nil
		}

		kv.mu.RLock()
		value, exists := kv.data[args.Cmd.Key]
		kv.mu.RUnlock()

		if exists {
			reply.Value = value
			return nil
		}

		// If key doesn't exist, still process through Raft to ensure linearizability
	}

	// Start command in Raft
	index, term, isLeader := kv.rf.Start(args.Cmd)
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	kv.logger.Printf("received command %v, index %d, term %d", args.Cmd, index, term)

	// Create a notification channel for this command
	ch := make(chan CommandReply, 1)
	kv.mu.Lock()
	kv.notifyCh[index] = ch
	kv.mu.Unlock()

	// Wait for the command to be applied, with a timeout
	select {
	case result := <-ch:
		*reply = result
	case <-time.After(time.Second):
		reply.Err = "timeout"
	}

	// Clean up
	kv.mu.Lock()
	delete(kv.notifyCh, index)
	kv.mu.Unlock()

	return nil
}

// applier applies commands from the Raft log to the key-value store
func (kv *KVServer) applier() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.applyCommand(msg)
		} else if msg.SnapshotValid {
			kv.applySnapshot(msg)
		}

		// Check if we need to make a snapshot
		if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
			kv.makeSnapshot(msg.CommandIndex)
		}
	}
}

// applyCommand applies a single command to the key-value store
func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {
	cmd, ok := msg.Command.(Command)
	if !ok {
		kv.logger.Printf("error: command type assertion failed at index %d", msg.CommandIndex)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := CommandReply{}

	// Apply the command to the key-value store
	switch cmd.Op {
	case "Put":
		kv.data[cmd.Key] = cmd.Value
		kv.logger.Printf("applied Put %s -> %s at index %d", cmd.Key, cmd.Value, msg.CommandIndex)
	case "Get":
		if value, ok := kv.data[cmd.Key]; ok {
			reply.Value = value
			kv.logger.Printf("applied Get %s -> %s at index %d", cmd.Key, value, msg.CommandIndex)
		} else {
			reply.Err = "key not found"
			kv.logger.Printf("applied Get %s -> not found at index %d", cmd.Key, msg.CommandIndex)
		}
	case "Delete":
		delete(kv.data, cmd.Key)
		kv.logger.Printf("applied Delete %s at index %d", cmd.Key, msg.CommandIndex)
	default:
		reply.Err = "unknown operation"
		kv.logger.Printf("unknown operation %s at index %d", cmd.Op, msg.CommandIndex)
	}

	kv.lastApplied[msg.CommandIndex] = reply
	kv.lastIndex = msg.CommandIndex

	// Notify waiting client, if any
	if ch, ok := kv.notifyCh[msg.CommandIndex]; ok {
		select {
		case ch <- reply:
			// Successfully notified
		default:
			// Channel buffer full (shouldn't happen with buffer size 1)
		}
	}
}

// applySnapshot applies a snapshot to the key-value store
func (kv *KVServer) applySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Only apply if snapshot is newer than current state
	if msg.SnapshotIndex <= kv.lastIndex {
		kv.logger.Printf("ignoring older snapshot at index %d, already at index %d", 
			msg.SnapshotIndex, kv.lastIndex)
		return
	}

	kv.logger.Printf("applying snapshot at index %d, term %d", msg.SnapshotIndex, msg.SnapshotTerm)
	kv.loadSnapshot(msg.Snapshot)
	kv.lastIndex = msg.SnapshotIndex
}

// makeSnapshot creates a snapshot of the current state
func (kv *KVServer) makeSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.logger.Printf("creating snapshot at index %d", index)
	
	// Encode KV state
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastIndex)
	snapshot := w.Bytes()

	// Save snapshot to persistent storage
	kv.persister.SaveSnapshot(snapshot)

	// Tell Raft to save the snapshot
	kv.rf.SaveSnapshot(index, snapshot)
}

// loadSnapshot loads a snapshot into the key-value store
func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)

	var data map[string]string
	var lastIndex int

	if d.Decode(&data) != nil || d.Decode(&lastIndex) != nil {
		kv.logger.Printf("error decoding snapshot")
		return
	}

	kv.data = data
	kv.lastIndex = lastIndex
	kv.logger.Printf("loaded snapshot with %d keys, last index %d", len(data), lastIndex)
}

// GetRaft returns the Raft instance
func (kv *KVServer) GetRaft() *raft.Raft {
	return kv.rf
}

// KVPersister manages persistent storage for KV data
type KVPersister struct {
	mu          sync.Mutex
	dataFile    string
	snapshotDir string
}

// NewKVPersister creates a new persister for KV data
func NewKVPersister(id string) *KVPersister {
	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	return &KVPersister{
		dataFile:    fmt.Sprintf("%s/%s-data", dataDir, id),
		snapshotDir: fmt.Sprintf("%s/%s-snapshots", dataDir, id),
	}
}

// SaveSnapshot saves a snapshot to disk
func (p *KVPersister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := os.WriteFile(p.dataFile, snapshot, 0644); err != nil {
		log.Printf("Failed to write snapshot: %v", err)
	}
}

// LoadSnapshot loads a snapshot from disk
func (p *KVPersister) LoadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.dataFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to read snapshot: %v", err)
		}
		return nil
	}

	return data
}

// RaftStateSize returns the size of the persisted Raft state in bytes
func (p *KVPersister) RaftStateSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	info, err := os.Stat(p.dataFile)
	if err != nil {
		return 0
	}
	
	return int(info.Size())
}

// The KVServiceHandler is the RPC handler for KV service methods
type KVServiceHandler struct {
	KV *KVServer
}

// Command is the RPC handler for the Command method
func (h *KVServiceHandler) Command(args *CommandArgs, reply *CommandReply) error {
	return h.KV.Command(args, reply)
}

// Get is the RPC handler for the Get method
func (h *KVServiceHandler) Get(args *GetArgs, reply *GetReply) error {
	cmd := Command{
		Op:  "Get",
		Key: args.Key,
	}
	
	cmdArgs := &CommandArgs{Cmd: cmd}
	cmdReply := &CommandReply{}
	
	err := h.KV.Command(cmdArgs, cmdReply)
	if err != nil {
		return err
	}
	
	reply.WrongLeader = cmdReply.WrongLeader
	reply.Err = cmdReply.Err
	reply.Value = cmdReply.Value
	
	return nil
}

// Put is the RPC handler for the Put method
func (h *KVServiceHandler) Put(args *PutArgs, reply *PutReply) error {
	cmd := Command{
		Op:    "Put",
		Key:   args.Key,
		Value: args.Value,
	}
	
	cmdArgs := &CommandArgs{Cmd: cmd}
	cmdReply := &CommandReply{}
	
	err := h.KV.Command(cmdArgs, cmdReply)
	if err != nil {
		return err
	}
	
	reply.WrongLeader = cmdReply.WrongLeader
	reply.Err = cmdReply.Err
	
	return nil
}

// Delete is the RPC handler for the Delete method
func (h *KVServiceHandler) Delete(args *DeleteArgs, reply *DeleteReply) error {
	cmd := Command{
		Op:  "Delete",
		Key: args.Key,
	}
	
	cmdArgs := &CommandArgs{Cmd: cmd}
	cmdReply := &CommandReply{}
	
	err := h.KV.Command(cmdArgs, cmdReply)
	if err != nil {
		return err
	}
	
	reply.WrongLeader = cmdReply.WrongLeader
	reply.Err = cmdReply.Err
	
	return nil
}
