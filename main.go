package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/goyalg325/kvstore/kv"
	"github.com/goyalg325/kvstore/raft"
	"github.com/goyalg325/kvstore/rpc"
)

// Command-line parameters
var (
	id           = flag.Int("id", 1, "Server ID")
	port         = flag.Int("port", 8001, "Port to listen on")
	clusterFlag  = flag.String("cluster", "localhost:8001,localhost:8002,localhost:8003", "Comma-separated list of cluster members")
	dataDir      = flag.String("datadir", "data", "Directory to store data")
	maxraftstate = flag.Int("maxraftstate", 1024*1024, "Max Raft state size in bytes before snapshotting")
	verbose      = flag.Bool("v", false, "Verbose logging")
	clientMode   = flag.Bool("client", false, "Run in client mode")
	operation    = flag.String("op", "", "Operation to perform (put, get, delete)")
	key          = flag.String("key", "", "Key for the operation")
	value        = flag.String("value", "", "Value for put operation")
)

func main() {
	flag.Parse()

	// Initialize logger
	logFlags := log.Ldate | log.Ltime
	if *verbose {
		logFlags |= log.Lmicroseconds
	}
	logger := log.New(os.Stdout, "", logFlags)

	// Parse cluster members
	cluster := strings.Split(*clusterFlag, ",")
	if len(cluster) == 0 {
		logger.Fatalf("Invalid cluster configuration")
	}

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		logger.Fatalf("Failed to create data directory: %v", err)
	}

	// Client mode
	if *clientMode {
		runClient(cluster, logger)
		return
	}

	// Server mode
	runServer(cluster, logger)
}

// runClient runs the client with the specified operation
func runClient(cluster []string, logger *log.Logger) {
	if *operation == "" {
		logger.Fatalf("Operation is required (-op=put|get|delete)")
	}
	if *key == "" {
		logger.Fatalf("Key is required (-key=<key>)")
	}

	// Create client
	client := kv.NewClient(cluster)

	// Perform operation
	switch *operation {
	case "put":
		if *value == "" {
			logger.Fatalf("Value is required for put operation (-value=<value>)")
		}
		err := client.Put(*key, *value)
		if err != nil {
			logger.Fatalf("Put failed: %v", err)
		}
		logger.Printf("Put successful: %s -> %s", *key, *value)

	case "get":
		value, err := client.Get(*key)
		if err != nil {
			logger.Fatalf("Get failed: %v", err)
		}
		logger.Printf("Get successful: %s -> %s", *key, value)

	case "delete":
		err := client.Delete(*key)
		if err != nil {
			logger.Fatalf("Delete failed: %v", err)
		}
		logger.Printf("Delete successful: %s", *key)

	default:
		logger.Fatalf("Unknown operation: %s", *operation)
	}
}

// runServer starts a KV server
func runServer(cluster []string, logger *log.Logger) {
	if *id < 1 || *id > len(cluster) {
		logger.Fatalf("Invalid server ID: %d", *id)
	}

	// Server address
	serverAddr := cluster[*id-1]
	_, _, err := net.SplitHostPort(serverAddr)
	if err != nil {
		logger.Fatalf("Invalid server address: %s", serverAddr)
	}

	// Create RPC server
	rpcServer := rpc.NewServer(serverAddr, logger)

	// Create RPC clients for all peers
	peers := make([]raft.RaftPeer, len(cluster))
	for i, addr := range cluster {
		peers[i] = newRaftRPCClient(addr, logger)
	}

	// Create persister
	persister, err := raft.NewFilePersister(*dataDir, *id)
	if err != nil {
		logger.Fatalf("Failed to create persister: %v", err)
	}
	
	// Create KV server
	kvserver := kv.NewKVServer(peers, *id-1, persister, *maxraftstate)
	
	// Create KV service handler
	kvHandler := &kv.KVServiceHandler{KV: kvserver}

	// Register KV service with RPC server
	if err := rpcServer.RegisterName("KVService", kvHandler); err != nil {
		logger.Fatalf("Failed to register KV server: %v", err)
	}
	
	// Create and register Raft service
	raftService := newRaftService(kvserver.GetRaft())
	if err := rpcServer.RegisterName("Raft", raftService); err != nil {
		logger.Fatalf("Failed to register Raft service: %v", err)
	}
	
	// Start RPC server
	if err := rpcServer.Start(); err != nil {
		logger.Fatalf("Failed to start RPC server: %v", err)
	}

	logger.Printf("Server %d started on %s", *id, serverAddr)

	// Wait for signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	// Graceful shutdown
	logger.Printf("Shutting down...")
	rpcServer.Stop()
	time.Sleep(100 * time.Millisecond) // Give a little time for connections to close
}

// newRaftRPCClient creates a new RPC client for a Raft peer
func newRaftRPCClient(addr string, logger *log.Logger) *raftRPCClient {
	return &raftRPCClient{
		addr:   addr,
		client: rpc.NewClient(addr, logger),
	}
}

// raftRPCClient implements the raft.RaftPeer interface using RPC
type raftRPCClient struct {
	addr   string
	client *rpc.Client
}

// RequestVote sends a RequestVote RPC to a Raft peer
func (c *raftRPCClient) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	return c.client.Call("Raft.RequestVote", args, reply)
}

// AppendEntries sends an AppendEntries RPC to a Raft peer
func (c *raftRPCClient) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	return c.client.Call("Raft.AppendEntries", args, reply)
}

// InstallSnapshot sends an InstallSnapshot RPC to a Raft peer
func (c *raftRPCClient) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	return c.client.Call("Raft.InstallSnapshot", args, reply)
}

// raftService wraps a Raft instance for RPC
type raftService struct {
	rf *raft.Raft
}

// newRaftService creates a new RaftService
func newRaftService(rf *raft.Raft) *raftService {
	return &raftService{rf: rf}
}

// RequestVote handles RequestVote RPCs
func (rs *raftService) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	return rs.rf.RequestVote(args, reply)
}

// AppendEntries handles AppendEntries RPCs
func (rs *raftService) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	return rs.rf.AppendEntries(args, reply)
}

// InstallSnapshot handles InstallSnapshot RPCs
func (rs *raftService) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	return rs.rf.InstallSnapshot(args, reply)
}
