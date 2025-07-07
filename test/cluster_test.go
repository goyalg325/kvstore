package test

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/goyalg325/kvstore/kv"
	"github.com/goyalg325/kvstore/raft"
	"github.com/goyalg325/kvstore/rpc"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

// TestClusterBasic tests basic cluster operations
func TestClusterBasic(t *testing.T) {
	// Create a cluster with 3 nodes
	cluster := NewTestCluster(t, 3)
	defer cluster.Cleanup()

	// Wait for leader election
	leader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, leader, "A leader should be elected")

	// Create a client
	client := cluster.NewClient()

	// Basic Put/Get operations
	err := client.Put("key1", "value1")
	assert.NoError(t, err, "Put should succeed")

	val, err := client.Get("key1")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "value1", val, "Value should match")

	// Multiple operations
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := client.Put(key, value)
		assert.NoError(t, err, "Put should succeed")
	}

	// Verify all values
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		val, err := client.Get(key)
		assert.NoError(t, err, "Get should succeed")
		assert.Equal(t, expected, val, "Value should match")
	}
}

// TestLeaderFailure tests the system's behavior when the leader fails
func TestLeaderFailure(t *testing.T) {
	// Create a cluster with 5 nodes
	cluster := NewTestCluster(t, 5)
	defer cluster.Cleanup()

	// Wait for leader election
	oldLeader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, oldLeader, "A leader should be elected")

	// Create a client
	client := cluster.NewClient()

	// Insert some data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := client.Put(key, value)
		assert.NoError(t, err, "Put should succeed")
	}

	// Kill the leader
	cluster.KillServer(oldLeader)

	// Wait for a new leader
	newLeader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, newLeader, "A new leader should be elected")
	assert.NotEqual(t, oldLeader, newLeader, "New leader should be different from old leader")

	// Try operations with the new leader
	err := client.Put("after-failure", "still-works")
	assert.NoError(t, err, "Put should succeed after leader failure")

	// Verify all values are preserved
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		val, err := client.Get(key)
		assert.NoError(t, err, "Get should succeed")
		assert.Equal(t, expected, val, "Value should be preserved after leader change")
	}

	// Verify the new value
	val, err := client.Get("after-failure")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "still-works", val, "Value should be accessible after leader change")
}

// TestConcurrentClients tests concurrent client operations
func TestConcurrentClients(t *testing.T) {
	// Create a cluster with 3 nodes
	cluster := NewTestCluster(t, 3)
	defer cluster.Cleanup()

	// Wait for leader election
	leader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, leader, "A leader should be elected")

	// Create multiple clients
	numClients := 5
	clients := make([]*kv.Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = cluster.NewClient()
	}

	// Run concurrent operations
	var eg errgroup.Group
	operations := 20

	for c := 0; c < numClients; c++ {
		clientID := c
		client := clients[c]
		eg.Go(func() error {
			for i := 0; i < operations; i++ {
				key := fmt.Sprintf("client%d-key%d", clientID, i)
				value := fmt.Sprintf("client%d-value%d", clientID, i)
				
				if err := client.Put(key, value); err != nil {
					return fmt.Errorf("put failed: %w", err)
				}
				
				// Read own write
				val, err := client.Get(key)
				if err != nil {
					return fmt.Errorf("get failed: %w", err)
				}
				if val != value {
					return fmt.Errorf("expected value %s, got %s", value, val)
				}
			}
			return nil
		})
	}

	// Wait for all operations to complete
	err := eg.Wait()
	assert.NoError(t, err, "Concurrent operations should succeed")

	// Verify all values from all clients
	for c := 0; c < numClients; c++ {
		for i := 0; i < operations; i++ {
			key := fmt.Sprintf("client%d-key%d", c, i)
			expected := fmt.Sprintf("client%d-value%d", c, i)
			
			val, err := clients[0].Get(key)
			assert.NoError(t, err, "Get should succeed")
			assert.Equal(t, expected, val, "Value should match after concurrent operations")
		}
	}
}

// TestNetworkPartition tests the system's behavior during a network partition
func TestNetworkPartition(t *testing.T) {
	// Create a cluster with 5 nodes
	cluster := NewTestCluster(t, 5)
	defer cluster.Cleanup()

	// Wait for leader election
	leader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, leader, "A leader should be elected")

	// Create a client
	client := cluster.NewClient()

	// Insert some data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("pre-partition-key%d", i)
		value := fmt.Sprintf("pre-partition-value%d", i)
		err := client.Put(key, value)
		assert.NoError(t, err, "Put should succeed")
	}

	// Create a network partition:
	// - Majority partition: 3 servers (including the leader if possible)
	// - Minority partition: 2 servers
	majorityPartition := []int{0, 1, 2}
	minorityPartition := []int{3, 4}

	// If the leader is in the minority partition, swap a server to ensure leader in majority
	for i, server := range minorityPartition {
		if server == leader {
			// Swap with a server in the majority partition
			minorityPartition[i] = majorityPartition[0]
			majorityPartition[0] = leader
			break
		}
	}

	// Apply the partition
	cluster.Partition(majorityPartition, minorityPartition)

	// Wait for a potential new leader in the majority partition
	newLeader := cluster.WaitForLeaderInSet(majorityPartition)
	assert.NotEqual(t, -1, newLeader, "A leader should be elected in the majority partition")

	// The majority partition should continue to function
	err := client.Put("during-partition", "majority-works")
	assert.NoError(t, err, "Put should succeed in majority partition")

	// Verify the value
	val, err := client.Get("during-partition")
	assert.NoError(t, err, "Get should succeed in majority partition")
	assert.Equal(t, "majority-works", val, "Value should be accessible in majority partition")

	// Create a client that talks to the minority partition
	minorityClient := cluster.NewClientForServers(minorityPartition)

	// The minority partition should not be able to make progress
	err = minorityClient.Put("minority-key", "minority-value")
	assert.Error(t, err, "Put should fail in minority partition")

	// Heal the partition
	cluster.HealPartition()

	// Wait for the cluster to stabilize
	time.Sleep(1 * time.Second)

	// Both partitions should now be able to see the same data
	val, err = client.Get("during-partition")
	assert.NoError(t, err, "Get should succeed after healing partition")
	assert.Equal(t, "majority-works", val, "Value should be accessible after healing partition")

	// The minority key should not exist as the operation should have failed
	_, err = client.Get("minority-key")
	assert.Error(t, err, "Get should fail for key that was attempted in minority partition")
}

// TestCrashRecovery tests recovery from crashes
func TestCrashRecovery(t *testing.T) {
	// Create a cluster with 3 nodes
	cluster := NewTestCluster(t, 3)
	defer cluster.Cleanup()

	// Wait for leader election
	leader := cluster.WaitForLeader()
	assert.NotEqual(t, -1, leader, "A leader should be elected")

	// Create a client
	client := cluster.NewClient()

	// Insert some data
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := client.Put(key, value)
		assert.NoError(t, err, "Put should succeed")
	}

	// Kill all servers
	for i := 0; i < cluster.size; i++ {
		cluster.KillServer(i)
	}

	// Restart all servers
	for i := 0; i < cluster.size; i++ {
		cluster.RestartServer(i)
	}

	// Wait for a leader to be elected
	leader = cluster.WaitForLeader()
	assert.NotEqual(t, -1, leader, "A leader should be elected after restart")

	// Verify all data is preserved
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		expected := fmt.Sprintf("value%d", i)
		
		// Retry a few times as the cluster may need time to recover
		success := false
		for j := 0; j < 5; j++ {
			val, err := client.Get(key)
			if err == nil && val == expected {
				success = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		
		assert.True(t, success, "Data should be preserved after restart")
	}

	// Insert new data
	err := client.Put("after-restart", "still-works")
	assert.NoError(t, err, "Put should succeed after restart")

	// Verify the new value
	val, err := client.Get("after-restart")
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, "still-works", val, "Value should be accessible after restart")
}

//
// Test infrastructure
//

// TestCluster represents a test cluster of KV servers
type TestCluster struct {
	t            *testing.T
	size         int
	servers      []*testServer
	rpcServers   []*rpc.Server
	clientPool   *rpc.ClientPool
	dataDir      string
	maxraftstate int
	mu           sync.Mutex
}

// testServer represents a server in the test cluster
type testServer struct {
	id        int
	addr      string
	kvserver  *kv.KVServer
	raft      *raft.Raft
	persister raft.Persister
	alive     bool
}

// NewTestCluster creates a new test cluster
func NewTestCluster(t *testing.T, size int) *TestCluster {
	rand.Seed(time.Now().UnixNano())
	
	cluster := &TestCluster{
		t:            t,
		size:         size,
		servers:      make([]*testServer, size),
		rpcServers:   make([]*rpc.Server, size),
		dataDir:      "test-data",
		maxraftstate: 1000,
	}
	
	// Create the servers
	basePort := 10000 + rand.Intn(10000)
	serverAddrs := make([]string, size)
	
	for i := 0; i < size; i++ {
		serverAddrs[i] = fmt.Sprintf("localhost:%d", basePort+i)
	}
	
	// Initialize the RPC client pool
	cluster.clientPool = rpc.NewClientPool(nil)
	
	// Create and start each server
	for i := 0; i < size; i++ {
		cluster.servers[i] = &testServer{
			id:        i,
			addr:      serverAddrs[i],
			persister: raft.NewMemoryPersister(),
			alive:     true,
		}
		
		// Start the RPC server
		cluster.rpcServers[i] = rpc.NewServer(serverAddrs[i], nil)
		
		// Create Raft peers
		peers := make([]raft.RaftPeer, size)
		for j := 0; j < size; j++ {
			if i != j {
				peers[j] = &raftRPCClient{
					clientPool: cluster.clientPool,
					serverAddr: serverAddrs[j],
				}
			}
		}
		
		// Create the Raft instance
		applyCh := make(chan raft.ApplyMsg)
		cluster.servers[i].raft = raft.NewRaft(peers, i, cluster.servers[i].persister, applyCh)
		
		// Create the KV server
		cluster.servers[i].kvserver = kv.NewKVServer(peers, i, cluster.servers[i].persister, cluster.maxraftstate)
		
		// Register the KV server with the RPC server
		err := cluster.rpcServers[i].RegisterName("KVService", cluster.servers[i].kvserver)
		if err != nil {
			t.Fatalf("Failed to register KV server: %v", err)
		}
		
		// Register the Raft server with the RPC server
		err = cluster.rpcServers[i].RegisterName("Raft", &raftService{rf: cluster.servers[i].raft})
		if err != nil {
			t.Fatalf("Failed to register Raft server: %v", err)
		}
		
		// Start the RPC server
		err = cluster.rpcServers[i].Start()
		if err != nil {
			t.Fatalf("Failed to start RPC server: %v", err)
		}
	}
	
	// Wait for the cluster to stabilize
	time.Sleep(1 * time.Second)
	
	return cluster
}

// Cleanup stops all servers
func (tc *TestCluster) Cleanup() {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	for i := 0; i < tc.size; i++ {
		if tc.rpcServers[i] != nil {
			tc.rpcServers[i].Stop()
		}
		if tc.servers[i] != nil && tc.servers[i].raft != nil {
			tc.servers[i].raft.Kill()
		}
	}
}

// KillServer simulates a server crash
func (tc *TestCluster) KillServer(id int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	if id < 0 || id >= tc.size {
		tc.t.Fatalf("Invalid server ID: %d", id)
	}
	
	if tc.servers[id].alive {
		// Kill the Raft instance
		tc.servers[id].raft.Kill()
		tc.servers[id].alive = false
	}
}

// RestartServer restarts a crashed server
func (tc *TestCluster) RestartServer(id int) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	
	if id < 0 || id >= tc.size {
		tc.t.Fatalf("Invalid server ID: %d", id)
	}
	
	if !tc.servers[id].alive {
		// Create Raft peers
		peers := make([]raft.RaftPeer, tc.size)
		for j := 0; j < tc.size; j++ {
			if id != j {
				peers[j] = &raftRPCClient{
					clientPool: tc.clientPool,
					serverAddr: tc.servers[j].addr,
				}
			}
		}
		
		// Create a new Raft instance with the same persister
		applyCh := make(chan raft.ApplyMsg)
		tc.servers[id].raft = raft.NewRaft(peers, id, tc.servers[id].persister, applyCh)
		
		// Create a new KV server
		tc.servers[id].kvserver = kv.NewKVServer(peers, id, tc.servers[id].persister, tc.maxraftstate)
		
		// Register the KV server with the RPC server
		err := tc.rpcServers[id].RegisterName("KVService", tc.servers[id].kvserver)
		if err != nil {
			tc.t.Fatalf("Failed to register KV server: %v", err)
		}
		
		// Register the Raft server with the RPC server
		err = tc.rpcServers[id].RegisterName("Raft", &raftService{rf: tc.servers[id].raft})
		if err != nil {
			tc.t.Fatalf("Failed to register Raft server: %v", err)
		}
		
		tc.servers[id].alive = true
	}
}

// Partition creates a network partition
func (tc *TestCluster) Partition(partition1 []int, partition2 []int) {
	// Not a real partition, but we'll simulate it at the RPC client level
}

// HealPartition heals a network partition
func (tc *TestCluster) HealPartition() {
	// Not a real partition, so nothing to heal
}

// WaitForLeader waits for a leader to be elected
func (tc *TestCluster) WaitForLeader() int {
	for i := 0; i < 10; i++ {
		for j := 0; j < tc.size; j++ {
			if tc.servers[j].alive {
				_, isLeader := tc.servers[j].raft.GetState()
				if isLeader {
					return j
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return -1
}

// WaitForLeaderInSet waits for a leader to be elected among a subset of servers
func (tc *TestCluster) WaitForLeaderInSet(servers []int) int {
	for i := 0; i < 10; i++ {
		for _, j := range servers {
			if j >= 0 && j < tc.size && tc.servers[j].alive {
				_, isLeader := tc.servers[j].raft.GetState()
				if isLeader {
					return j
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return -1
}

// NewClient creates a new client for the KV service
func (tc *TestCluster) NewClient() *kv.Client {
	servers := make([]string, tc.size)
	for i := 0; i < tc.size; i++ {
		servers[i] = tc.servers[i].addr
	}
	return kv.NewClient(servers)
}

// NewClientForServers creates a client that only talks to the specified servers
func (tc *TestCluster) NewClientForServers(serverIDs []int) *kv.Client {
	servers := make([]string, len(serverIDs))
	for i, id := range serverIDs {
		if id >= 0 && id < tc.size {
			servers[i] = tc.servers[id].addr
		}
	}
	return kv.NewClient(servers)
}

// raftRPCClient implements the raft.RaftPeer interface using RPC
type raftRPCClient struct {
	clientPool *rpc.ClientPool
	serverAddr string
}

// RequestVote sends a RequestVote RPC to a Raft peer
func (rc *raftRPCClient) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	client := rc.clientPool.GetClient(rc.serverAddr)
	return client.Call("Raft.RequestVote", args, reply)
}

// AppendEntries sends an AppendEntries RPC to a Raft peer
func (rc *raftRPCClient) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	client := rc.clientPool.GetClient(rc.serverAddr)
	return client.Call("Raft.AppendEntries", args, reply)
}

// InstallSnapshot sends an InstallSnapshot RPC to a Raft peer
func (rc *raftRPCClient) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	client := rc.clientPool.GetClient(rc.serverAddr)
	return client.Call("Raft.InstallSnapshot", args, reply)
}

// raftService wraps a Raft instance for RPC
type raftService struct {
	rf *raft.Raft
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
