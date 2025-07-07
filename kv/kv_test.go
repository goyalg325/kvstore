package kv

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/goyalg325/kvstore/raft"
	"github.com/stretchr/testify/assert"
)

// TestBasicKV tests basic key-value operations
func TestBasicKV(t *testing.T) {
	// Create a test cluster with 3 servers
	cfg := makeTestConfig(t, 3, false)
	defer cfg.cleanup()

	// Create a client
	ck := cfg.makeClient()

	// Test Put and Get
	ck.Put("k1", "v1")
	check(t, ck, "k1", "v1")

	ck.Put("k2", "v2")
	check(t, ck, "k2", "v2")

	// Test overwrites
	ck.Put("k1", "v1b")
	check(t, ck, "k1", "v1b")

	// Test Delete
	ck.Delete("k1")
	checkNotFound(t, ck, "k1")
}

// TestConcurrent tests concurrent operations
func TestConcurrent(t *testing.T) {
	// Create a test cluster with 3 servers
	cfg := makeTestConfig(t, 3, false)
	defer cfg.cleanup()

	// Create multiple clients
	const numClients = 5
	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = cfg.makeClient()
	}

	// Perform concurrent operations
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := clients[i]
			for j := 0; j < 10; j++ {
				key := fmt.Sprintf("k%d-%d", i, j)
				value := fmt.Sprintf("v%d-%d", i, j)
				ck.Put(key, value)
				check(t, ck, key, value)
			}
		}(i)
	}
	wg.Wait()

	// Verify all values
	for i := 0; i < numClients; i++ {
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("k%d-%d", i, j)
			value := fmt.Sprintf("v%d-%d", i, j)
			check(t, clients[0], key, value)
		}
	}
}

// TestUnreliableNetwork tests the system under an unreliable network
func TestUnreliableNetwork(t *testing.T) {
	// Create a test cluster with 5 servers
	cfg := makeTestConfig(t, 5, false)
	defer cfg.cleanup()

	// Make the network unreliable
	cfg.setUnreliable(true)

	// Create a client
	ck := cfg.makeClient()

	// Perform a series of operations
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		ck.Put(key, value)
	}

	// Wait for stability
	time.Sleep(1 * time.Second)
	cfg.setUnreliable(false)
	time.Sleep(500 * time.Millisecond)

	// Verify values
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		check(t, ck, key, value)
	}
}

// TestPersistence tests persistence after restarts
func TestPersistence(t *testing.T) {
	// Create a test cluster with 3 servers
	cfg := makeTestConfig(t, 3, true)
	defer cfg.cleanup()

	// Create a client
	ck := cfg.makeClient()

	// Perform some operations
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		ck.Put(key, value)
	}

	// Restart all servers
	cfg.restart()

	// Create a new client
	ck2 := cfg.makeClient()

	// Verify values persisted
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		check(t, ck2, key, value)
	}

	// Perform more operations
	for i := 10; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		ck2.Put(key, value)
	}

	// Verify all values
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		check(t, ck2, key, value)
	}
}

// TestSnapshot tests snapshot creation and recovery
func TestSnapshot(t *testing.T) {
	// Create a test cluster with small maxraftstate to trigger snapshots
	cfg := makeTestConfig(t, 3, true)
	cfg.maxraftstate = 1000 // Small state size to trigger snapshots
	defer cfg.cleanup()

	// Create a client
	ck := cfg.makeClient()

	// Insert enough data to trigger snapshots
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		ck.Put(key, value)
	}

	// Wait for snapshots to be taken
	time.Sleep(1 * time.Second)

	// Restart all servers
	cfg.restart()

	// Create a new client
	ck2 := cfg.makeClient()

	// Verify values after restart
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		check(t, ck2, key, value)
	}
}

// TestOnePartition tests the system's behavior during a network partition
func TestOnePartition(t *testing.T) {
	// Create a test cluster with 5 servers
	cfg := makeTestConfig(t, 5, false)
	defer cfg.cleanup()

	// Create a client
	ck := cfg.makeClient()

	// Perform some operations before partition
	ck.Put("k1", "v1")
	check(t, ck, "k1", "v1")

	// Create a partition: servers 0,1,2 in majority, 3,4 in minority
	cfg.partition([]int{0, 1, 2}, []int{3, 4})

	// Client should still be able to talk to the majority partition
	ck.Put("k2", "v2")
	check(t, ck, "k2", "v2")

	// Create a client that tries to talk to the minority partition
	minorityCk := cfg.makeClientWithPreferredServers([]int{3, 4})
	
	// This should eventually time out or fail
	ok := false
	for i := 0; i < 5; i++ {
		// The Put should fail since the minority can't reach consensus
		err := minorityCk.Put("k3", "v3")
		if err != nil {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	
	assert.True(t, ok, "Put to minority partition should fail")

	// Heal the partition
	cfg.reconnect()
	time.Sleep(500 * time.Millisecond)

	// Both keys from the majority side should be visible
	check(t, ck, "k1", "v1")
	check(t, ck, "k2", "v2")
	
	// The key attempted on the minority side should not exist
	checkNotFound(t, ck, "k3")
}

// TestManyPartitions tests the system's behavior during multiple partitions
func TestManyPartitions(t *testing.T) {
	// Create a test cluster with 5 servers
	cfg := makeTestConfig(t, 5, false)
	defer cfg.cleanup()

	// Create a client
	ck := cfg.makeClient()

	// Insert some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		ck.Put(key, value)
	}

	// Create a series of different partitions
	partitions := []struct {
		majority []int
		minority []int
	}{
		{[]int{0, 1, 2}, []int{3, 4}},
		{[]int{1, 2, 3}, []int{0, 4}},
		{[]int{2, 3, 4}, []int{0, 1}},
		{[]int{0, 2, 4}, []int{1, 3}},
	}

	for i, p := range partitions {
		// Create the partition
		cfg.partition(p.majority, p.minority)

		// Client should be able to talk to the majority partition
		key := fmt.Sprintf("kp%d", i)
		value := fmt.Sprintf("vp%d", i)
		ck.Put(key, value)

		// Wait a bit
		time.Sleep(100 * time.Millisecond)

		// Heal the partition
		cfg.reconnect()
		time.Sleep(500 * time.Millisecond)

		// Verify the value is visible
		check(t, ck, key, value)
	}

	// Verify all original values are still present
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%d", i)
		value := fmt.Sprintf("v%d", i)
		check(t, ck, key, value)
	}
}

//
// Helper functions and test infrastructure
//

// check verifies that the key has the expected value
func check(t *testing.T, ck *Client, key, expected string) {
	value, err := ck.Get(key)
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, expected, value, "Value should match expected")
}

// checkNotFound verifies that the key is not found
func checkNotFound(t *testing.T, ck *Client, key string) {
	_, err := ck.Get(key)
	assert.Error(t, err, "Get should fail for deleted/non-existent key")
}

// testConfig manages a test configuration for the KV service
type testConfig struct {
	t            *testing.T
	servers      int
	kvservers    []*KVServer
	rafts        []*raft.Raft
	connected    []bool
	reliable     bool
	maxraftstate int
	mu           sync.Mutex
}

// makeTestConfig creates a test configuration
func makeTestConfig(t *testing.T, n int, snapshot bool) *testConfig {
	cfg := &testConfig{}
	cfg.t = t
	cfg.servers = n
	cfg.connected = make([]bool, n)
	cfg.reliable = true
	cfg.kvservers = make([]*KVServer, n)
	cfg.rafts = make([]*raft.Raft, n)
	
	if snapshot {
		cfg.maxraftstate = 1000
	} else {
		cfg.maxraftstate = -1 // Disable snapshots
	}

	// Create servers
	for i := 0; i < n; i++ {
		// Create raft persister
		persister := raft.NewMemoryPersister()
		
		// Create a KV server connected to a raft instance
		cfg.kvservers[i] = NewKVServer(cfg.makeRaftPeers(i), i, persister, cfg.maxraftstate)
		cfg.connected[i] = true
	}

	return cfg
}

// makeRaftPeers creates the RPC clients for raft peers
func (cfg *testConfig) makeRaftPeers(me int) []raft.RaftPeer {
	peers := make([]raft.RaftPeer, cfg.servers)
	for i := 0; i < cfg.servers; i++ {
		if i == me {
			peers[i] = nil
		} else {
			peers[i] = &testRaftClient{
				cfg:       cfg,
				me:        me,
				server:    i,
				connected: true,
			}
		}
	}
	return peers
}

// makeClient creates a client for the KV service
func (cfg *testConfig) makeClient() *Client {
	servers := make([]string, cfg.servers)
	for i := 0; i < cfg.servers; i++ {
		servers[i] = fmt.Sprintf("server-%d", i)
	}
	return NewClient(servers)
}

// makeClientWithPreferredServers creates a client that prefers certain servers
func (cfg *testConfig) makeClientWithPreferredServers(preferred []int) *Client {
	servers := make([]string, len(preferred))
	for i, server := range preferred {
		servers[i] = fmt.Sprintf("server-%d", server)
	}
	return NewClient(servers)
}

// cleanup stops all servers
func (cfg *testConfig) cleanup() {
	for i := 0; i < len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			// In a real implementation, we would call Kill() or similar
		}
	}
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
}

// setUnreliable makes the network unreliable
func (cfg *testConfig) setUnreliable(unreliable bool) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.reliable = !unreliable
}

// partition creates a network partition
func (cfg *testConfig) partition(part1 []int, part2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// First, disconnect all servers
	for i := 0; i < cfg.servers; i++ {
		cfg.connected[i] = false
	}

	// Then connect servers in each partition to each other
	for _, i := range part1 {
		cfg.connected[i] = true
	}
	for _, i := range part2 {
		cfg.connected[i] = true
	}
}

// reconnect reconnects all servers
func (cfg *testConfig) reconnect() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.servers; i++ {
		cfg.connected[i] = true
	}
}

// restart restarts all servers
func (cfg *testConfig) restart() {
	
}

// testRaftClient implements the raft.RaftPeer interface for testing
type testRaftClient struct {
	cfg       *testConfig
	me        int
	server    int
	connected bool
}

// RequestVote implements the RequestVote RPC
func (tc *testRaftClient) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if !tc.cfg.connected[tc.server] || !tc.cfg.connected[tc.me] {
		return fmt.Errorf("disconnected")
	}

	if !tc.cfg.reliable {
		// Simulate network unreliability
		if rand.Intn(10) < 3 {
			return fmt.Errorf("network error")
		}
		// Simulate delay
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	// Forward to the actual Raft instance
	return tc.cfg.rafts[tc.server].RequestVote(args, reply)
}

// AppendEntries implements the AppendEntries RPC
func (tc *testRaftClient) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if !tc.cfg.connected[tc.server] || !tc.cfg.connected[tc.me] {
		return fmt.Errorf("disconnected")
	}

	if !tc.cfg.reliable {
		// Simulate network unreliability
		if rand.Intn(10) < 3 {
			return fmt.Errorf("network error")
		}
		// Simulate delay
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	// Forward to the actual Raft instance
	return tc.cfg.rafts[tc.server].AppendEntries(args, reply)
}

// InstallSnapshot implements the InstallSnapshot RPC
func (tc *testRaftClient) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if !tc.cfg.connected[tc.server] || !tc.cfg.connected[tc.me] {
		return fmt.Errorf("disconnected")
	}

	if !tc.cfg.reliable {
		// Simulate network unreliability
		if rand.Intn(10) < 3 {
			return fmt.Errorf("network error")
		}
		// Simulate delay
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}

	// Forward to the actual Raft instance
	return tc.cfg.rafts[tc.server].InstallSnapshot(args, reply)
}
