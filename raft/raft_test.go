package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestBasicElection tests the basic leader election functionality
func TestBasicElection(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, false)
	defer cfg.cleanup()

	// Start all servers
	cfg.start()

	// Wait for a leader to be elected
	leader1 := cfg.checkOneLeader()
	
	// Basic sanity check
	term1, _ := cfg.rafts[leader1].GetState()
	
	// Sleep a bit to avoid random failures due to timing
	time.Sleep(50 * time.Millisecond)
	
	// Check if leader is still the leader
	term2, _ := cfg.rafts[leader1].GetState()
	if term1 != term2 {
		t.Fatalf("Term changed unexpectedly from %v to %v", term1, term2)
	}
}

// TestReElection tests the re-election after a leader disconnection
func TestReElection(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, false)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Wait for a leader to be elected
	leader1 := cfg.checkOneLeader()

	// Disconnect the leader
	cfg.disconnect(leader1)

	// Wait for a new leader to be elected
	leader2 := cfg.checkOneLeader()
	assert.NotEqual(t, leader1, leader2, "New leader should be different from old leader")

	// Reconnect the old leader
	cfg.connect(leader1)

	// Wait for the cluster to stabilize
	time.Sleep(100 * time.Millisecond)
	
	// Make sure there's still only one leader
	leader3 := cfg.checkOneLeader()
	
	// Check terms
	term1, _ := cfg.rafts[leader1].GetState()
	term2, _ := cfg.rafts[leader2].GetState()
	term3, _ := cfg.rafts[leader3].GetState()
	
	if term1 > term3 || term2 > term3 {
		t.Fatalf("Term inconsistency: %v, %v, %v", term1, term2, term3)
	}
}

// TestBasicAgree tests whether servers agree on log entries
func TestBasicAgree(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, false)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Wait for a leader
	leader := cfg.checkOneLeader()
	
	// Submit commands
	n := 5
	for i := 0; i < n; i++ {
		cmd := fmt.Sprintf("op%d", i)
		index, _, isLeader := cfg.rafts[leader].Start(cmd)
		if !isLeader {
			t.Fatalf("Leader lost leadership unexpectedly")
		}
		if index != i+1 {
			t.Fatalf("Expected index %v, got %v", i+1, index)
		}
	}
	
	// Wait for all commands to be committed
	for index := 1; index <= n; index++ {
		cmd := fmt.Sprintf("op%d", index-1)
		cfg.waitCommit(index, servers, cmd)
	}
}

// TestFailAgree tests whether servers agree despite failures
func TestFailAgree(t *testing.T) {
	servers := 5
	cfg := makeTestConfig(t, servers, false)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Wait for a leader
	leader := cfg.checkOneLeader()
	
	// Submit a command
	cfg.rafts[leader].Start("op1")
	
	// Wait for it to be committed
	cfg.waitCommit(1, servers, "op1")

	// Disconnect two followers
	follower1 := (leader + 1) % servers
	follower2 := (leader + 2) % servers
	cfg.disconnect(follower1)
	cfg.disconnect(follower2)

	// Submit more commands
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("op%d", i+2)
		cfg.rafts[leader].Start(cmd)
	}
	
	// Wait for them to be committed on the majority
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("op%d", i+2)
		cfg.waitCommit(i+2, servers-2, cmd)
	}

	// Reconnect the followers
	cfg.connect(follower1)
	cfg.connect(follower2)
	
	// Wait for the reconnected followers to catch up
	time.Sleep(200 * time.Millisecond)
	
	// Submit another command
	cfg.rafts[leader].Start("op7")
	
	// All servers should eventually commit all the commands
	for i := 0; i < 7; i++ {
		cmd := fmt.Sprintf("op%d", i+1)
		cfg.waitCommit(i+1, servers, cmd)
	}
}

// TestPersistence1 tests persistence with restarts
func TestPersistence1(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, false)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Submit a few commands
	for i := 1; i <= 5; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers, true)
	}

	// Find the leader
	leader := cfg.checkOneLeader()
	
	// Restart all servers
	cfg.restart()

	// Submit a few more commands
	for i := 6; i <= 10; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers, true)
	}
	
	// Check that all servers have all commands
	for i := 1; i <= 10; i++ {
		cmd := fmt.Sprintf("op%d", i)
		cfg.waitCommit(i, servers, cmd)
	}
}

// TestSnapshotBasic tests basic snapshot functionality
func TestSnapshotBasic(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, true)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Submit many commands to trigger snapshots
	for i := 1; i <= 50; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers, true)
	}

	// Restart all servers
	cfg.restart()

	// Submit a few more commands
	for i := 51; i <= 60; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers, true)
	}
	
	// Check that all servers have the last few commands
	for i := 55; i <= 60; i++ {
		cmd := fmt.Sprintf("op%d", i)
		cfg.waitCommit(i, servers, cmd)
	}
}

// TestSnapshotRecovery tests recovery from snapshots after failures
func TestSnapshotRecovery(t *testing.T) {
	servers := 3
	cfg := makeTestConfig(t, servers, true)
	defer cfg.cleanup()
	
	// Start all servers
	cfg.start()

	// Submit commands to trigger snapshots
	for i := 1; i <= 50; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers, true)
	}

	// Find the leader
	leader := cfg.checkOneLeader()
	
	// Disconnect a follower
	follower := (leader + 1) % servers
	cfg.disconnect(follower)

	// Submit more commands
	for i := 51; i <= 100; i++ {
		cfg.one(fmt.Sprintf("op%d", i), servers-1, true)
	}

	// Reconnect the follower
	cfg.connect(follower)
	
	// Wait for the follower to catch up
	time.Sleep(500 * time.Millisecond)
	
	// Submit one more command to ensure the follower has caught up
	cfg.one("final", servers, true)
	
	// Check that the follower has all the latest commands
	for i := 95; i <= 100; i++ {
		cmd := fmt.Sprintf("op%d", i)
		cfg.waitCommit(i, servers, cmd)
	}
	cfg.waitCommit(101, servers, "final")
}

//
// Test infrastructure
//

type testConfig struct {
	t           *testing.T
	servers     int
	rafts       []*Raft
	peers       [][]RaftPeer
	connected   []bool
	saved       []*MemoryPersister
	endnames    [][]string
	logs        []*log.Logger
	mu          sync.Mutex
	applyCh     []chan ApplyMsg
	start       time.Time
	t0          time.Time
	maxraftstate int
}

// makeTestConfig creates a test configuration
func makeTestConfig(t *testing.T, n int, snapshot bool) *testConfig {
	cfg := &testConfig{}
	cfg.t = t
	cfg.servers = n
	cfg.connected = make([]bool, n)
	cfg.endnames = make([][]string, n)
	cfg.peers = make([][]RaftPeer, n)
	cfg.saved = make([]*MemoryPersister, n)
	cfg.applyCh = make([]chan ApplyMsg, n)
	cfg.logs = make([]*log.Logger, n)
	cfg.start = time.Now()
	cfg.t0 = time.Now()
	cfg.maxraftstate = 1000
	
	if !snapshot {
		cfg.maxraftstate = -1 // Disable snapshots
	}

	// Create peer network
	for i := 0; i < n; i++ {
		cfg.logs[i] = log.New(os.Stderr, fmt.Sprintf("[Test Server %d] ", i), log.Ltime|log.Lmicroseconds)
		cfg.applyCh[i] = make(chan ApplyMsg)
		cfg.saved[i] = NewMemoryPersister()
		cfg.peers[i] = make([]RaftPeer, n)
		cfg.endnames[i] = make([]string, n)
		for j := 0; j < n; j++ {
			cfg.endnames[i][j] = fmt.Sprintf("server-%d-%d", i, j)
		}
	}

	// Create RPC connections
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				cfg.peers[i][j] = &testRPCClient{
					cfg:      cfg,
					me:       i,
					peer:     j,
					endname:  cfg.endnames[i][j],
					persister: cfg.saved[j],
				}
			}
		}
	}

	return cfg
}

// start starts all Raft instances
func (cfg *testConfig) start() {
	for i := 0; i < cfg.servers; i++ {
		cfg.rafts[i] = NewRaft(cfg.peers[i], i, cfg.saved[i], cfg.applyCh[i])
		cfg.connected[i] = true
	}
}

// cleanup cleans up the test configuration
func (cfg *testConfig) cleanup() {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.rafts[i] != nil {
			cfg.rafts[i].Kill()
		}
	}
}

// checkOneLeader checks that there is exactly one leader
func (cfg *testConfig) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaders := make(map[int][]int)
		for i := 0; i < cfg.servers; i++ {
			if cfg.connected[i] {
				term, isLeader := cfg.rafts[i].GetState()
				if isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaders {
			if len(leaders) > 1 {
				cfg.t.Fatalf("term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaders) != 0 {
			return leaders[lastTermWithLeader][0]
		}
	}
	cfg.t.Fatalf("expected one leader, got none")
	return -1
}

// one submits a command to the leader and waits for it to be committed
func (cfg *testConfig) one(cmd interface{}, expectedServers int, retry bool) interface{} {
	t0 := time.Now()
	starts := 0
	for time.Since(t0).Seconds() < 10 {
		// Try to find a server that will accept the command
		index := -1
		for si := 0; si < cfg.servers; si++ {
			starts = (starts + 1) % cfg.servers
			if cfg.connected[starts] {
				index, _, isLeader := cfg.rafts[starts].Start(cmd)
				if isLeader {
					break
				}
			}
		}

		if index != -1 {
			// Wait for the command to be committed
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				committed := cfg.checkCommitted(index, expectedServers, cmd)
				if committed {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
		}

		if !retry {
			cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

// waitCommit waits for a specific entry to be committed on the specified number of servers
func (cfg *testConfig) waitCommit(index int, servers int, cmd interface{}) {
	t0 := time.Now()
	for time.Since(t0).Seconds() < 10 {
		if cfg.checkCommitted(index, servers, cmd) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cfg.t.Fatalf("waitCommit(%v, %v, %v) failed", index, servers, cmd)
}

// checkCommitted checks if a specific entry is committed on the specified number of servers
func (cfg *testConfig) checkCommitted(index int, servers int, cmd interface{}) bool {
	count := 0
	for i := 0; i < cfg.servers; i++ {
		if cfg.connected[i] {
			ok := false
			for j := 0; j < len(cfg.applyCh[i]); j++ {
				msg := <-cfg.applyCh[i]
				if msg.CommandValid && msg.CommandIndex == index && msg.Command == cmd {
					ok = true
				}
			}
			if ok {
				count++
			}
		}
	}
	return count >= servers
}

// disconnect disconnects a server from the network
func (cfg *testConfig) disconnect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	
	cfg.connected[i] = false
	
	// Inform the server that it's been disconnected
	if cfg.rafts[i] != nil {
		cfg.rafts[i].Kill()
		cfg.rafts[i] = nil
	}
}

// connect reconnects a server to the network
func (cfg *testConfig) connect(i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	
	cfg.connected[i] = true
	
	// Restart the server
	cfg.rafts[i] = NewRaft(cfg.peers[i], i, cfg.saved[i], cfg.applyCh[i])
}

// restart restarts all servers
func (cfg *testConfig) restart() {
	for i := 0; i < cfg.servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}
}

// testRPCClient implements the RaftPeer interface for testing
type testRPCClient struct {
	cfg      *testConfig
	me       int
	peer     int
	endname  string
	persister *MemoryPersister
}

// RequestVote implements the RequestVote RPC
func (tc *testRPCClient) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if tc.cfg.connected[tc.peer] {
		err := tc.cfg.rafts[tc.peer].RequestVote(args, reply)
		return err
	}

	return fmt.Errorf("disconnected")
}

// AppendEntries implements the AppendEntries RPC
func (tc *testRPCClient) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if tc.cfg.connected[tc.peer] {
		err := tc.cfg.rafts[tc.peer].AppendEntries(args, reply)
		return err
	}

	return fmt.Errorf("disconnected")
}

// InstallSnapshot implements the InstallSnapshot RPC
func (tc *testRPCClient) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	tc.cfg.mu.Lock()
	defer tc.cfg.mu.Unlock()

	if tc.cfg.connected[tc.peer] {
		err := tc.cfg.rafts[tc.peer].InstallSnapshot(args, reply)
		return err
	}

	return fmt.Errorf("disconnected")
}
