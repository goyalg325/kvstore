// Package raft implements the Raft consensus algorithm for distributed systems.
// This implementation follows the Raft paper by Diego Ongaro and John Ousterhout:
// "In Search of an Understandable Consensus Algorithm"
package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Role represents the role of a Raft server
type Role int

const (
	// Follower is the initial role of a Raft server
	Follower Role = iota
	// Candidate is the role when a server is campaigning for leadership
	Candidate
	// Leader is the role when a server has been elected leader
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// ApplyMsg is the message sent by Raft to the service (or tester) to apply
// a committed log entry to the state machine.
type ApplyMsg struct {
	CommandValid bool        // True if this contains a command to apply
	Command      interface{} // The command to apply
	CommandIndex int         // The index of this command in the log

	// For snapshots:
	SnapshotValid bool   // True if this contains a snapshot
	Snapshot      []byte // The snapshot data
	SnapshotTerm  int    // The term when the snapshot was created
	SnapshotIndex int    // The index of the last entry in the snapshot
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int         // Term when entry was received by leader
	Command interface{} // Command for state machine
	Index   int         // Index of entry in the log
}

// Configuration constants
const (
	// Timeout constants in milliseconds
	MinElectionTimeout  = 150
	MaxElectionTimeout  = 300
	HeartbeatInterval   = 50
	RPCTimeout          = 100
	MaxRPCRetries       = 3
	PersistenceInterval = 100
	SnapshotThreshold   = 1000 // Number of log entries before considering a snapshot
)

// Raft is the consensus module that implements the Raft algorithm
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []RaftPeer          // RPC endpoints of all peers
	persister Persister           // Object to persist Raft state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()
	logger    *log.Logger         // Logger for this Raft instance
	applyCh   chan ApplyMsg       // Channel to send applied commands
	role      Role                // Current role: follower, candidate or leader
	applyCond *sync.Cond          // Condition variable for applying log entries

	// Persistent state on all servers
	currentTerm int        // Latest term server has seen
	votedFor    int        // CandidateId that received vote in current term (or -1)
	log         []LogEntry // Log entries

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // For each server, index of the next log entry to send
	matchIndex []int // For each server, index of highest log entry known to be replicated

	// Election timer
	electionTimer     *time.Timer
	electionTimeout   time.Duration
	heartbeatTimer    *time.Timer
	heartbeatInterval time.Duration

	// Snapshot state
	lastIncludedIndex int // The index of the last entry in the snapshot
	lastIncludedTerm  int // The term of the last entry in the snapshot
}

// RaftPeer defines the interface for a Raft peer
type RaftPeer interface {
	// RequestVote is called by candidates to gather votes
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
	
	// AppendEntries is called by the leader to replicate log entries and as a heartbeat
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error
	
	// InstallSnapshot is called by the leader to send chunks of a snapshot to a follower
	InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error
}

// RequestVoteArgs contains the arguments for the RequestVote RPC
type RequestVoteArgs struct {
	Term         int // Candidate's term
	CandidateID  int // Candidate requesting vote
	LastLogIndex int // Index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// RequestVoteReply contains the results for the RequestVote RPC
type RequestVoteReply struct {
	Term        int  // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs contains the arguments for the AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        // Leader's term
	LeaderID     int        // So follower can redirect clients
	PrevLogIndex int        // Index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit int        // Leader's commitIndex
}

// AppendEntriesReply contains the results for the AppendEntries RPC
type AppendEntriesReply struct {
	Term          int  // Current term, for leader to update itself
	Success       bool // True if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // The first index it stores for the conflicting term
	ConflictTerm  int  // The term of the conflicting entry
}

// InstallSnapshotArgs contains the arguments for the InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int    // Leader's term
	LeaderID          int    // So follower can redirect clients
	LastIncludedIndex int    // The snapshot replaces all entries up through this index
	LastIncludedTerm  int    // Term of lastIncludedIndex
	Data              []byte // Raw snapshot data
	Done              bool   // True if this is the last chunk
}

// InstallSnapshotReply contains the results for the InstallSnapshot RPC
type InstallSnapshotReply struct {
	Term int // Current term, for leader to update itself
}

// Persister interface defines the persistence layer for Raft state
type Persister interface {
	// Save Raft state and snapshot
	SaveStateAndSnapshot(state []byte, snapshot []byte)
	
	// Read persisted Raft state
	ReadRaftState() []byte
	
	// Read persisted snapshot
	ReadSnapshot() []byte
	
	// Return size of persisted Raft state
	RaftStateSize() int
}

// NewRaft creates a new Raft server
func NewRaft(peers []RaftPeer, me int, persister Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.dead = 0
	rf.logger = log.New(os.Stderr, fmt.Sprintf("[Raft %d] ", me), log.Ltime|log.Lmicroseconds)
	rf.applyCond = sync.NewCond(&rf.mu)

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Initialize volatile state
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.role = Follower
	rf.votedFor = -1
	rf.heartbeatInterval = time.Duration(HeartbeatInterval) * time.Millisecond
	
	// Initialize election timer with random timeout
	rf.resetElectionTimer()
	
	// Start background goroutines
	go rf.ticker()
	go rf.applier()

	rf.logger.Printf("initialized server with %d peers", len(peers))
	return rf
}

// Start adds a new command to the Raft log
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If not leader, return immediately
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	// Add command to log
	index := rf.getLastLogIndex() + 1
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   index,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	rf.logger.Printf("appending command at index %d in term %d", index, rf.currentTerm)
	rf.persist()

	// Replicate log to followers
	go rf.replicateLogToFollowers()

	return index, rf.currentTerm, true
}

// GetState returns the current term and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.role == Leader
}

// Kill marks this Raft instance as killed (for testing)
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.logger.Printf("server killed")
}

// killed returns true if this server was killed
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// resetElectionTimer resets the election timer with a random timeout
func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
	rf.electionTimeout = timeout

	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeout)
	} else {
		if !rf.electionTimer.Stop() {
			// Drain the timer channel if stop returns false
			select {
			case <-rf.electionTimer.C:
			default:
			}
		}
		rf.electionTimer.Reset(timeout)
	}
}

// ticker is a background goroutine that triggers timeouts
func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.role != Leader {
				rf.startElection()
			}
			rf.resetElectionTimer()
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// startElection begins a new election round
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.persist()

	term := rf.currentTerm
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	rf.logger.Printf("starting election for term %d, last log index: %d, last log term: %d", 
		term, lastLogIndex, lastLogTerm)

	// Vote for self
	votes := 1
	voteCh := make(chan bool, len(rf.peers))

	// Request votes from all other servers
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}

			// Send RequestVote RPC
			if err := rf.peers[server].RequestVote(args, reply); err != nil {
				rf.logger.Printf("RequestVote RPC to server %d failed: %v", server, err)
				voteCh <- false
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// If we've moved on to a new term, ignore the response
			if rf.currentTerm != term || rf.role != Candidate {
				voteCh <- false
				return
			}

			// If the RPC response contains a higher term, revert to follower
			if reply.Term > rf.currentTerm {
				rf.logger.Printf("discovered higher term %d from server %d", reply.Term, server)
				rf.becomeFollower(reply.Term)
				voteCh <- false
				return
			}

			voteCh <- reply.VoteGranted
		}(i)
	}

	// Count votes
	go func() {
		for i := 0; i < len(rf.peers)-1; i++ {
			if <-voteCh {
				rf.mu.Lock()
				votes++
				
				// If we've moved on to a new term or role, ignore the vote
				if rf.currentTerm != term || rf.role != Candidate {
					rf.mu.Unlock()
					return
				}

				// If we have a majority, become leader
				if votes > len(rf.peers)/2 {
					rf.logger.Printf("won election for term %d with %d votes", term, votes)
					rf.becomeLeader()
				}
				rf.mu.Unlock()
			}
		}
	}()
}

// becomeLeader transitions this server to the leader role
func (rf *Raft) becomeLeader() {
	if rf.role == Candidate {
		rf.role = Leader
		rf.logger.Printf("becoming leader for term %d", rf.currentTerm)

		// Initialize leader state
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		nextIndex := rf.getLastLogIndex() + 1
		for i := range rf.peers {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = rf.getLastLogIndex()

		// Start sending heartbeats immediately
		rf.heartbeatTimer = time.NewTimer(0)
		go rf.sendHeartbeats()
	}
}

// becomeFollower transitions this server to the follower role
func (rf *Raft) becomeFollower(term int) {
	rf.logger.Printf("becoming follower in term %d", term)
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimer()
}

// sendHeartbeats sends heartbeats to all followers
func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		<-rf.heartbeatTimer.C

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.currentTerm
		rf.mu.Unlock()

		// Send AppendEntries RPCs to all followers
		rf.replicateLogToFollowers()

		rf.mu.Lock()
		// Only continue if still leader of same term
		if rf.role == Leader && rf.currentTerm == term {
			rf.heartbeatTimer.Reset(rf.heartbeatInterval)
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

// replicateLogToFollowers sends AppendEntries to all followers to replicate log entries
func (rf *Raft) replicateLogToFollowers() {
	rf.mu.RLock()
	if rf.role != Leader {
		rf.mu.RUnlock()
		return
	}

	term := rf.currentTerm
	rf.mu.RUnlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.replicateLogToServer(server, term)
		}(i)
	}

	// After attempting replication, try to advance commitIndex
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If no longer leader or term changed, don't advance commit index
	if rf.role != Leader || rf.currentTerm != term {
		return
	}

	// Find the largest N such that a majority of matchIndex[i] ≥ N and log[N].term == currentTerm
	for n := rf.commitIndex + 1; n <= rf.getLastLogIndex(); n++ {
		if rf.getLogTerm(n) != rf.currentTerm {
			continue
		}

		count := 1 // Count self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.logger.Printf("advanced commit index to %d", n)
			rf.applyCond.Broadcast()
		}
	}
}

// replicateLogToServer sends AppendEntries to a specific follower
func (rf *Raft) replicateLogToServer(server int, term int) {
	rf.mu.Lock()
	
	// If no longer leader or term changed, don't replicate
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	// If nextIndex is before snapshot, send InstallSnapshot RPC instead
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		rf.sendSnapshot(server)
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.getLogTerm(prevLogIndex)
	entries := rf.getLogEntriesAfter(prevLogIndex)
	
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	
	rf.logger.Printf("sending AppendEntries to server %d: prevIndex=%d, prevTerm=%d, entries=%d", 
		server, prevLogIndex, prevLogTerm, len(entries))
	
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if err := rf.peers[server].AppendEntries(args, reply); err != nil {
		rf.logger.Printf("AppendEntries to server %d failed: %v", server, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If term has changed or no longer leader, ignore response
	if rf.currentTerm != term || rf.role != Leader {
		return
	}

	// If the RPC response contains a higher term, revert to follower
	if reply.Term > rf.currentTerm {
		rf.logger.Printf("discovered higher term %d from server %d", reply.Term, server)
		rf.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Update matchIndex and nextIndex on success
		newMatchIndex := prevLogIndex + len(entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
			rf.nextIndex[server] = newMatchIndex + 1
			rf.logger.Printf("updated server %d: matchIndex=%d, nextIndex=%d", 
				server, rf.matchIndex[server], rf.nextIndex[server])
		}
	} else {
		// Optimization: use conflict information to update nextIndex faster
		if reply.ConflictTerm > 0 {
			// Find the last entry with ConflictTerm in leader's log
			lastTermIndex := -1
			for i := rf.getLastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.getLogTerm(i) == reply.ConflictTerm {
					lastTermIndex = i
					break
				}
			}

			if lastTermIndex > 0 {
				// Leader has ConflictTerm, set nextIndex to the index after lastTermIndex
				rf.nextIndex[server] = lastTermIndex + 1
			} else {
				// Leader doesn't have ConflictTerm, set nextIndex to ConflictIndex
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			// Use ConflictIndex directly when ConflictTerm is 0
			rf.nextIndex[server] = reply.ConflictIndex
		}

		// Ensure nextIndex doesn't go below lastIncludedIndex+1
		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
		}

		rf.logger.Printf("AppendEntries to server %d failed, updated nextIndex to %d", 
			server, rf.nextIndex[server])
	}
}

// sendSnapshot sends an InstallSnapshot RPC to a follower
func (rf *Raft) sendSnapshot(server int) {
	rf.mu.Lock()
	
	// If no longer leader, don't send snapshot
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
		Done:              true,
	}
	
	rf.logger.Printf("sending InstallSnapshot to server %d: lastIndex=%d, lastTerm=%d", 
		server, rf.lastIncludedIndex, rf.lastIncludedTerm)
	
	rf.mu.Unlock()

	reply := &InstallSnapshotReply{}
	if err := rf.peers[server].InstallSnapshot(args, reply); err != nil {
		rf.logger.Printf("InstallSnapshot to server %d failed: %v", server, err)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If no longer leader, ignore response
	if rf.role != Leader {
		return
	}

	// If the RPC response contains a higher term, revert to follower
	if reply.Term > rf.currentTerm {
		rf.logger.Printf("discovered higher term %d from server %d", reply.Term, server)
		rf.becomeFollower(reply.Term)
		return
	}

	// Update nextIndex and matchIndex on success
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	rf.matchIndex[server] = rf.lastIncludedIndex
	rf.logger.Printf("updated server %d after snapshot: matchIndex=%d, nextIndex=%d", 
		server, rf.matchIndex[server], rf.nextIndex[server])
}

// applier is a background goroutine that applies committed log entries to the state machine
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		// Copy data to avoid holding lock during channel send
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied

		// Entries to apply
		entriesToApply := make([]ApplyMsg, 0, commitIndex-lastApplied)

		for i := lastApplied + 1; i <= commitIndex; i++ {
			if i <= rf.lastIncludedIndex {
				// Skip entries included in the snapshot
				continue
			}

			entry := rf.getLogEntry(i)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: i,
			}
			entriesToApply = append(entriesToApply, msg)
			rf.lastApplied = i
		}

		rf.mu.Unlock()

		// Apply entries without holding the lock
		for _, msg := range entriesToApply {
			rf.applyCh <- msg
			rf.logger.Printf("applied command at index %d", msg.CommandIndex)
		}
	}
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Update current term if needed
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Check if vote can be granted
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLastLogTerm()

	// Check if candidate's log is at least as up-to-date as receiver's log
	upToDate := false
	if args.LastLogTerm > lastLogTerm {
		upToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		upToDate = true
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.logger.Printf("granting vote to candidate %d in term %d", args.CandidateID, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer()
	} else {
		if !upToDate {
			rf.logger.Printf("denying vote to candidate %d in term %d: log not up-to-date "+
				"(candidate: index=%d, term=%d; me: index=%d, term=%d)", 
				args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm, 
				lastLogIndex, lastLogTerm)
		} else {
			rf.logger.Printf("denying vote to candidate %d in term %d: already voted for %d", 
				args.CandidateID, args.Term, rf.votedFor)
		}
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return nil
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Set default values
	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = 0
	reply.ConflictTerm = 0

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		rf.logger.Printf("rejecting AppendEntries from %d: term %d < current term %d", 
			args.LeaderID, args.Term, rf.currentTerm)
		return nil
	}

	// Update current term if needed and recognize leader
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.role != Follower {
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// Reset election timer since we heard from leader
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		// Follower's log is too short
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = 0
		rf.logger.Printf("rejecting AppendEntries from %d: log too short, need index %d, have %d", 
			args.LeaderID, args.PrevLogIndex, rf.getLastLogIndex())
		return nil
	}

	if args.PrevLogIndex >= rf.lastIncludedIndex && rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		// Term mismatch at prevLogIndex
		reply.ConflictTerm = rf.getLogTerm(args.PrevLogIndex)
		
		// Find the first index with this conflicting term
		for i := rf.lastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
			if rf.getLogTerm(i) == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}

		rf.logger.Printf("rejecting AppendEntries from %d: term mismatch at index %d, "+
			"expected term %d, found term %d", 
			args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, reply.ConflictTerm)
		return nil
	}

	// If we get here, log consistency check passed
	reply.Success = true

	// If new entries to append
	if len(args.Entries) > 0 {
		// Find the first conflicting entry
		newEntries := make([]LogEntry, 0)
		for i, entry := range args.Entries {
			index := args.PrevLogIndex + 1 + i
			
			// If beyond the end of log, just append
			if index > rf.getLastLogIndex() {
				newEntries = append(newEntries, args.Entries[i:]...)
				break
			}
			
			// If entry conflicts with existing entry, delete the rest and append
			if rf.getLogTerm(index) != entry.Term {
				// Truncate log at this point
				if index > rf.lastIncludedIndex {
					rf.log = rf.getLogEntriesUpTo(index - 1)
				}
				newEntries = append(newEntries, args.Entries[i:]...)
				break
			}
		}

		// Append new entries
		if len(newEntries) > 0 {
			rf.log = append(rf.log, newEntries...)
			rf.logger.Printf("appended %d new entries from index %d", 
				len(newEntries), args.PrevLogIndex+1)
			rf.persist()
		}
	}

	// Update commitIndex if leader's commitIndex > local commitIndex
	if args.LeaderCommit > rf.commitIndex {
		// Set commitIndex to min(leaderCommit, index of last new entry)
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		rf.logger.Printf("updated commitIndex to %d", rf.commitIndex)
		rf.applyCond.Broadcast()
	}

	return nil
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return nil
	}

	// Update current term if needed and recognize leader
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.role != Follower {
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// Reset election timer since we heard from leader
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Ignore older snapshots
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.logger.Printf("ignoring snapshot with lastIndex %d, already have %d", 
			args.LastIncludedIndex, rf.lastIncludedIndex)
		return nil
	}

	rf.logger.Printf("installing snapshot with lastIndex %d, lastTerm %d", 
		args.LastIncludedIndex, args.LastIncludedTerm)

	// Update Raft state with snapshot info
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// Discard log entries already included in the snapshot
	newLog := make([]LogEntry, 0)
	
	// If we have later entries, keep them
	for i := args.LastIncludedIndex + 1; i <= rf.getLastLogIndex(); i++ {
		entry := rf.getLogEntry(i)
		if entry.Index > 0 { // Valid entry
			newLog = append(newLog, entry)
		}
	}
	
	// Reset log with a dummy entry at lastIncludedIndex
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		Term:    args.LastIncludedTerm,
		Index:   args.LastIncludedIndex,
		Command: nil, // Dummy command
	}
	
	// Append any remaining log entries
	rf.log = append(rf.log, newLog...)

	// Update volatile state
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	// Save state and snapshot
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	// Send the snapshot to the state machine
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.logger.Printf("sent snapshot to state machine: lastIndex=%d, lastTerm=%d", 
			args.LastIncludedIndex, args.LastIncludedTerm)
	}()

	return nil
}

// SaveSnapshot saves a snapshot of the state machine
func (rf *Raft) SaveSnapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Ignore if snapshot is older than what we already have
	if index <= rf.lastIncludedIndex {
		return
	}

	rf.logger.Printf("saving snapshot at index %d", index)

	// Update snapshot info
	rf.lastIncludedTerm = rf.getLogTerm(index)
	
	// Discard log entries already included in the snapshot
	newLog := make([]LogEntry, 1) // Keep a dummy entry at lastIncludedIndex
	newLog[0] = LogEntry{
		Term:    rf.lastIncludedTerm,
		Index:   index,
		Command: nil, // Dummy command
	}
	
	// Keep only entries after the snapshot point
	for i := index + 1; i <= rf.getLastLogIndex(); i++ {
		entry := rf.getLogEntry(i)
		if entry.Index > 0 { // Valid entry
			newLog = append(newLog, entry)
		}
	}

	// Update lastIncludedIndex after capturing lastIncludedTerm
	rf.lastIncludedIndex = index
	rf.log = newLog

	// Save state and snapshot
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

// getLastLogIndex returns the index of the last log entry
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

// getLastLogTerm returns the term of the last log entry
func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

// getLogTerm returns the term of the log entry at the given index
func (rf *Raft) getLogTerm(index int) int {
	if index < rf.lastIncludedIndex {
		return -1
	}
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	
	// Convert from global index to log array index
	arrayIndex := index - rf.lastIncludedIndex - 1
	if arrayIndex >= 0 && arrayIndex < len(rf.log) {
		return rf.log[arrayIndex].Term
	}
	return -1
}

// getLogEntry returns the log entry at the given index
func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < rf.lastIncludedIndex {
		return LogEntry{} // Invalid entry
	}
	if index == rf.lastIncludedIndex {
		return LogEntry{
			Term:    rf.lastIncludedTerm,
			Index:   rf.lastIncludedIndex,
			Command: nil, // Dummy command
		}
	}
	
	// Convert from global index to log array index
	arrayIndex := index - rf.lastIncludedIndex - 1
	if arrayIndex >= 0 && arrayIndex < len(rf.log) {
		return rf.log[arrayIndex]
	}
	return LogEntry{} // Invalid entry
}

// getLogEntriesAfter returns all log entries after the given index
func (rf *Raft) getLogEntriesAfter(index int) []LogEntry {
	if index < rf.lastIncludedIndex {
		return []LogEntry{} // Invalid index
	}
	
	// Convert from global index to log array index
	arrayIndex := index - rf.lastIncludedIndex - 1
	if arrayIndex >= -1 && arrayIndex < len(rf.log) {
		entries := make([]LogEntry, len(rf.log)-arrayIndex-1)
		copy(entries, rf.log[arrayIndex+1:])
		return entries
	}
	return []LogEntry{} // Invalid index
}

// getLogEntriesUpTo returns all log entries up to and including the given index
func (rf *Raft) getLogEntriesUpTo(index int) []LogEntry {
	if index < rf.lastIncludedIndex {
		return []LogEntry{} // Invalid index
	}
	
	// Convert from global index to log array index
	arrayIndex := index - rf.lastIncludedIndex - 1
	if arrayIndex >= -1 && arrayIndex < len(rf.log) {
		entries := make([]LogEntry, arrayIndex+1)
		if arrayIndex >= 0 {
			copy(entries, rf.log[:arrayIndex+1])
		}
		return entries
	}
	return rf.log // If index is beyond the end, return all entries
}

// persist saves Raft state to stable storage
func (rf *Raft) persist() {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), rf.persister.ReadSnapshot())
}

// encodeState encodes Raft state for persistence
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// readPersist restores Raft state from stable storage
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		// Initialize with default values
		rf.currentTerm = 0
		rf.votedFor = -1
		rf.log = make([]LogEntry, 0)
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}
	
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&log) != nil ||
	   d.Decode(&lastIncludedIndex) != nil ||
	   d.Decode(&lastIncludedTerm) != nil {
		rf.logger.Printf("error decoding persisted state")
		return
	}
	
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	
	rf.logger.Printf("restored state: term=%d, votedFor=%d, logLen=%d, lastIncludedIndex=%d", 
		rf.currentTerm, rf.votedFor, len(rf.log), rf.lastIncludedIndex)
}
