// Package raft implements the Raft consensus algorithm.
package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister implements the Persister interface using the local filesystem
type FilePersister struct {
	mu           sync.Mutex
	stateFile    string
	snapshotFile string
	raftState    []byte
	snapshot     []byte
}

// NewFilePersister creates a new file-based persister for Raft state
func NewFilePersister(dataDir string, serverID int) (*FilePersister, error) {
	// Create the data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	stateFile := filepath.Join(dataDir, fmt.Sprintf("raft-%d-state", serverID))
	snapshotFile := filepath.Join(dataDir, fmt.Sprintf("raft-%d-snapshot", serverID))

	persister := &FilePersister{
		stateFile:    stateFile,
		snapshotFile: snapshotFile,
		raftState:    []byte{},
		snapshot:     []byte{},
	}

	// Try to load existing state and snapshot
	persister.loadFromDisk()

	return persister, nil
}

// SaveStateAndSnapshot saves both state and snapshot to stable storage
func (fp *FilePersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	fp.raftState = clone(state)
	fp.snapshot = clone(snapshot)

	// Write state to disk
	if err := os.WriteFile(fp.stateFile, state, 0644); err != nil {
		panic("Failed to write Raft state to disk: " + err.Error())
	}

	// Write snapshot to disk if non-empty
	if len(snapshot) > 0 {
		if err := os.WriteFile(fp.snapshotFile, snapshot, 0644); err != nil {
			panic("Failed to write Raft snapshot to disk: " + err.Error())
		}
	}
}

// ReadRaftState returns the saved Raft state
func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return clone(fp.raftState)
}

// ReadSnapshot returns the saved snapshot
func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return clone(fp.snapshot)
}

// RaftStateSize returns the size of the Raft state in bytes
func (fp *FilePersister) RaftStateSize() int {
	fp.mu.Lock()
	defer fp.mu.Unlock()
	return len(fp.raftState)
}

// loadFromDisk loads state and snapshot from disk files
func (fp *FilePersister) loadFromDisk() {
	// Load state if file exists
	if _, err := os.Stat(fp.stateFile); err == nil {
		if data, err := os.ReadFile(fp.stateFile); err == nil {
			fp.raftState = data
		}
	}

	// Load snapshot if file exists
	if _, err := os.Stat(fp.snapshotFile); err == nil {
		if data, err := os.ReadFile(fp.snapshotFile); err == nil {
			fp.snapshot = data
		}
	}
}

// MemoryPersister is an in-memory implementation of the Persister interface
// Useful for testing and debugging
type MemoryPersister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

// NewMemoryPersister creates a new in-memory persister
func NewMemoryPersister() *MemoryPersister {
	return &MemoryPersister{}
}

// SaveStateAndSnapshot saves both state and snapshot to memory
func (mp *MemoryPersister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	mp.raftState = clone(state)
	mp.snapshot = clone(snapshot)
}

// ReadRaftState returns the saved Raft state
func (mp *MemoryPersister) ReadRaftState() []byte {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return clone(mp.raftState)
}

// ReadSnapshot returns the saved snapshot
func (mp *MemoryPersister) ReadSnapshot() []byte {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return clone(mp.snapshot)
}

// RaftStateSize returns the size of the Raft state in bytes
func (mp *MemoryPersister) RaftStateSize() int {
	mp.mu.Lock()
	defer mp.mu.Unlock()
	return len(mp.raftState)
}

// clone makes a deep copy of a byte slice
func clone(original []byte) []byte {
	if original == nil {
		return nil
	}
	clone := make([]byte, len(original))
	copy(clone, original)
	return clone
}
