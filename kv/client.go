// Package kv implements a distributed key-value store using the Raft consensus algorithm.
package kv

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Client provides a client interface to the distributed key-value store
type Client struct {
	servers   []string   // Addresses of all KV servers
	leader    int        // Index of the last known leader
	mu        sync.Mutex // Protects shared access to client state
	clientID  int64      // Unique client ID
	requestID int64      // Monotonically increasing request ID
	logger    *log.Logger
}

// ClientOptions configures a new Client
type ClientOptions struct {
	// Timeout for RPC requests in milliseconds
	RequestTimeout int
	// Maximum number of retries for a single operation
	MaxRetries int
	// Logger for client operations
	Logger *log.Logger
}

// DefaultClientOptions returns reasonable default options
func DefaultClientOptions() ClientOptions {
	return ClientOptions{
		RequestTimeout: 500,
		MaxRetries:     5,
		Logger:         log.New(os.Stderr, "[KV Client] ", log.Ltime|log.Lmicroseconds),
	}
}

// NewClient creates a new client for the key-value store
func NewClient(servers []string, opts ...ClientOptions) *Client {
	// Use default options if none provided
	options := DefaultClientOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	// Generate a random client ID
	rand.Seed(time.Now().UnixNano())
	clientID := rand.Int63()

	return &Client{
		servers:   servers,
		leader:    0,
		clientID:  clientID,
		requestID: 0,
		logger:    options.Logger,
	}
}

// Get retrieves the value for a key
func (c *Client) Get(key string) (string, error) {
	c.mu.Lock()
	requestID := c.requestID
	c.requestID++
	c.mu.Unlock()

	args := &GetArgs{
		Key:       key,
		ClientID:  c.clientID,
		RequestID: requestID,
	}

	var reply GetReply
	if err := c.sendRequest("KVService.Get", args, &reply); err != nil {
		return "", err
	}

	if reply.Err != "" {
		return "", fmt.Errorf(reply.Err)
	}

	return reply.Value, nil
}

// Put sets a key-value pair
func (c *Client) Put(key, value string) error {
	c.mu.Lock()
	requestID := c.requestID
	c.requestID++
	c.mu.Unlock()

	args := &PutArgs{
		Key:       key,
		Value:     value,
		ClientID:  c.clientID,
		RequestID: requestID,
	}

	var reply PutReply
	if err := c.sendRequest("KVService.Put", args, &reply); err != nil {
		return err
	}

	if reply.Err != "" {
		return fmt.Errorf(reply.Err)
	}

	return nil
}

// Delete removes a key
func (c *Client) Delete(key string) error {
	c.mu.Lock()
	requestID := c.requestID
	c.requestID++
	c.mu.Unlock()

	args := &DeleteArgs{
		Key:       key,
		ClientID:  c.clientID,
		RequestID: requestID,
	}

	var reply DeleteReply
	if err := c.sendRequest("KVService.Delete", args, &reply); err != nil {
		return err
	}

	if reply.Err != "" {
		return fmt.Errorf(reply.Err)
	}

	return nil
}

// sendRequest sends an RPC request to the servers, trying each one until success
func (c *Client) sendRequest(method string, args interface{}, reply interface{}) error {
	c.mu.Lock()
	leader := c.leader
	c.mu.Unlock()

	maxRetries := 5
	for retry := 0; retry < maxRetries; retry++ {
		// Try each server, starting with the last known leader
		for i := 0; i < len(c.servers); i++ {
			serverIndex := (leader + i) % len(c.servers)
			server := c.servers[serverIndex]

			// Connect to the server
			client, err := rpc.DialHTTP("tcp", server)
			if err != nil {
				c.logger.Printf("failed to connect to server %s: %v", server, err)
				continue
			}

			// Set a timeout for the RPC call
			call := client.Go(method, args, reply, nil)
			select {
			case <-call.Done:
				client.Close()
				if call.Error != nil {
					c.logger.Printf("RPC call to %s on server %s failed: %v", method, server, call.Error)
					continue
				}

				// Check if the server is not the leader
				isWrongLeader := false
				switch r := reply.(type) {
				case *GetReply:
					isWrongLeader = r.WrongLeader
				case *PutReply:
					isWrongLeader = r.WrongLeader
				case *DeleteReply:
					isWrongLeader = r.WrongLeader
				}

				if isWrongLeader {
					c.logger.Printf("server %s is not the leader", server)
					continue
				}

				// Successfully executed the request
				c.mu.Lock()
				c.leader = serverIndex
				c.mu.Unlock()
				return nil

			case <-time.After(500 * time.Millisecond):
				client.Close()
				c.logger.Printf("RPC call to %s on server %s timed out", method, server)
				continue
			}
		}

		// All servers failed, wait a bit before retrying
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("failed to execute request after multiple retries")
}

// GetArgs contains the arguments for the Get RPC
type GetArgs struct {
	Key       string
	ClientID  int64
	RequestID int64
}

// GetReply contains the result of the Get RPC
type GetReply struct {
	WrongLeader bool
	Err         string
	Value       string
}

// PutArgs contains the arguments for the Put RPC
type PutArgs struct {
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

// PutReply contains the result of the Put RPC
type PutReply struct {
	WrongLeader bool
	Err         string
}

// DeleteArgs contains the arguments for the Delete RPC
type DeleteArgs struct {
	Key       string
	ClientID  int64
	RequestID int64
}

// DeleteReply contains the result of the Delete RPC
type DeleteReply struct {
	WrongLeader bool
	Err         string
}

// BatchOperation represents a batch of operations to be executed atomically
type BatchOperation struct {
	Operations []Operation
}

// Operation represents a single operation in a batch
type Operation struct {
	Type  string // "Get", "Put", or "Delete"
	Key   string
	Value string
}

// BatchResult represents the results of a batch operation
type BatchResult struct {
	Results []OperationResult
	Err     error
}

// OperationResult represents the result of a single operation
type OperationResult struct {
	Value string
	Err   string
}

// ExecuteBatch executes a batch of operations atomically
func (c *Client) ExecuteBatch(batch BatchOperation) (*BatchResult, error) {
	// Convert batch to JSON
	batchJSON, err := json.Marshal(batch)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch: %v", err)
	}

	// Use Put with a special key to execute the batch
	batchKey := fmt.Sprintf("__batch_%d_%d", c.clientID, time.Now().UnixNano())
	if err := c.Put(batchKey, string(batchJSON)); err != nil {
		return nil, fmt.Errorf("failed to execute batch: %v", err)
	}

	// Get the results
	resultJSON, err := c.Get(batchKey + "_result")
	if err != nil {
		return nil, fmt.Errorf("failed to get batch results: %v", err)
	}

	// Parse the results
	var result BatchResult
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch results: %v", err)
	}

	// Clean up
	c.Delete(batchKey)
	c.Delete(batchKey + "_result")

	return &result, nil
}
