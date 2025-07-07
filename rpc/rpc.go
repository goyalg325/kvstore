// Package rpc provides the network communication layer for the Raft consensus algorithm.
package rpc

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Client is an RPC client for sending requests to servers
type Client struct {
	mu         sync.Mutex
	serverAddr string
	connection *rpc.Client
	connected  bool
	logger     *log.Logger
}

// NewClient creates a new RPC client for the specified server address
func NewClient(serverAddr string, logger *log.Logger) *Client {
	return &Client{
		serverAddr: serverAddr,
		connected:  false,
		logger:     logger,
	}
}

// Call makes an RPC call to the specified service method
func (c *Client) Call(serviceMethod string, args interface{}, reply interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try to establish connection if not connected
	if !c.connected {
		if err := c.connect(); err != nil {
			return fmt.Errorf("failed to connect to %s: %v", c.serverAddr, err)
		}
	}

	// Make the RPC call with timeout
	callChan := make(chan error, 1)
	go func() {
		callChan <- c.connection.Call(serviceMethod, args, reply)
	}()

	select {
	case err := <-callChan:
		if err != nil {
			c.logger.Printf("RPC call to %s.%s failed: %v", c.serverAddr, serviceMethod, err)
			c.disconnect() // Close connection on error
			return err
		}
		return nil
	case <-time.After(time.Second):
		c.logger.Printf("RPC call to %s.%s timed out", c.serverAddr, serviceMethod)
		c.disconnect() // Close connection on timeout
		return errors.New("RPC call timed out")
	}
}

// connect establishes a connection to the server
func (c *Client) connect() error {
	var err error
	c.connection, err = rpc.DialHTTP("tcp", c.serverAddr)
	if err != nil {
		return err
	}
	c.connected = true
	return nil
}

// disconnect closes the connection to the server
func (c *Client) disconnect() {
	if c.connected && c.connection != nil {
		c.connection.Close()
		c.connected = false
	}
}

// Server is an RPC server that listens for incoming requests
type Server struct {
	listener net.Listener
	server   *rpc.Server
	logger   *log.Logger
	addr     string
	wg       sync.WaitGroup
	mu       sync.Mutex
	stopped  bool
}

// NewServer creates a new RPC server on the specified address
func NewServer(addr string, logger *log.Logger) *Server {
	return &Server{
		server:  rpc.NewServer(),
		logger:  logger,
		addr:    addr,
		stopped: false,
	}
}

// Register registers a service with the RPC server
func (s *Server) Register(rcvr interface{}) error {
	return s.server.Register(rcvr)
}

// RegisterName registers a service with a custom name
func (s *Server) RegisterName(name string, rcvr interface{}) error {
	return s.server.RegisterName(name, rcvr)
}

// Start starts the RPC server
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("server already stopped")
	}

	// Register HTTP handlers for RPC
	s.server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", s.addr, err)
	}

	s.listener = listener
	s.logger.Printf("RPC server listening on %s", s.addr)

	s.wg.Add(1)
	go s.serve()

	return nil
}

// serve handles incoming connections
func (s *Server) serve() {
	defer s.wg.Done()

	// Start serving HTTP
	if err := http.Serve(s.listener, nil); err != nil {
		s.mu.Lock()
		stopped := s.stopped
		s.mu.Unlock()

		if !stopped {
			s.logger.Printf("HTTP server error: %v", err)
		}
	}
}

// Stop stops the RPC server
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}

	s.stopped = true
	if s.listener != nil {
		s.listener.Close()
	}

	s.wg.Wait()
	s.logger.Printf("RPC server on %s stopped", s.addr)
}

// ClientPool manages a pool of RPC clients
type ClientPool struct {
	mu      sync.Mutex
	clients map[string]*Client
	logger  *log.Logger
}

// NewClientPool creates a new client pool
func NewClientPool(logger *log.Logger) *ClientPool {
	return &ClientPool{
		clients: make(map[string]*Client),
		logger:  logger,
	}
}

// GetClient returns a client for the specified server address
func (cp *ClientPool) GetClient(serverAddr string) *Client {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	client, exists := cp.clients[serverAddr]
	if !exists {
		client = NewClient(serverAddr, cp.logger)
		cp.clients[serverAddr] = client
	}

	return client
}

// CloseAll closes all clients in the pool
func (cp *ClientPool) CloseAll() {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	for _, client := range cp.clients {
		client.disconnect()
	}

	cp.clients = make(map[string]*Client)
}
