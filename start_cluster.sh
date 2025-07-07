#!/bin/bash

# This script starts a 3-node Raft cluster for testing

# Clean up function
cleanup() {
    echo "Stopping cluster..."
    kill $PID1 $PID2 $PID3 2>/dev/null
    wait
    echo "Cluster stopped"
    exit 0
}

# Setup signal handling
trap cleanup SIGINT SIGTERM

# Clean up previous data directories
rm -rf data

# Create data directories
mkdir -p data/node1 data/node2 data/node3

echo "Building kvstore..."
go build -o kvstore main.go

# Start the cluster in background
echo "Starting node 1..."
./kvstore -id 1 -port 8001 -cluster "localhost:8001,localhost:8002,localhost:8003" -datadir data/node1 -v > node1.log 2>&1 &
PID1=$!

echo "Starting node 2..."
./kvstore -id 2 -port 8002 -cluster "localhost:8001,localhost:8002,localhost:8003" -datadir data/node2 -v > node2.log 2>&1 &
PID2=$!

echo "Starting node 3..."
./kvstore -id 3 -port 8003 -cluster "localhost:8001,localhost:8002,localhost:8003" -datadir data/node3 -v > node3.log 2>&1 &
PID3=$!

echo "Cluster is running with PIDs: $PID1, $PID2, $PID3"
echo "Press Ctrl+C to stop the cluster"

# Wait a bit for the cluster to initialize
sleep 2

# Check if nodes are still running
for pid in $PID1 $PID2 $PID3; do
    if ! kill -0 $pid 2>/dev/null; then
        echo "Warning: Process $pid is not running"
    fi
done

echo "Cluster is ready for client operations"
echo "To run client tests: ./test_client.sh"
echo "To run performance tests: ./performance_test.sh"

# Wait for all processes to finish
wait
