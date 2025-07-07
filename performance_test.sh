#!/bin/bash

# This script runs a performance test on the KV store

NUM_OPS=1000
CONCURRENT=20  # number of concurrent operations
DURATION=10    # seconds

echo "Running performance test with $NUM_OPS operations ($CONCURRENT concurrent)..."

# Build the client if needed
if [ ! -f ./kvstore ]; then
    echo "Building kvstore..."
    go build -o kvstore main.go
fi

# Start timer
start_time=$(date +%s.%N)

# Run operations in parallel with controlled concurrency
for ((i=0; i<$NUM_OPS; i+=$CONCURRENT)); do
    for ((j=0; j<$CONCURRENT && i+j<$NUM_OPS; j++)); do
        idx=$((i+j))
        ./kvstore -client -op put -key "perf-key-$idx" -value "perf-value-$idx" &
    done
    wait
done

# End timer for put operations
mid_time=$(date +%s.%N)
put_duration=$(echo "$mid_time - $start_time" | bc)

# Verify operations
echo "Verifying operations..."
success=0
failed=0

# Start timer for get operations
get_start_time=$(date +%s.%N)

for ((i=0; i<$NUM_OPS; i+=$CONCURRENT)); do
    for ((j=0; j<$CONCURRENT && i+j<$NUM_OPS; j++)); do
        idx=$((i+j))
        ./kvstore -client -op get -key "perf-key-$idx" > /tmp/kvget-$idx.out 2>/dev/null &
    done
    wait
    
    # Process results
    for ((j=0; j<$CONCURRENT && i+j<$NUM_OPS; j++)); do
        idx=$((i+j))
        value=$(cat /tmp/kvget-$idx.out | grep "value" | awk '{print $NF}')
        if [ "$value" == "perf-value-$idx" ]; then
            success=$((success+1))
        else
            failed=$((failed+1))
        fi
        rm -f /tmp/kvget-$idx.out
    done
done

# End timer for get operations
end_time=$(date +%s.%N)
get_duration=$(echo "$end_time - $get_start_time" | bc)
total_duration=$(echo "$end_time - $start_time" | bc)

# Calculate ops/sec
put_ops_per_sec=$(echo "$NUM_OPS / $put_duration" | bc)
get_ops_per_sec=$(echo "$NUM_OPS / $get_duration" | bc)
total_ops_per_sec=$(echo "($NUM_OPS * 2) / $total_duration" | bc)

echo "Performance test results:"
echo "- Total operations: $((NUM_OPS * 2)) ($NUM_OPS puts, $NUM_OPS gets)"
echo "- Put duration: $put_duration seconds"
echo "- Get duration: $get_duration seconds"
echo "- Total duration: $total_duration seconds"
echo "- Put operations per second: $put_ops_per_sec"
echo "- Get operations per second: $get_ops_per_sec"
echo "- Overall operations per second: $total_ops_per_sec"
echo "- Successful operations: $success"
echo "- Failed operations: $failed"
