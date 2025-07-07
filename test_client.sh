#!/bin/bash

# This script runs basic client operations against a running Raft cluster

echo "Testing client operations..."

# Build the client if needed
if [ ! -f ./kvstore ]; then
    echo "Building kvstore..."
    go build -o kvstore main.go
fi

# Put operations
echo "Putting key1=value1"
./kvstore -client -op put -key key1 -value value1

echo "Putting key2=value2"
./kvstore -client -op put -key key2 -value value2

# Get operations
echo "Getting key1"
./kvstore -client -op get -key key1

echo "Getting key2"
./kvstore -client -op get -key key2

# Delete operations
echo "Deleting key1"
./kvstore -client -op delete -key key1

echo "Getting key1 (should fail)"
./kvstore -client -op get -key key1

echo "Client test complete!"

# Continue to iterate?
read -p "Continue to iterate? (y/n) " answer
if [ "$answer" != "y" ]; then
    exit 0
fi

# More tests
echo "Putting 10 more keys..."
for i in {1..10}; do
    ./kvstore -client -op put -key "iter-key$i" -value "iter-value$i"
done

echo "Getting the 10 keys..."
for i in {1..10}; do
    ./kvstore -client -op get -key "iter-key$i"
done

echo "Iteration complete!"
