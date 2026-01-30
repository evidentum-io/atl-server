#!/bin/bash
set -e

echo "=============================================="
echo "ATL Server Performance Benchmark"
echo "=============================================="
echo ""
echo "Implementation: atl-server (Rust)"
echo "Benchmark: wrk (12 threads, 500 connections, 30s)"
echo "Endpoint: POST /v1/anchor"
echo ""
echo "Starting server..."

# Start atl-server in background
atl-server &
SERVER_PID=$!

# Wait for server to be ready (check if port is listening)
echo "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s -X POST -H "Content-Type: application/json" \
       -d '{"payload":"healthcheck"}' \
       http://127.0.0.1:3388/v1/anchor > /dev/null 2>&1; then
        echo "Server ready."
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: Server failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

echo ""
echo "Running benchmark..."
echo "----------------------------------------------"
echo ""

# Run benchmark
wrk -t12 -c500 -d30s -s /app/post.lua http://127.0.0.1:3388/v1/anchor --latency

echo ""
echo "----------------------------------------------"
echo "Benchmark complete."
echo ""
echo "Source code: https://github.com/evidentum-io/atl-server"
echo "Protocol:    https://atl-protocol.org"
echo "=============================================="

# Cleanup
kill $SERVER_PID 2>/dev/null || true
