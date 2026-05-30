#!/usr/bin/env sh
set -eu

mkdir -p data logs

go run ./cmd/raftkv -config configs/n1.yaml > logs/n1.log 2>&1 &
N1=$!
go run ./cmd/raftkv -config configs/n2.yaml > logs/n2.log 2>&1 &
N2=$!
go run ./cmd/raftkv -config configs/n3.yaml > logs/n3.log 2>&1 &
N3=$!

echo "started n1=$N1 n2=$N2 n3=$N3"
echo "logs: logs/n1.log logs/n2.log logs/n3.log"
echo "metrics: http://127.0.0.1:9001/metrics http://127.0.0.1:9002/metrics http://127.0.0.1:9003/metrics"
echo "debug ui: http://127.0.0.1:9001/debug/ui http://127.0.0.1:9002/debug/ui http://127.0.0.1:9003/debug/ui"
echo "status json: http://127.0.0.1:9001/debug/status"
echo "try: go run ./cmd/raftkvctl -addr 127.0.0.1:7001 put hello raft"
echo "press Ctrl+C to stop"

trap 'kill "$N1" "$N2" "$N3"' INT TERM EXIT
wait
