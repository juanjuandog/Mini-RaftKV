#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p bin data logs
go build -o bin/raftkv ./cmd/raftkv
go build -o bin/raftkvctl ./cmd/raftkvctl
DEMO_ID="$(date +%s)"
CLIENT_ID="demo-$DEMO_ID"

cleanup() {
  for pid in ${N1:-} ${N2:-} ${N3:-}; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

ctl_first_ok() {
  for addr in 127.0.0.1:7001 127.0.0.1:7002 127.0.0.1:7003; do
    if output="$(bin/raftkvctl -addr "$addr" "$@" 2>/dev/null)"; then
      printf '%s\n' "$output"
      return 0
    fi
  done
  echo "all raftkv nodes rejected command: $*" >&2
  return 1
}

bin/raftkv -config configs/n1.yaml > logs/n1.log 2>&1 &
N1=$!
bin/raftkv -config configs/n2.yaml > logs/n2.log 2>&1 &
N2=$!
bin/raftkv -config configs/n3.yaml > logs/n3.log 2>&1 &
N3=$!

echo "cluster started: n1=$N1 n2=$N2 n3=$N3"
sleep 3

echo "== put through n1 =="
PUT_OUTPUT="$(bin/raftkvctl -addr 127.0.0.1:7001 -client "$CLIENT_ID" -request 1 put "language-$DEMO_ID" go)"
echo "$PUT_OUTPUT"

echo "== get through n2 =="
bin/raftkvctl -addr 127.0.0.1:7002 get "language-$DEMO_ID"

echo "== duplicate request should not overwrite =="
bin/raftkvctl -addr 127.0.0.1:7001 -client "$CLIENT_ID" -request 2 put "once-$DEMO_ID" first
bin/raftkvctl -addr 127.0.0.1:7001 -client "$CLIENT_ID" -request 2 put "once-$DEMO_ID" second || true
bin/raftkvctl -addr 127.0.0.1:7003 get "once-$DEMO_ID"

LEADER_ID="$(printf '%s\n' "$PUT_OUTPUT" | sed -n 's/.*leader=\([^ ]*\).*/\1/p')"
echo "detected leader: ${LEADER_ID:-unknown}"
case "$LEADER_ID" in
  n1) kill "$N1"; N1="" ;;
  n2) kill "$N2"; N2="" ;;
  n3) kill "$N3"; N3="" ;;
  *) echo "skip leader kill because leader was not detected" ;;
esac

if [ -n "${LEADER_ID:-}" ]; then
  echo "leader $LEADER_ID killed, waiting for reelection"
  sleep 4
  echo "== write after failover =="
  ctl_first_ok -client "$CLIENT_ID" -request 3 put "failover-$DEMO_ID" ok
fi

echo "== delete =="
ctl_first_ok -client "$CLIENT_ID" -request 4 delete "language-$DEMO_ID"

echo "demo complete. logs are in logs/"
