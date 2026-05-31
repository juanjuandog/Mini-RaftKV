#!/usr/bin/env sh
set -eu

ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p bin data logs
go build -o bin/raftkv ./cmd/raftkv
go build -o bin/raftkvctl ./cmd/raftkvctl

DEMO_ID="$(date +%s)"
CLIENT_ID="failover-$DEMO_ID"

N1=""
N2=""
N3=""

cleanup() {
  for pid in ${N1:-} ${N2:-} ${N3:-}; do
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}
trap cleanup EXIT INT TERM

status_field() {
  addr="$1"
  field="$2"
  curl -fsS "http://$addr/debug/status" | sed -n "s/.*\"$field\":\"\\([^\"]*\\)\".*/\\1/p"
}

wait_for_leader() {
  deadline=$((SECONDS + 20))
  while [ "$SECONDS" -lt "$deadline" ]; do
    for http_addr in 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003; do
      state="$(status_field "$http_addr" state 2>/dev/null || true)"
      node_id="$(status_field "$http_addr" id 2>/dev/null || true)"
      if [ "$state" = "Leader" ] && [ -n "$node_id" ]; then
        printf '%s\n' "$node_id"
        return 0
      fi
    done
    sleep 1
  done
  echo "leader election timed out" >&2
  return 1
}

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

print_cluster() {
  bin/raftkvctl cluster -http-addrs 127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003 || true
}

bin/raftkv -config configs/n1.yaml > logs/n1.log 2>&1 &
N1=$!
bin/raftkv -config configs/n2.yaml > logs/n2.log 2>&1 &
N2=$!
bin/raftkv -config configs/n3.yaml > logs/n3.log 2>&1 &
N3=$!

echo "cluster started: n1=$N1 n2=$N2 n3=$N3"
echo "debug ui:"
echo "  http://127.0.0.1:9001/debug/ui"
echo "  http://127.0.0.1:9002/debug/ui"
echo "  http://127.0.0.1:9003/debug/ui"

LEADER_ID="$(wait_for_leader)"
echo "initial leader: $LEADER_ID"
print_cluster

echo "== write before failover =="
ctl_first_ok -client "$CLIENT_ID" -request 1 put "before-$DEMO_ID" "ok-before"
ctl_first_ok get "before-$DEMO_ID"

case "$LEADER_ID" in
  n1) kill "$N1"; N1="" ;;
  n2) kill "$N2"; N2="" ;;
  n3) kill "$N3"; N3="" ;;
  *) echo "unknown leader: $LEADER_ID" >&2; exit 1 ;;
esac

echo "leader $LEADER_ID killed, waiting for reelection"
NEW_LEADER_ID="$(wait_for_leader)"
echo "new leader: $NEW_LEADER_ID"
print_cluster

echo "== write after failover =="
ctl_first_ok -client "$CLIENT_ID" -request 2 put "after-$DEMO_ID" "ok-after"
ctl_first_ok get "before-$DEMO_ID"
ctl_first_ok get "after-$DEMO_ID"

echo "failover demo complete"
