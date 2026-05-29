#!/usr/bin/env sh
set -eu

ADDR="${ADDR:-127.0.0.1:7001}"
N="${N:-100}"
CLIENT="bench-$(date +%s)"

mkdir -p bin
go build -o bin/raftkvctl ./cmd/raftkvctl

START="$(date +%s)"
OK=0
FAIL=0

i=1
while [ "$i" -le "$N" ]; do
  if bin/raftkvctl -addr "$ADDR" -client "$CLIENT" -request "$i" put "bench-$i" "$i" >/dev/null 2>&1; then
    OK=$((OK + 1))
  else
    FAIL=$((FAIL + 1))
  fi
  i=$((i + 1))
done

END="$(date +%s)"
DURATION=$((END - START))
if [ "$DURATION" -eq 0 ]; then
  DURATION=1
fi
QPS=$((OK / DURATION))

echo "addr=$ADDR requests=$N ok=$OK fail=$FAIL seconds=$DURATION qps=$QPS"
