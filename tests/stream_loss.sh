#!/bin/bash
# A stream's worker is killed: a head, three workers and
# tests/stream_loss_driver.mojo, as separate processes. Killed before the
# stream answered any item, it runs again on another worker and the driver
# reads every item, exact. Killed after the driver took three, the end fails
# with "worker lost while running this stream, after some of its items were
# answered", and the three are right. With one slot a worker and with four.
set -euo pipefail
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/stream_loss_driver.mojo -o build/stream-loss-driver

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

port_in() {
  for _ in $(seq 100); do
    p=$(sed -n "s/^$2 on port //p" "$1" | head -1)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 0.1
  done
  echo "no '$2' in $1" >&2; cat "$1" >&2; exit 1
}

# Waits up to 60 s for a file to exist.
wait_file() {
  for _ in $(seq 600); do [ -e "$1" ] && return; sleep 0.1; done
  echo "FAIL ($slots slots): no $1"; cat "$log-driver.log"; exit 1
}

for slots in 1 4; do
  log=build/stream-loss-$slots
  dir=$PWD/$log.d
  rm -rf "$dir" "$log"-*.log
  mkdir -p "$dir"
  export DISTA_WORKER_SLOTS=$slots STREAM_LOSS_DIR=$dir
  DISTA_ROLE=head DISTA_PORT=0 ./build/stream-loss-driver > "$log-head.log" 2>&1 &
  head=$!; pids+=($head)
  port=$(port_in "$log-head.log" "dista head listening")
  workers=()
  for w in 1 2 3; do
    DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/stream-loss-driver > "$log-worker$w.log" 2>&1 &
    workers+=($!); pids+=($!)
  done
  DISTA_HEAD=localhost:$port ./build/stream-loss-driver > "$log-driver.log" 2>&1 &
  driver=$!; pids+=($driver)

  # Before any item: kill the worker running it, then let the retry go on.
  wait_file "$dir/1.pid"
  first=$(cat "$dir/1.pid")
  kill -9 "$first"
  touch "$dir/1.go"
  # After three items were taken.
  for _ in $(seq 600); do grep -q '^TAKEN' "$log-driver.log" && break; sleep 0.1; done
  grep -q '^TAKEN' "$log-driver.log" || { echo "FAIL ($slots slots): the driver never took three items"; cat "$log-driver.log"; exit 1; }
  second=$(cat "$dir/2.pid")
  [ "$second" != "$first" ] || { echo "FAIL ($slots slots): the killed worker ran the second stream"; exit 1; }
  kill -9 "$second"

  status=0
  wait "$driver" || status=$?
  retried=$(cat "$dir/1.pid")
  if [ $status != 0 ] || ! grep -q '^PASS before' "$log-driver.log" || ! grep -q '^PASS after' "$log-driver.log"; then
    echo "FAIL ($slots slots): the driver (exit $status)"; cat "$log-driver.log"; tail -20 "$log-head.log"; exit 1
  fi
  [ "$retried" != "$first" ] || { echo "FAIL ($slots slots): the stream didn't run again elsewhere"; exit 1; }
  echo "$slots slots: $(grep '^PASS before' "$log-driver.log" | cut -d: -f2-);$(grep '^PASS after' "$log-driver.log" | cut -d: -f2-)"
  kill -9 "$head" "${workers[@]}" 2>/dev/null || true
  wait "$head" "${workers[@]}" 2>/dev/null || true
  unset DISTA_WORKER_SLOTS STREAM_LOSS_DIR
done
echo "PASS stream loss: a stream killed before its first item ran again elsewhere, exact; one killed after failed its end, its items right"
