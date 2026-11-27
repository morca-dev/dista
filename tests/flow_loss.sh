#!/bin/bash
# A flow's worker holding finished batches is killed: a head, four workers
# and tests/flow_loss_driver.mojo, as separate processes. The flow's last
# operator waits for a file, so the batches of the one before pile up on the
# workers, too large to go back inline. The script kills the worker that made
# the most of them by its pid, then lets the last operator go. The flow reads
# the inputs it lost batches of again, and its results come out exact, with
# at least one rerun counted.
set -euo pipefail
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/flow_loss_driver.mojo -o build/flow-loss-driver

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

log=build/flow-loss
dir=$PWD/$log.d
rm -rf "$dir" "$log"-*.log
mkdir -p "$dir"
export DISTA_WORKER_SLOTS=4 FLOW_LOSS_DIR=$dir
DISTA_ROLE=head DISTA_PORT=0 ./build/flow-loss-driver > "$log-head.log" 2>&1 &
head=$!; pids+=($head)
port=$(port_in "$log-head.log" "dista head listening")
workers=()
for w in 1 2 3 4; do
  DISTA_ROLE=worker DISTA_HEAD=localhost:$port ./build/flow-loss-driver > "$log-worker$w.log" 2>&1 &
  workers+=($!); pids+=($!)
done
DISTA_HEAD=localhost:$port ./build/flow-loss-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

# The first operator's budget is 8 and the last's 2: once 10 batches are
# made, the read has stopped, and 8 of them wait on their workers.
made() { cat "$dir"/made-* 2>/dev/null | wc -l | tr -d ' '; }
for _ in $(seq 600); do [ "$(made)" -ge 10 ] && break; sleep 0.1; done
[ "$(made)" -ge 10 ] || { echo "FAIL: only $(made) batches made"; cat "$log-driver.log"; exit 1; }
sleep 0.5
victim=$(wc -l "$dir"/made-* | grep -v ' total$' | sort -rn | head -1 | sed 's|.*/made-||')
found=0
for w in "${workers[@]}"; do [ "$w" = "$victim" ] && found=1; done
[ $found = 1 ] || { echo "FAIL: pid $victim isn't one of the workers started here"; exit 1; }
echo "killing worker $victim, which made $(wc -l < "$dir/made-$victim" | tr -d " ") of $(made) batches"
kill -9 "$victim"
touch "$dir/go"

status=0
wait "$driver" || status=$?
if [ $status != 0 ] || ! grep -q '^PASS flow loss' "$log-driver.log"; then
  echo "FAIL: the driver (exit $status)"; cat "$log-driver.log"; tail -20 "$log-head.log"; exit 1
fi
grep '^stats' "$log-driver.log"
grep '^PASS flow loss' "$log-driver.log"
