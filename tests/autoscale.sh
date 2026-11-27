#!/bin/bash
# Autoscaling, end to end on one machine: a `dista head` with no workers, a
# driver submitting 200 tasks of 0.5 s, and a provider loop here that runs
# `dista scale --once` twice a second, starts the workers it asks for as
# processes and drains the ones it names with `dista drain`. The job must
# grow to max_workers (6), in steps of at most max_step, and once it's done
# shrink back to zero; every result exact, no task retried, nothing failed,
# and every drained worker exiting 0.
#
# `tests/autoscale.sh groups` runs two groups instead: 60 tasks of group `b`
# queued with no worker of `b`, beside group `a`'s tasks run one at a time.
# The loop runs `dista scale --once --group` for each and starts workers with
# that `DISTA_GROUP`. `status --json` must count each group's tasks as
# submitted, `b` alone grow to 4 and drain back to 0, and `a` stay at 1.
set -euo pipefail
scenario=${1:-one}
mkdir -p build
scripts/mojo-build.sh -I src ${DEP_INC:--I src} cli/main.mojo -o build/dista
if [ "$scenario" = groups ]; then
  scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/autoscale_groups_driver.mojo -o build/autoscale-groups-driver
else
  scripts/mojo-build.sh -I src ${DEP_INC:--I src} tests/autoscale_driver.mojo -o build/autoscale-driver
fi

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

log=build/autoscale
[ "$scenario" = groups ] && log=build/autoscale-groups
rm -f "$log"-*.log
DISTA_METRICS_PORT=0 ./build/dista head --port 0 > "$log-head.log" 2>&1 &
head=$!; pids+=($head)
port_in() {
  for _ in $(seq 100); do
    p=$(sed -n "s/^$1 on port //p" "$log-head.log" | head -1)
    [ -n "$p" ] && { echo "$p"; return; }
    sleep 0.1
  done
  echo "no '$1' in the head's log" >&2; cat "$log-head.log" >&2; exit 1
}
port=$(port_in "dista head listening")
metrics=$(port_in "dista metrics")
export DISTA_HEAD=localhost:$port
value() { curl -sf "http://127.0.0.1:$metrics/metrics" | awk -v m="$1" '$1 == m {print $2}'; }
joined() { ./build/dista status --json | python3 -c 'import json, sys; print(len(json.load(sys.stdin)["workers"]))'; }

if [ "$scenario" = groups ]; then
  # A field of one group in `status --json`, by a Python expression on g
  # (that group's entry, {} when it isn't listed) and s (the whole status).
  group() {
    ./build/dista status --json | python3 -c "import json, sys
s = json.load(sys.stdin)
g = next((x for x in s['groups'] if x['group'] == '$1'), {})
print($2)"
  }
  wait_for() {  # wait_for <what> <group> <expression>: until it prints True
    for _ in $(seq 200); do
      [ "$(group "$2" "$3")" = True ] && return
      sleep 0.05
    done
    echo "FAIL: $1"; ./build/dista status; exit 1
  }
  ./build/autoscale-groups-driver > "$log-driver.log" 2>&1 &
  driver=$!; pids+=($driver)
  # Before any worker: b's 60 tasks queued and a's first, each in its group.
  wait_for "60 tasks of b and 1 of a queued, no workers" b \
    "g.get('tasks_ready') == 60 and g['workers'] == 0 and s['tasks_ready'] == 61 \
     and [x['tasks_ready'] for x in s['groups'] if x['group'] == 'a'] == [1]"
  ./build/dista status | grep -q '^  b  workers 0  slots 0  ready 60' \
    || { echo "FAIL: dista status shows no group b"; ./build/dista status; exit 1; }
  m="$(value 'dista_group_tasks_ready{group="b"}') $(value 'dista_group_workers{group="b"}') $(value dista_tasks_ready)"
  [ "$m" = "60 0 61" ] || { echo "FAIL: /metrics by group: $m"; exit 1; }
  # a's worker by hand, as its policy (min 1) would start it: a's task runs,
  # b's 60 stay queued.
  workers=(); group_of=()
  start_worker() {
    DISTA_ROLE=worker DISTA_GROUP=$1 ./build/autoscale-groups-driver >> "$log-workers.log" 2>&1 &
    workers+=($!); group_of+=("$1"); pids+=($!)
    started=$((started + 1))
  }
  started=0; drained=0; driver_done=""; a_moved=""; b_back=""
  start_worker a
  wait_for "a's task running on its worker, b's 60 still queued" a \
    "g.get('tasks_running') == 1 and g['tasks_ready'] == 0 and g['workers'] == 1 \
     and [(x['tasks_ready'], x['tasks_running']) for x in s['groups'] if x['group'] == 'b'] == [(60, 0)] \
     and s['tasks_running'] == 1"
  since_a=0; since_b=0; most_a=0; most_b=0
  for tick in $(seq 600); do
    status=$(./build/dista status --json)
    alive_a=0; alive_b=0
    for k in "${!workers[@]}"; do
      if kill -0 "${workers[$k]}" 2>/dev/null; then
        eval "alive_${group_of[$k]}=\$((alive_${group_of[$k]} + 1))"
      fi
    done
    line="tick $tick"
    for g in a b; do
      if [ "$g" = a ]; then policy="--min 1 --max 3"; else policy="--min 0 --max 4"; fi
      n=$(python3 -c "import json, sys; print(sum(w['group'] == '$g' for w in json.loads(sys.argv[1])['workers']))" "$status")
      eval "alive=\$alive_$g; since=\$since_$g; most=\$most_$g"
      pending=$((alive > n ? alive - n : 0))
      out=$(./build/dista scale --once --group "$g" $policy --step 4 --up-after 1 --idle 2 \
        --pending "$pending" --short-since "$since")
      since=$(awk '$1 == "short_since" {print $2}' <<< "$out")
      start=$(awk '$1 == "start" {print $2}' <<< "$out")
      for ((k = 0; k < start; k++)); do start_worker "$g"; done
      for a in $(awk '$1 == "drain" {print $2}' <<< "$out"); do
        ./build/dista drain "$a" > /dev/null
        drained=$((drained + 1))
      done
      total=$((alive + start))
      [ "$total" -gt "$most" ] && most=$total
      # a must stay at exactly its one worker throughout.
      if [ "$g" = a ] && { [ "$total" != 1 ] || [ "$n" != 1 ]; }; then a_moved="tick $tick: $n joined, $total"; fi
      eval "since_$g=$since; most_$g=$most; joined_$g=$n"
      line="$line | $g alive $alive joined $n pending $pending: $(tr '\n' ' ' <<< "$out")"
    done
    echo "$line" >> "$log-ticks.log"
    if [ -z "$driver_done" ] && ! kill -0 "$driver" 2>/dev/null; then driver_done=$tick; fi
    if [ -n "$driver_done" ] && [ "$most_b" -gt 0 ] && [ "$alive_b" = 0 ] && [ "$joined_b" = 0 ]; then
      b_back=$joined_a
      break
    fi
    sleep 0.5
  done
  wait "$driver" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
  grep -q '^PASS autoscale groups driver' "$log-driver.log" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
  # a's worker goes last, by hand: its policy keeps one.
  for k in "${!workers[@]}"; do
    if [ "${group_of[$k]}" = a ] && kill -0 "${workers[$k]}" 2>/dev/null; then
      a_addr=$(python3 -c "import json, sys; print([w['address'] for w in json.loads(sys.argv[1])['workers'] if w['group'] == 'a'][0])" "$(./build/dista status --json)")
      ./build/dista drain "$a_addr" > /dev/null
      drained=$((drained + 1))
    fi
  done
  codes=0
  for w in "${workers[@]}"; do wait "$w" || codes=$((codes + 1)); done
  retried=$(value dista_tasks_retried_total)
  failed=$(value 'dista_tasks_finished_total{outcome="error"}')
  left=$(value dista_workers)
  echo "autoscale groups: a at most $most_a, b at most $most_b; a had $b_back when b was back to 0; started $started, drained $drained; $retried retried, $failed failed, $left left, $codes exited non-zero"
  if [ -n "$a_moved" ] || [ "$most_a" != 1 ] || [ "$most_b" != 4 ] || [ "$b_back" != 1 ] \
    || [ "${left:-1}" != 0 ] || [ "${retried:-1}" != 0 ] || [ "${failed:-1}" != 0 ] || [ "$codes" != 0 ] \
    || [ "$drained" != "$started" ]; then
    echo "FAIL: autoscaling by group ${a_moved:+(a changed at $a_moved)}"; tail -20 "$log-head.log"; exit 1
  fi
  echo "PASS autoscale groups: b grew from 0 to 4 and back to 0 alone, a stayed at 1, every result exact"
  exit 0
fi

./build/autoscale-driver > "$log-driver.log" 2>&1 &
driver=$!; pids+=($driver)

workers=()
since=0
most=0
step=0
started=0
drained=0
driver_done=""
for tick in $(seq 600); do
  alive=0
  for w in "${workers[@]+"${workers[@]}"}"; do kill -0 "$w" 2>/dev/null && alive=$((alive + 1)); done
  n=$(joined)
  pending=$((alive > n ? alive - n : 0))
  out=$(./build/dista scale --once --min 0 --max 6 --step 4 --up-after 1 --idle 2 \
    --pending "$pending" --short-since "$since")
  since=$(awk '$1 == "short_since" {print $2}' <<< "$out")
  start=$(awk '$1 == "start" {print $2}' <<< "$out")
  [ "$start" -gt "$step" ] && step=$start
  for ((k = 0; k < start; k++)); do
    DISTA_ROLE=worker ./build/autoscale-driver >> "$log-workers.log" 2>&1 &
    workers+=($!); pids+=($!)
    started=$((started + 1))
  done
  for a in $(awk '$1 == "drain" {print $2}' <<< "$out"); do
    ./build/dista drain "$a" > /dev/null
    drained=$((drained + 1))
  done
  echo "tick $tick alive $alive joined $n pending $pending: $(tr '\n' ' ' <<< "$out")" >> "$log-ticks.log"
  total=$((alive + start))
  [ "$total" -gt "$most" ] && most=$total
  if [ -z "$driver_done" ] && ! kill -0 "$driver" 2>/dev/null; then
    driver_done=$tick
  fi
  if [ -n "$driver_done" ] && [ "$alive" = 0 ] && [ "$n" = 0 ]; then
    break
  fi
  sleep 0.5
done

wait "$driver" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
grep -q '^PASS autoscale driver' "$log-driver.log" || { echo "FAIL: the driver"; cat "$log-driver.log"; exit 1; }
codes=0
for w in "${workers[@]}"; do wait "$w" || codes=$((codes + 1)); done
retried=$(value dista_tasks_retried_total)
failed=$(value 'dista_tasks_finished_total{outcome="error"}')
left=$(value dista_workers)
echo "autoscale: started $started workers (at most $most at once, $step in one step), drained $drained; $retried retried, $failed failed, $left left, $codes exited non-zero"
if [ "$most" != 6 ] || [ "$step" -gt 4 ] || [ "$step" -lt 1 ] || [ "${left:-1}" != 0 ] \
  || [ "${retried:-1}" != 0 ] || [ "${failed:-1}" != 0 ] || [ "$codes" != 0 ] || [ "$drained" != "$started" ]; then
  echo "FAIL: autoscaling"; tail -20 "$log-head.log"; exit 1
fi
echo "PASS autoscale: grew from 0 to 6 workers and back to 0, every result exact, nothing retried or lost"
