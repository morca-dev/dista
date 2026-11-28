#!/bin/sh
# The runs of PR 3 (bench/embed/README.md, "Results so far"), one after
# another, each through run.py, which appends a row to RESULTS/runs.tsv.
#
#   sh bench/embed/runs.sh sai RESULTS [part...]   # Linux, 16 threads
#   sh bench/embed/runs.sh mac RESULTS [part...]   # the Mac
#
# Parts (all of them by default): ref, speed, busy, encode, recovery, stub,
# check (the full check of the reference, last since it is slow). The
# reference is the first dista run at the largest size, kept in
# build/embed/runs/ref; every other model run is compared with it by id,
# and `check` checks it against one process running encoder.mojo.
# EMBED_RAY_TMP is Ray's temp directory (short on macOS: socket paths).
set -u
machine=$1
results=$2
shift 2
parts=${*:-ref speed busy encode recovery stub check}
ref=build/embed/runs/ref
tmp=${EMBED_RAY_TMP:-$PWD/rt}
run() { pixi run -e ray python bench/embed/run.py --results="$results" --ray-tmp="$tmp" "$@"; }

if [ "$machine" = sai ]; then
    big=400000
    # E=2: one CPU set of 4 physical cores per process (0-3 and 4-7, one
    # CCX each). E=4: 2 physical cores and their siblings each.
    configs="2:0-3/4-7 4:0,1,8,9/2,3,10,11/4,5,12,13/6,7,14,15"
    sizes="100000 100000 100000 200000 400000"
else
    big=100000
    configs="1: 2: 4:"
    sizes="20000 20000 20000 100000"
fi
ck="--check=ref:$ref"

for part in $parts; do
    case $part in
    ref)
        e=${configs%% *}; e=${e%%:*}
        run --system=dista --texts=$big --embed-workers=$e --check=none \
            $( [ $machine = sai ] && echo --pin=0-3/4-7 ) --keep=$ref --tag=reference ;;
    speed)
        for c in $configs; do
            e=${c%%:*}; pin=${c#*:}
            for n in $sizes; do
                [ $machine = sai ] && [ $n = $big ] && [ $e = 2 ] && continue  # the ref run
                [ $machine = mac ] && [ $n = $big ] && [ $e = 1 ] && continue
                run --system=dista --texts=$n --embed-workers=$e ${pin:+--pin=$pin} --threads=4 $ck
                run --system=ray --texts=$n --embed-workers=$e ${pin:+--pin=$pin} --threads=4 $ck
            done
        done ;;
    busy)
        for c in $configs; do
            e=${c%%:*}; pin=${c#*:}
            for n in $sizes; do
                [ $n = 100000 ] || [ $machine = mac ] || continue
                run --system=dista --texts=$n --embed-workers=$e ${pin:+--pin=$pin} --threads=4 --busy-wait-0 $ck
            done
        done ;;
    encode)
        if [ $machine = sai ]; then
            for pin in 0-3 0,1,8,9; do
                run --system=dista --mode=encode --texts=20000 --pin=$pin --threads=4
                run --system=dista --mode=encode --texts=20000 --pin=$pin --threads=4 --busy-wait-0
                run --system=ray --mode=encode --texts=20000 --pin=$pin --threads=4
            done
        else
            run --system=dista --mode=encode --texts=20000
            run --system=dista --mode=encode --texts=20000 --busy-wait-0
            run --system=ray --mode=encode --texts=20000 --threads=4
        fi ;;
    recovery)
        if [ $machine = sai ]; then n=40000 k=60 pin=--pin=0-3/4-7; else n=20000 k=10 pin=; fi
        for s in dista ray; do
            run --system=$s --texts=$n --embed-workers=2 --threads=4 $pin $ck --tag=no-kill
            run --system=$s --texts=$n --embed-workers=2 --threads=4 $pin $ck --kill-after=$k --tag=kill
        done ;;
    stub)
        [ $machine = sai ] || continue
        for n in 1000000 4000000 10000000; do
            run --system=dista --stub --texts=$n --embed-workers=4 --threads=4 \
                --pin=0,1,8,9/2,3,10,11/4,5,12,13/6,7,14,15 --check=full
            run --system=ray --stub --texts=$n --embed-workers=4 --threads=4 \
                --pin=0,1,8,9/2,3,10,11/4,5,12,13/6,7,14,15 --check=full
        done
        rm -rf build/embed/runs/out-dista build/embed/runs/out-ray build/embed/runs/corpus-stub-* ;;
    check)
        pixi run -e embed ./build/bench-embed check --out=$ref \
            --corpus=build/embed/runs/corpus-model-$big-512 \
            > "$results/reference-check.log" 2>&1
        echo "exit $?" >> "$results/reference-check.log" ;;
    esac
done
