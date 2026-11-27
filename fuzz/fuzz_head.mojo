"""Fuzz: the head's protocol, driven by a fake driver and a fake worker.

Each input is a fresh head with one driver and one worker connected over
socket pairs, both past their hello. The rest of the input is a script of
frames, each sent by one of the two, or a disconnect. When the first byte
has bit 2 set, the worker is in group `g` and the driver's groups frame puts
tasks 1 and 2 in it, and task 3 in `h`, which no worker is in; the script's
random kinds include groups frames too. A task type is an actor's too, so
actors of each group are created, and each must be placed only on a worker
of its own group.
The frames are random kinds, object
IDs from a small range (so they collide with submitted tasks), random
payloads. A frame the head rejects drops that peer, as the real loop does.

After every step the head's bookkeeping must hold together: counts positive,
every held object counted, every location naming a worker connection,
every actor's call log holding just the calls its checkpoint doesn't cover,
every running stream naming a worker connection and a stream not ended, and
nothing pending for an object that no longer exists.

    pixi run -e fuzz fuzz-head
"""

from std.os import getenv

from mozz import FuzzConfig, fuzz

from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_GROUPS,
    KIND_HELLO,
    ROLE_DRIVER,
    Frame,
    encode_groups,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, stream_pair
from dista.worker import worker_hello

comptime HASH: UInt64 = 0x5EED
comptime DRIVER = 0
comptime WORKER = 1


def _check(head: Head) raises:
    for entry in head._refs.items():
        if entry.value <= 0:
            raise Error("assertion failed: count ", entry.value)
    for entry in head._held.items():
        if entry.key not in head._refs:
            raise Error("assertion failed: held but uncounted")
    for entry in head._location.items():
        if entry.value < 0 or entry.value >= len(head._conns):
            raise Error("assertion failed: location names no connection")
        if entry.key not in head._where:
            raise Error("assertion failed: located but no address")
    for entry in head._actors.items():
        var w = entry.value.worker
        if w >= 0 and head._hosting[w] != entry.key and not entry.value.failed:
            raise Error("assertion failed: placed actor not hosted")
        ref act = entry.value
        if len(act.log) != act.calls - max(act.saved, 0):
            raise Error("assertion failed: the call log lost or kept a call")
        if act.saved > act.calls:
            raise Error("assertion failed: a checkpoint covers unmade calls")
        if w >= 0 and not act.failed and head._group[w] != act.group:
            raise Error("assertion failed: an actor placed outside its group")
    for i in range(len(head._hosting)):
        if head._hosting[i] != 0 and head._hosting[i] not in head._actors:
            raise Error("assertion failed: worker hosts a gone actor")
    for entry in head._stream_at.items():
        if entry.value < 0 or entry.value >= len(head._conns):
            raise Error("assertion failed: a stream runs on no connection")
        if entry.key not in head._streams:
            raise Error("assertion failed: a stream runs, ended or unknown")
    for entry in head._deps_of.items():
        if entry.key not in head._refs and entry.key not in head._dead:
            raise Error("assertion failed: inputs held for a gone object")


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def target(data: List[UInt8]) raises:
    with open("build/fuzz-head-last.bin", "w") as f:
        f.write_bytes(Span(data))
    var head = Head(Listener(0), HASH, 2, False)
    var peers = List[Stream]()
    var conn: List[Int] = [_connect(head, peers), _connect(head, peers)]
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(conn[DRIVER], Frame(KIND_HELLO, 0, 0, hello^))
    var group = String()
    if len(data) > 0 and (data[0] & 4) != 0:
        group = "g"
        var table = encode_groups(
            [UInt64(1), UInt64(2), UInt64(3)], ["g", "g", "h"]
        )
        head._handle(conn[DRIVER], Frame(KIND_GROUPS, 0, 0, table^))
    hello = worker_hello(HASH, [127, 0, 0, 1], 7000, 1, group)
    head._handle(conn[WORKER], Frame(KIND_HELLO, 0, 0, hello^))
    # A worker's hello with the input's first bytes after its address, where
    # its slots and group go: it parses or raises, and the worker then goes.
    var extra = _connect(head, peers)
    var long = worker_hello(HASH, [127, 0, 0, 1], 7001, 1)
    long.extend(Span(data)[: min(len(data), 48)])
    try:
        head._handle(extra, Frame(KIND_HELLO, 0, 0, long^))
    except:
        pass
    head._drop(extra)
    var alive: List[Bool] = [True, True]

    # Each step: who (bit 0) and whether it hangs up (bit 1), kind, object,
    # task (an object with its top bit set), payload length, payload.
    var pos = 0
    while pos + 5 <= len(data):
        var who = Int(data[pos] & 1)
        var hang_up = (data[pos] & 2) != 0
        var kind = data[pos + 1] % (KIND_GROUPS + 1)
        var obj = (UInt64(1) << DRIVER_SHIFT) | UInt64(data[pos + 2] % 16)
        # A task type, or with the top bit set an object, as a stream's
        # item frames carry their stream's ID.
        var task = UInt64(data[pos + 3] % 4)
        if data[pos + 3] >= 128:
            task = (UInt64(1) << DRIVER_SHIFT) | UInt64(data[pos + 3] % 16)
        var n = min(Int(data[pos + 4]), len(data) - pos - 5)
        var payload = List[UInt8](Span(data)[pos + 5 : pos + 5 + n])
        pos += 5 + n
        if not alive[who]:
            continue
        var i = conn[who]
        if hang_up:
            head._drop(i)
            alive[who] = False
        else:
            try:
                head._handle(i, Frame(kind, obj, task, payload^))
            except:
                head._drop(i)  # what the loop does with a peer that misbehaves
                alive[who] = False
        head._release_waiting()
        head._dispatch()
        _check(head)
    # Mojo destroys a value at its last use: keep the peers' ends open until
    # here, or the head's replies hit closed sockets.
    _ = len(peers)


def main() raises:
    var replay = getenv("FUZZ_REPLAY")
    if replay != "":
        # Run one saved input, e.g. build/fuzz-<name>-last.bin after an abort.
        var data: List[UInt8]
        with open(replay, "r") as f:
            data = f.read_bytes()
        target(data)
        print("replayed", replay, "without a crash")
        return
    var seeds = List[List[UInt8]]()
    # The driver submits a task (kind 1) on object 1, the worker answers it
    # (kind 2), the driver releases it (kind 11) and asks for stats (13).
    var first: List[UInt8] = [
        0,
        1,
        1,
        0,
        1,
        7,
        1,
        2,
        1,
        0,
        1,
        9,
        0,
        11,
        1,
        0,
        0,
        0,
        13,
        0,
        0,
        0,
    ]
    seeds.append(first^)
    # A task-after (6) on object 1, then the worker stores 1 (7), errors 2.
    var second: List[UInt8] = [
        0,
        1,
        1,
        0,
        0,
        0,
        6,
        2,
        0,
        17,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        7,
        1,
        0,
        0,
        1,
        3,
        2,
        0,
        1,
        33,
    ]
    seeds.append(second^)
    # A task-stream (36) on object 1 with a window of 4, the worker answers
    # its item 2 (task 129 names object 1), the driver credits it (37) and
    # cancels it (32), and the worker answers its end.
    var third: List[UInt8] = [0, 36, 1, 0, 8, 4, 0, 0, 0, 0, 0, 0, 0]
    third.extend([UInt8(1), 2, 2, 129, 1, 9])
    third.extend([UInt8(0), 37, 1, 0, 8, 1, 0, 0, 0, 0, 0, 0, 0])
    third.extend([UInt8(0), 32, 1, 0, 0])
    third.extend([UInt8(1), 2, 1, 0, 1, 1])
    seeds.append(third^)
    # Groups on (bit 2): actors (19) of type 1, in g, of type 0, in none,
    # and of type 3, in h; then a call (20) to the first.
    var fourth: List[UInt8] = [4, 19, 1, 1, 8, 0, 0, 0, 0, 0, 0, 0, 0]
    fourth.extend([UInt8(0), 19, 3, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0])
    fourth.extend([UInt8(0), 19, 4, 3, 8, 0, 0, 0, 0, 0, 0, 0, 0])
    fourth.extend([UInt8(0), 20, 2, 1, 8, 1, 0, 0, 0, 0, 1, 0, 0])
    seeds.append(fourth^)
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "20000")),
            seed=0,
            crash_dir="build/fuzz-crashes/head",
            max_input_len=2048,
        ),
        seeds,
    )
