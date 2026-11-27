"""The driver tests/stream_loss.sh runs: a stream whose worker is killed.

`gated` writes its worker's pid to `STREAM_LOSS_DIR/<mode>.pid` and waits
for `<mode>.go`, which the script makes once it has killed that worker.

- Mode 1 waits before its first item. Killed then, it runs again on another
  worker, and the driver reads all `N` items, exact.
- Mode 2 puts three items first. Once the driver has read them it says
  TAKEN, the script kills the worker, and the stream's end must fail with
  the worker-lost rule a `TaskMany` has.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import Emit, Runtime, TaskStream

comptime N = 100


def _wait_for(path: String) raises:
    for _ in range(600):
        if exists(path):
            return
        sleep(0.1)
    raise Error("nobody made ", path)


def gated(mode: Int, mut out: Emit[Int]) raises:
    var dir = getenv("STREAM_LOSS_DIR")
    if mode == 2:
        for k in range(3):
            out.put(k)
    var pid = external_call["getpid", c_int]()
    with open(String(dir, "/", mode, ".pid"), "w") as f:
        f.write(String(pid))
    _wait_for(String(dir, "/", mode, ".go"))
    for k in range(N):
        out.put(k)


comptime Gated = TaskStream[Int, Int, gated]


def main() raises:
    var rt = Runtime[Gated](workers=3)
    var s = rt.stream[Gated](1)
    var k = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        var got = rt.take(r.value())
        if got != k:
            raise Error("item ", k, " of the retried stream was ", got)
        k += 1
    if k != N:
        raise Error("the retried stream had ", k, " items, not ", N)
    print("PASS before: the stream ran again elsewhere,", k, "items exact")

    var t = rt.stream[Gated](2)
    for want in range(3):
        var r = rt.next(t)
        if not r:
            raise Error("the stream ended before item ", want)
        var got = rt.take(r.value())
        if got != want:
            raise Error("item ", want, " was ", got)
    print("TAKEN 3", flush=True)
    var msg = String("no error")
    try:
        if rt.next(t):
            msg = String("an item after the worker was killed")
    except e:
        msg = String(e)
    var want = String(
        "worker lost while running this stream, after some of its items were"
        " answered"
    )
    if msg != want:
        raise Error("the end failed with '", msg, "', not '", want, "'")
    print("PASS after: three items, then the end failed:", msg)
