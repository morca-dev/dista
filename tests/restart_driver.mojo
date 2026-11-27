"""The driver tests/head_restart.sh runs while it kills and restarts the head.

Large results that stay on workers, a reduce over them after the driver has
released them, small results through the head, and an actor checkpointed
every 50 calls. The actor's count must come out exact: the new head
restores it from its newest checkpoint, and the driver resends the calls no
checkpoint includes.

The driver sends everything, then waits until the file named by
`RESTART_GATE` exists, which the script creates once it has killed and
restarted the head. So the restart always lands while the driver still has
every answer to collect, however fast or slow the machine is.

A stream starts first, and the driver reads two of its items before it sends
the rest. Its window then holds it, unfinished, through the restart: after
it, `next` returns the items that had come and then fails with "stream
interrupted: the head restarted", since the driver doesn't send a stream
again. The items it returned must be right. Without `RESTART_GATE` (in
tests/cli.sh) the driver cancels the stream instead.

A task and an actor of group gpu run beside them, on the two workers the
script starts with `DISTA_GROUP=gpu`: both raise if they run anywhere else.
After the restart the gpu workers rejoin in their group, and the driver
sends its groups frame before it resends anything, so the gpu tasks and
the gpu actor's calls, restored from its checkpoint, stay on them.
"""

from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import Actor, Emit, Method, ObjectRef, Runtime, Seq, Task
from dista import TaskStream


def blob(n: Int) -> String:
    sleep(0.01)
    return String("x") * n


def total_length(xs: Seq[String]) -> Int:
    var t = 0
    for i in range(len(xs)):
        t += xs[i].byte_length()
    return t


def square(x: Int) -> Int:
    sleep(0.01)
    return x * x


@fieldwise_init
struct Counter(Movable):
    var total: Int


def new_counter(start: Int) -> Counter:
    return Counter(start)


def slow_add(mut c: Counter, n: Int) -> Int:
    sleep(0.01)
    c.total += n
    return c.total


def _on_gpu() raises:
    if getenv("DISTA_GROUP") != "gpu":
        raise Error("ran outside group gpu")


def gpu_square(x: Int) raises -> Int:
    _on_gpu()
    sleep(0.01)
    return x * x


def new_gpu_counter(start: Int) raises -> Counter:
    _on_gpu()
    return Counter(start)


def gpu_add(mut c: Counter, n: Int) raises -> Int:
    _on_gpu()
    sleep(0.01)
    c.total += n
    return c.total


def count_slowly(n: Int, mut out: Emit[Int]) raises:
    for k in range(n):
        sleep(0.001)
        out.put(k)


comptime Blob = Task[Int, String, blob]
comptime TotalLength = Task[Seq[String], Int, total_length]
comptime Square = Task[Int, Int, square]
comptime CounterActor = Actor[Counter, Int, new_counter]
comptime SlowAdd = Method[CounterActor, Int, Int, slow_add]
comptime CountSlowly = TaskStream[Int, Int, count_slowly]
comptime GpuSquare = Task[Int, Int, gpu_square, group="gpu"]
comptime GpuCounter = Actor[Counter, Int, new_gpu_counter, group="gpu"]
comptime GpuAdd = Method[GpuCounter, Int, Int, gpu_add]

comptime CALLS = 300
comptime GPU_CALLS = 200


def wait_for_gate() raises:
    """Waits, up to a minute, for the script's sign that the head restarted."""
    var gate = getenv("RESTART_GATE")
    if not gate:
        return
    for _ in range(600):
        if exists(gate):
            return
        sleep(0.1)
    raise Error("the head was never restarted: no ", gate)


def main() raises:
    var rt = Runtime[
        Blob,
        TotalLength,
        Square,
        CounterActor,
        SlowAdd,
        CountSlowly,
        GpuSquare,
        GpuCounter,
        GpuAdd,
    ](workers=4)
    # Far more items than its window: it can't end before the restart.
    var s = rt.stream[CountSlowly](1_000_000, window=4)
    var items = List[Int]()
    for _ in range(2):
        items.append(rt.take(rt.next(s).value()))
    var counter = rt.create[CounterActor](0, checkpoint_every=50)
    var adds = List[ObjectRef[Int]](capacity=CALLS)
    for _ in range(CALLS):
        adds.append(rt.call[SlowAdd](counter, 1))
    var gpu_counter = rt.create[GpuCounter](0, checkpoint_every=50)
    var gpu_adds = List[ObjectRef[Int]](capacity=GPU_CALLS)
    for _ in range(GPU_CALLS):
        gpu_adds.append(rt.call[GpuAdd](gpu_counter, 1))
    var gpu_squares = List[ObjectRef[Int]]()
    for i in range(100):
        gpu_squares.append(rt.remote[GpuSquare](i))
    var maps = List[ObjectRef[String]]()
    for i in range(400):
        maps.append(rt.remote[Blob](100_000 + i))
    var reduced = rt.remote[TotalLength](maps)
    rt.release(maps)
    var squares = List[ObjectRef[Int]]()
    for i in range(400):
        squares.append(rt.remote[Square](i))
    wait_for_gate()
    if not getenv("RESTART_GATE"):
        # No restart is coming (tests/cli.sh): the stream would run for ever.
        rt.cancel(s)
    var why = String("it ended")
    try:
        while True:
            var r = rt.next(s)
            if not r:
                break
            items.append(rt.take(r.value()))
    except e:
        why = String(e)
    for k in range(len(items)):
        if items[k] != k:
            raise Error("stream item ", k, " was ", items[k])
    print("stream:", len(items), "items, then", why)
    var sum = 0
    for r in squares:
        sum += rt.get(r)
    for i in range(CALLS):
        var got = rt.get(adds[i])
        if got != i + 1:
            raise Error("call ", i, " answered ", got, ", expected ", i + 1)
    var count = rt.get(rt.call[SlowAdd](counter, 0))
    rt.kill(counter)
    var gpu_sum = 0
    for r in gpu_squares:
        gpu_sum += rt.get(r)
    for i in range(GPU_CALLS):
        var got = rt.get(gpu_adds[i])
        if got != i + 1:
            raise Error("gpu call ", i, " answered ", got, ", expected ", i + 1)
    var gpu_count = rt.get(rt.call[GpuAdd](gpu_counter, 0))
    rt.kill(gpu_counter)
    print(
        "lengths",
        rt.get(reduced),
        "squares",
        sum,
        "counter",
        count,
        "gpu squares",
        gpu_sum,
        "gpu counter",
        gpu_count,
    )
