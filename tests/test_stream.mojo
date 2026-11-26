"""`TaskStream`: a task that hands over each item as it makes it, read with
`rt.next`, on a worker with one slot and on one with several; its window,
and cancelling it.

Slots are set with `DISTA_WORKER_SLOTS` before a `Runtime` forks its
workers, which inherit it, and unset once they're forked. The test of a
driver that leaves drives a `Head` directly, over socket pairs, as
tests/test_cancel.mojo does."""

from std.os import getenv, setenv, unsetenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_false, assert_true
from std.time import perf_counter_ns, sleep

from dista import Emit, ObjectRef, Runtime, StreamRef, Task, TaskStream
from dista.codec import encode, put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_ERROR,
    KIND_FREE,
    KIND_HELLO,
    KIND_RELEASE,
    KIND_RESULT,
    KIND_STREAM_STOP,
    KIND_TASK,
    KIND_TASK_STREAM,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
    read_frame,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, poll_readable, stream_pair


def count_up(n: Int, mut out: Emit[Int]) raises:
    for k in range(n):
        out.put(k)


def _big(k: Int) -> String:
    """Item `k` of `big_items`: more than 64 KiB, so it's stored on its
    worker, and different for every `k`."""
    return String(k, ":") + String("x") * (70_000 + k)


def big_items(n: Int, mut out: Emit[String]) raises:
    for k in range(n):
        out.put(_big(k))


def slow_two(ms: Int, mut out: Emit[Int]) raises:
    """Puts 1, waits `ms`, then puts 2."""
    out.put(1)
    sleep(Float64(ms) / 1000.0)
    out.put(2)


def five_then_fail(n: Int, mut out: Emit[Int]) raises:
    for k in range(5):
        out.put(k * k)
    raise Error("broke after 5")


def late(ms: Int, mut out: Emit[Int]) raises:
    """Waits `ms`, then puts it."""
    sleep(Float64(ms) / 1000.0)
    out.put(ms)


def megabytes(n: Int, mut out: Emit[String]) raises:
    """Puts `n` items of 1 MiB each."""
    for k in range(n):
        out.put(String(k, ":") + String("m") * (1 << 20))


def put_until_stopped(path: String, mut out: Emit[Int]) raises:
    """Puts up to 1,000 items; when `put` raises, writes why to `path`, then
    raises it."""
    try:
        for k in range(1000):
            out.put(k)
    except e:
        with open(path, "w") as f:
            f.write(String(e))
        raise e^


def double(x: Int) -> Int:
    return 2 * x


def length(s: String) -> Int:
    return s.byte_length()


def big(k: Int) -> String:
    return _big(k)


def lengths_of(s: String, mut out: Emit[Int]) raises:
    """Puts its argument's length, then its first byte."""
    out.put(s.byte_length())
    out.put(Int(s.as_bytes()[0]))


comptime CountUp = TaskStream[Int, Int, count_up]
comptime BigItems = TaskStream[Int, String, big_items]
comptime SlowTwo = TaskStream[Int, Int, slow_two]
comptime FiveThenFail = TaskStream[Int, Int, five_then_fail]
comptime Late = TaskStream[Int, Int, late]
comptime Megabytes = TaskStream[Int, String, megabytes]
comptime PutUntilStopped = TaskStream[String, Int, put_until_stopped]
comptime Double = Task[Int, Int, double]
comptime Length = Task[String, Int, length]
comptime Big = Task[Int, String, big]
comptime LengthsOf = TaskStream[String, Int, lengths_of]


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def _slots(n: Int):
    if n == 1:
        _ = unsetenv("DISTA_WORKER_SLOTS")
    else:
        _ = setenv("DISTA_WORKER_SLOTS", String(n))


def _in_order(slots: Int) raises:
    """1,000 small items and 20 stored ones, each read in order and exact."""
    _slots(slots)
    var rt = Runtime[CountUp, BigItems](workers=1)
    _slots(1)
    var small = rt.stream[CountUp](1000)
    var big = rt.stream[BigItems](20, window=0)
    var k = 0
    while True:
        var r = rt.next(small)
        if not r:
            break
        assert_equal(r.value().id, small.id + 1 + UInt64(k))
        assert_equal(rt.take(r.value()), k)
        k += 1
    assert_equal(k, 1000)
    k = 0
    while True:
        var r = rt.next(big)
        if not r:
            break
        # Stored on its worker, not sent through the head.
        assert_true(rt.location(r.value()) != "")
        var s = rt.take(r.value())
        assert_equal(s.byte_length(), _big(k).byte_length())
        assert_true(s == _big(k), String("item ", k, " differs"))
        k += 1
    assert_equal(k, 20)
    # Read to the end, it stays ended.
    assert_false(rt.next(big))


def test_items_arrive_in_order_on_one_slot() raises:
    _in_order(1)


def test_items_arrive_in_order_on_four_slots() raises:
    _in_order(4)


def _first_while_running(slots: Int) raises:
    _slots(slots)
    var rt = Runtime[SlowTwo](workers=1)
    _slots(1)
    var t0 = perf_counter_ns()
    var s = rt.stream[SlowTwo](1500)
    var first = rt.next(s)
    var took = _seconds_since(t0)
    assert_true(Bool(first))
    assert_true(took < 1.2, String("the first item took ", took, " s"))
    # The task is still sleeping: its end hasn't come.
    assert_false(rt.ready(ObjectRef[Int](s.id)))
    assert_equal(rt.take(first.value()), 1)
    var second = rt.next(s)
    assert_true(_seconds_since(t0) >= 1.4)
    assert_equal(rt.take(second.value()), 2)
    assert_false(rt.next(s))


def test_the_first_item_is_read_while_the_task_runs() raises:
    _first_while_running(1)
    _first_while_running(4)


def test_an_error_comes_after_the_items_before_it() raises:
    var rt = Runtime[FiveThenFail](workers=1)
    var s = rt.stream[FiveThenFail](0)
    var got = List[Int]()
    var msg = String()
    try:
        while True:
            var r = rt.next(s)
            if not r:
                break
            got.append(rt.take(r.value()))
    except e:
        msg = String(e)
    assert_equal(len(got), 5)
    for k in range(5):
        assert_equal(got[k], k * k)
    assert_equal(msg, "broke after 5")
    assert_false(rt.next(s))


def test_an_empty_stream_ends_at_once() raises:
    var rt = Runtime[CountUp](workers=1)
    var t0 = perf_counter_ns()
    var s = rt.stream[CountUp](0)
    assert_false(rt.next(s, timeout=5.0))
    assert_true(_seconds_since(t0) < 1.0)


def test_items_feed_other_tasks() raises:
    var rt = Runtime[CountUp, BigItems, Double, Length](workers=2)
    var s = rt.stream[CountUp](50)
    var doubled = List[ObjectRef[Int]]()
    while True:
        var r = rt.next(s)
        if not r:
            break
        doubled.append(rt.remote[Double](r.value()))
        rt.release(r.value())  # the task given it still gets it
    assert_equal(len(doubled), 50)
    for k in range(50):
        assert_equal(rt.take(doubled[k]), 2 * k)
    # A stored item, fetched or mapped by the task that takes it.
    var b = rt.stream[BigItems](3)
    var lengths = List[ObjectRef[Int]]()
    while True:
        var r = rt.next(b)
        if not r:
            break
        lengths.append(rt.remote[Length](r.value()))
        rt.release(r.value())
    for k in range(3):
        assert_equal(rt.take(lengths[k]), _big(k).byte_length())


def _after(slots: Int) raises:
    """A stream on another task's result: a task-after with the `STREAM`
    flag, run as a stream once its input exists, which is small and goes
    through the head, or stored and fetched (task-inputs)."""
    _slots(slots)
    var rt = Runtime[CountUp, LengthsOf, Double, Big](workers=2)
    _slots(1)
    var s = rt.stream[CountUp](rt.remote[Double](5), window=4)
    var k = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        assert_equal(r.value().id, s.id + 1 + UInt64(k))
        assert_equal(rt.take(r.value()), k)
        k += 1
    assert_equal(k, 10)
    var input = rt.remote[Big](3)
    assert_true(rt.location(input) != "")  # stored on its worker
    var b = rt.stream[LengthsOf](input)
    rt.release(input)  # the stream given it still gets it
    var got = List[Int]()
    while True:
        var r = rt.next(b)
        if not r:
            break
        got.append(rt.take(r.value()))
    assert_equal(len(got), 2)
    assert_equal(got[0], _big(3).byte_length())
    assert_equal(got[1], Int(_big(3).as_bytes()[0]))


def test_a_stream_runs_on_another_tasks_result() raises:
    _after(1)
    _after(4)


def test_wait_picks_the_stream_with_an_item() raises:
    var rt = Runtime[Late](workers=3, pipeline=1)
    var slow1 = rt.stream[Late](2000)
    var fast = rt.stream[Late](10)
    var slow2 = rt.stream[Late](2000)
    var ids: List[UInt64] = [slow1.next_id(), fast.next_id(), slow2.next_id()]
    var t0 = perf_counter_ns()
    var done = rt.wait(ids)
    assert_true(_seconds_since(t0) < 1.5)
    assert_equal(len(done), 1)
    assert_equal(done[0], 1)
    assert_equal(rt.take(rt.next(fast).value()), 10)
    # Its end comes next: waiting on its id sees it.
    var ends: List[UInt64] = [slow1.id, fast.id, slow2.id]
    var ended = rt.wait(ends, count=1, timeout=5.0)
    assert_equal(len(ended), 1)
    assert_equal(ended[0], 1)
    assert_false(rt.next(fast))
    assert_equal(rt.take(rt.next(slow1).value()), 2000)
    assert_equal(rt.take(rt.next(slow2).value()), 2000)
    assert_false(rt.next(slow1))
    assert_false(rt.next(slow2))


# ── the window ──────────────────────────────────────────────────────────


def _one(id: UInt64) -> List[UInt64]:
    var ids: List[UInt64] = [id]
    return ids^


def _item[X: AnyType](s: StreamRef[X], k: Int) -> ObjectRef[X]:
    return ObjectRef[X](s.id + 1 + UInt64(k))


def _stops_at_its_window(slots: Int) raises:
    _slots(slots)
    var rt = Runtime[CountUp](workers=1)
    _slots(1)
    var s = rt.stream[CountUp](1000, window=4)
    assert_equal(len(rt.wait(_one(_item(s, 3).id), timeout=10.0)), 1)
    sleep(0.3)
    # The stream and its 4 items, held for this driver: no fifth was put.
    assert_equal(rt.stats().objects, 5)
    assert_false(rt.ready(_item(s, 4)), "a fifth item was put")
    # Reading one lets exactly one more through.
    assert_equal(rt.take(rt.next(s).value()), 0)
    assert_equal(len(rt.wait(_one(_item(s, 4).id), timeout=10.0)), 1)
    sleep(0.3)
    assert_equal(rt.stats().objects, 5)  # item 0 taken, item 4 put
    assert_false(rt.ready(_item(s, 5)), "a sixth item was put")
    rt.cancel(s)


def test_an_unread_stream_stops_at_its_window_on_one_slot() raises:
    _stops_at_its_window(1)


def test_an_unread_stream_stops_at_its_window_on_four_slots() raises:
    _stops_at_its_window(4)


def _bounded(slots: Int) raises:
    """60 items of 1 MiB read slowly: the head never holds more than the
    window of them, unread, besides the stream itself."""
    comptime N = 60
    comptime WINDOW = 4
    _slots(slots)
    var rt = Runtime[Megabytes](workers=1)
    _slots(1)
    var s = rt.stream[Megabytes](N, window=WINDOW)
    var most = 0
    var k = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        var x = rt.take(r.value())
        assert_true(x.startswith(String(k, ":")))
        assert_equal(x.byte_length(), String(k, ":").byte_length() + (1 << 20))
        k += 1
        sleep(0.02)
        var st = rt.stats()
        most = max(most, st.objects)
        # The stream, and at most the window of unread items: the reader
        # holds none, as `take` releases.
        assert_true(
            st.objects <= WINDOW + 1,
            String(st.objects, " objects held after item ", k),
        )
        assert_true(st.located <= WINDOW)
    assert_equal(k, N)
    print("  most objects held:", most)


def test_memory_stays_bounded_on_one_slot() raises:
    _bounded(1)


def test_memory_stays_bounded_on_four_slots() raises:
    _bounded(4)


def test_a_one_slot_worker_serves_a_fetch_while_its_stream_waits() raises:
    # Results in the worker's memory, so the driver fetches them over its
    # socket rather than mapping them.
    _ = setenv("DISTA_SHM", "0")
    var rt = Runtime[BigItems](workers=1)
    _ = unsetenv("DISTA_SHM")
    var s = rt.stream[BigItems](3, window=1)
    assert_equal(len(rt.wait(_one(_item(s, 0).id), timeout=10.0)), 1)
    sleep(0.2)
    # Item 0 is unread, so the task waits in its second `put`.
    assert_false(rt.ready(_item(s, 1)))
    # Read without `next`, so no credit: the worker serves it while it
    # waits.
    var got = rt.get(_item(s, 0), timeout=10.0)
    assert_true(got == _big(0))
    assert_false(rt.ready(_item(s, 1)), "the fetch let the stream go on")
    var k = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        assert_true(rt.take(r.value()) == _big(k))
        k += 1
    assert_equal(k, 3)


def test_a_zero_window_never_blocks() raises:
    var rt = Runtime[CountUp](workers=1)
    var s = rt.stream[CountUp](1000, window=0)
    # Nothing read, and the task still puts all its items and ends.
    assert_equal(len(rt.wait(_one(ObjectRef[Int](s.id).id), timeout=10.0)), 1)
    assert_equal(rt.stats().objects, 1001)
    var k = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        assert_equal(rt.take(r.value()), k)
        k += 1
    assert_equal(k, 1000)


# ── cancelling ──────────────────────────────────────────────────────────


def _marker(name: String) -> String:
    return String(
        getenv("TMPDIR", "/tmp"),
        "/dista-test-stream-",
        name,
        "-",
        perf_counter_ns(),
    )


def _cancel_blocked(slots: Int) raises:
    var path = _marker(String(slots))
    _slots(slots)
    var rt = Runtime[PutUntilStopped, Double](workers=1)
    _slots(1)
    var s = rt.stream[PutUntilStopped](path, window=2)
    assert_equal(len(rt.wait(_one(_item(s, 1).id), timeout=10.0)), 1)
    sleep(0.2)
    assert_false(rt.ready(_item(s, 2)))
    var t0 = perf_counter_ns()
    rt.cancel(s)
    # The task's waiting `put` raises "cancelled", and the task ends.
    while not exists(path):
        assert_true(_seconds_since(t0) < 1.0, "the task didn't end in 1 s")
        sleep(0.005)
    sleep(0.05)
    with open(path, "r") as f:
        assert_equal(f.read(), "cancelled")
    print("  ended", _seconds_since(t0), "s after the cancel")
    # The items answered before come first, then the error.
    var got = List[Int]()
    var msg = String()
    try:
        while True:
            var r = rt.next(s)
            if not r:
                break
            got.append(rt.take(r.value()))
    except e:
        msg = String(e)
    assert_equal(msg, "cancelled")
    assert_equal(len(got), 2)
    assert_equal(got[0], 0)
    assert_equal(got[1], 1)
    # The worker runs other tasks.
    assert_equal(rt.take(rt.remote[Double](21)), 42)
    var st = rt.stats()
    assert_equal(st.objects, 0)
    assert_equal(st.located + st.results + st.errors, 0)


def test_cancel_ends_a_blocked_stream_on_one_slot() raises:
    _cancel_blocked(1)


def test_cancel_ends_a_blocked_stream_on_four_slots() raises:
    _cancel_blocked(4)


def test_release_before_the_end_stops_a_stream() raises:
    var path = _marker("release")
    var rt = Runtime[PutUntilStopped, Double](workers=1)
    var s = rt.stream[PutUntilStopped](path, window=2)
    assert_equal(len(rt.wait(_one(_item(s, 1).id), timeout=10.0)), 1)
    rt.release(_item(s, 0))
    rt.release(_item(s, 1))
    rt.release(s)
    var t0 = perf_counter_ns()
    while not exists(path):
        assert_true(_seconds_since(t0) < 1.0, "the task didn't end in 1 s")
        sleep(0.005)
    assert_equal(rt.take(rt.remote[Double](1)), 2)
    assert_equal(rt.stats().objects, 0)


comptime HASH: UInt64 = 0x5EED


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _to_worker(mut head: Head, w: Int, s: Stream) raises -> Frame:
    """The next frame the head sent worker `w`, skipping its capacity
    announcement (a free of object 0)."""
    head._conns[w].flush()
    while True:
        if not poll_readable([s.fd], 2000)[0]:
            raise Error("the head sent the worker nothing")
        var f = read_frame(s)
        if not f:
            raise Error("the head hung up")
        var frame = f.take()
        if frame.kind == KIND_FREE and frame.object_id == 0:
            continue
        return frame^


def _task_stream(window: Int, arg: Int) -> List[UInt8]:
    var body = List[UInt8]()
    put_scalar[DType.uint64](body, UInt64(window))
    encode(arg, body)
    return body^


def test_a_driver_leaving_or_releasing_stops_its_streams() raises:
    var head = Head(Listener(0), HASH, 1, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(d, Frame(KIND_HELLO, 0, 0, hello^))
    var base = UInt64(0)
    for entry in head._driver_conn.items():
        if entry.value == d:
            base = entry.key << DRIVER_SHIFT
    var w = _connect(head, peers)
    hello = [ROLE_WORKER]
    put_scalar[DType.uint64](hello, HASH)
    hello.extend(encode_addr([127, 0, 0, 1], 7000))
    head._handle(w, Frame(KIND_HELLO, 0, 0, hello^))
    var a = base | 1
    var b = a + 1 + (1 << 20)
    head._handle(d, Frame(KIND_TASK_STREAM, a, 7, _task_stream(2, 10)))
    head._handle(d, Frame(KIND_TASK_STREAM, b, 7, _task_stream(2, 10)))
    head._release_waiting()
    head._dispatch()
    var f = _to_worker(head, w, peers[1])
    assert_equal(f.kind, KIND_TASK_STREAM)
    assert_equal(f.object_id, a)
    # Stream a puts an item, then its driver releases it before its end.
    head._handle(w, Frame(KIND_RESULT, a + 1, a, List[UInt8](length=8, fill=0)))
    head._handle(d, Frame(KIND_RELEASE, a + 1, 0, List[UInt8]()))
    head._handle(d, Frame(KIND_RELEASE, a, 0, List[UInt8]()))
    f = _to_worker(head, w, peers[1])
    assert_equal(f.kind, KIND_STREAM_STOP)
    assert_equal(f.object_id, a)
    # What it answers after is dropped: an item, then its end.
    head._handle(w, Frame(KIND_RESULT, a + 2, a, List[UInt8](length=8, fill=0)))
    var cancelled = String("cancelled")
    head._handle(w, Frame(KIND_ERROR, a, 0, List[UInt8](cancelled.as_bytes())))
    assert_true(a + 2 not in head._refs and a + 2 not in head._results)
    assert_true(a not in head._errors)
    # Stream b runs next; its driver leaves while it waits for credit.
    head._release_waiting()
    head._dispatch()
    f = _to_worker(head, w, peers[1])
    assert_equal(f.kind, KIND_TASK_STREAM)
    assert_equal(f.object_id, b)
    head._handle(w, Frame(KIND_RESULT, b + 1, b, List[UInt8](length=8, fill=0)))
    head._drop(d)
    f = _to_worker(head, w, peers[1])
    assert_equal(f.kind, KIND_STREAM_STOP)
    assert_equal(f.object_id, b)
    head._handle(w, Frame(KIND_RESULT, b + 2, b, List[UInt8](length=8, fill=0)))
    head._handle(w, Frame(KIND_ERROR, b, 0, List[UInt8](cancelled.as_bytes())))
    assert_equal(len(head._inflight[w]), 0, "the worker is still busy")
    assert_equal(len(head._refs), 0)
    assert_equal(len(head._held), 0)
    assert_equal(len(head._dead), 0)
    assert_equal(len(head._streams), 0)
    assert_equal(len(head._stream_at), 0)
    assert_equal(len(head._results), 0)
    # The worker is free: another driver's task goes to it.
    var d2 = _connect(head, peers)
    hello = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(d2, Frame(KIND_HELLO, 0, 0, hello^))
    var base2 = UInt64(0)
    for entry in head._driver_conn.items():
        if entry.value == d2:
            base2 = entry.key << DRIVER_SHIFT
    head._handle(d2, Frame(KIND_TASK, base2 | 1, 7, List[UInt8]()))
    head._release_waiting()
    head._dispatch()
    f = _to_worker(head, w, peers[1])
    assert_equal(f.kind, KIND_TASK)
    assert_equal(f.object_id, base2 | 1)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
