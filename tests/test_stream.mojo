"""`TaskStream`: a task that hands over each item as it makes it, read with
`rt.next`, on a worker with one slot and on one with several.

Slots are set with `DISTA_WORKER_SLOTS` before a `Runtime` forks its
workers, which inherit it, and unset once they're forked."""

from std.os import setenv, unsetenv
from std.testing import TestSuite, assert_equal, assert_false, assert_true
from std.time import perf_counter_ns, sleep

from dista import Emit, ObjectRef, Runtime, Task, TaskStream


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


def double(x: Int) -> Int:
    return 2 * x


def length(s: String) -> Int:
    return s.byte_length()


comptime CountUp = TaskStream[Int, Int, count_up]
comptime BigItems = TaskStream[Int, String, big_items]
comptime SlowTwo = TaskStream[Int, Int, slow_two]
comptime FiveThenFail = TaskStream[Int, Int, five_then_fail]
comptime Late = TaskStream[Int, Int, late]
comptime Double = Task[Int, Int, double]
comptime Length = Task[String, Int, length]


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


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
