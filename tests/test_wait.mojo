"""`wait`: finding which of several results are ready, without reading them."""

from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from dista import ObjectRef, Runtime, Task


def nap(ms: Int) -> Int:
    sleep(Float64(ms) / 1000.0)
    return ms


def fail(x: Int) raises -> Int:
    raise Error("no ", x)


def shout(x: Int) -> String:
    return String("#", x)


comptime Nap = Task[Int, Int, nap]
comptime Fail = Task[Int, Int, fail]
comptime Shout = Task[Int, String, shout]


def _seconds_since(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def test_wait_returns_the_first_to_finish() raises:
    var rt = Runtime[Nap](workers=2, pipeline=1)
    var refs: List[ObjectRef[Int]] = [rt.remote[Nap](700), rt.remote[Nap](50)]
    var t0 = perf_counter_ns()
    var done = rt.wait(refs)
    var took = _seconds_since(t0)
    assert_equal(len(done), 1)
    assert_equal(done[0], 1)
    assert_true(took < 0.5, String("waited ", took, " s for the short one"))
    # Waiting changes nothing: the result is still there to read.
    assert_equal(rt.take(refs[1]), 50)
    assert_equal(rt.take(refs[0]), 700)


def test_wait_for_several_and_for_all() raises:
    var rt = Runtime[Nap](workers=4, pipeline=1)
    var refs = List[ObjectRef[Int]]()
    for ms in [400, 30, 500, 20]:
        refs.append(rt.remote[Nap](ms))
    var two = rt.wait(refs, count=2)
    assert_equal(len(two), 2)
    assert_equal(two[0], 1)
    assert_equal(two[1], 3)
    # More than there are means all of them, in the list's order.
    var all = rt.wait(refs, count=99)
    assert_equal(len(all), 4)
    for i in range(4):
        assert_equal(all[i], i)
    # What had finished before the call counts at once.
    assert_equal(len(rt.wait(refs, count=1, timeout=0.0)), 4)
    rt.release(refs)


def test_wait_times_out_with_what_is_ready() raises:
    var rt = Runtime[Nap](workers=2, pipeline=1)
    var refs: List[ObjectRef[Int]] = [rt.remote[Nap](900), rt.remote[Nap](20)]
    assert_equal(len(rt.wait(refs, count=1)), 1)
    var t0 = perf_counter_ns()
    var done = rt.wait(refs, count=2, timeout=0.2)
    var took = _seconds_since(t0)
    assert_equal(len(done), 1)
    assert_equal(done[0], 1)
    assert_true(took >= 0.2 and took < 0.7, String("waited ", took, " s"))
    # A timeout of 0 only looks.
    t0 = perf_counter_ns()
    assert_equal(len(rt.wait(refs, count=2, timeout=0.0)), 1)
    assert_true(_seconds_since(t0) < 0.2)
    assert_equal(len(rt.wait(List[ObjectRef[Int]]())), 0)
    assert_equal(rt.take(refs[0]), 900)


def test_a_failed_task_is_ready() raises:
    var rt = Runtime[Nap, Fail](workers=2, pipeline=1)
    var refs: List[ObjectRef[Int]] = [rt.remote[Nap](700), rt.remote[Fail](7)]
    var done = rt.wait(refs)
    assert_equal(len(done), 1)
    assert_equal(done[0], 1)
    var msg = String()
    try:
        _ = rt.get(refs[1])
    except e:
        msg = String(e)
    assert_equal(msg, "no 7")


def test_wait_on_ids_of_several_types() raises:
    var rt = Runtime[Nap, Shout](workers=2, pipeline=1)
    var slow = rt.remote[Nap](700)
    var text = rt.remote[Shout](3)
    var ids: List[UInt64] = [slow.id, text.id]
    var done = rt.wait(ids)
    assert_equal(len(done), 1)
    assert_equal(done[0], 1)
    assert_equal(rt.take(text), "#3")


def test_wait_refuses_a_released_ref() raises:
    var rt = Runtime[Nap](workers=1)
    var r = rt.remote[Nap](1)
    assert_equal(rt.take(r), 1)
    var msg = String()
    try:
        var one: List[ObjectRef[Int]] = [r]
        _ = rt.wait(one)
    except e:
        msg = String(e)
    assert_true(msg.endswith("was released"), msg)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
