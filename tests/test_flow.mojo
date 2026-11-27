"""`dista.flow` in local mode: a `Read` and two `Map`s give exact results by
input and in the read's order, the read stops when the last operator is
held, an error a task or a stream raises fails the flow and leaves the
stores empty, and the edge cases: no inputs, an input with no items, and a
flow run twice.

The backpressure test's tasks find their directory in `FLOW_TEST_DIR`,
set before a `Runtime` forks its workers, which inherit it."""

from std.os import getenv, makedirs, setenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from dista import Emit, Runtime, Task, TaskLike, TaskStream
from dista.flow import Flow, Map, Read
from dista.socket import wall_ns

comptime ITEMS = 50


def items_of(i: Int, mut out: Emit[Int]) raises:
    """Input `i`'s 50 items: `i * 1000 + k`."""
    for k in range(ITEMS):
        out.put(i * 1000 + k)


def count_to(n: Int, mut out: Emit[Int]) raises:
    """`n` items, 0 to `n - 1`: none for 0."""
    for k in range(n):
        out.put(k)


def items_or_fail(i: Int, mut out: Emit[Int]) raises:
    """Input `i`'s items, but input 5 raises after 10 of them."""
    for k in range(ITEMS):
        if i == 5 and k == 10:
            raise Error("read broke on 5")
        out.put(i * 1000 + k)


def add_one(x: Int) -> Int:
    return x + 1


def checked_add_one(x: Int) raises -> Int:
    """`add_one`, raising on input 7's item 13."""
    if x == 7013:
        raise Error("bad batch 7013")
    return x + 1


def label(x: Int) -> String:
    return String("#", x)


def _dir() -> String:
    return getenv("FLOW_TEST_DIR")


def _read_int(path: String) -> Int:
    try:
        with open(path, "r") as f:
            return Int(f.read())
    except:
        return 0


def counted(i: Int, mut out: Emit[Int]) raises:
    """`items_of`, writing how many items it has put to `put-<i>` after each
    one."""
    var path = String(_dir(), "/put-", i)
    for k in range(ITEMS):
        out.put(i * 1000 + k)
        with open(path, "w") as f:
            f.write(String(k + 1))


def held(x: Int) raises -> String:
    """`label`, once the clock passes the deadline in `deadline`. The first
    call past it writes the items every stream had put by then to
    `snapshot`: nothing has gone through this operator until then."""
    var deadline = UInt64(_read_int(String(_dir(), "/deadline")))
    while wall_ns() < deadline:
        sleep(0.01)
    var snap = String(_dir(), "/snapshot")
    if not exists(snap):
        var total = 0
        for i in range(40):
            total += _read_int(String(_dir(), "/put-", i))
        with open(snap, "w") as f:
            f.write(String(total))
    return String("#", x)


comptime ItemsOf = TaskStream[Int, Int, items_of]
comptime CountTo = TaskStream[Int, Int, count_to]
comptime ItemsOrFail = TaskStream[Int, Int, items_or_fail]
comptime Counted = TaskStream[Int, Int, counted]
comptime AddOne = Task[Int, Int, add_one]
comptime CheckedAddOne = Task[Int, Int, checked_add_one]
comptime Label = Task[Int, String, label]
comptime Held = Task[Int, String, held]

comptime Labels = Flow[Read[ItemsOf], Map[AddOne], Map[Label]]


def _inputs(n: Int) -> List[Int]:
    var out = List[Int](capacity=n)
    for i in range(n):
        out.append(i)
    return out^


def _want(i: Int, k: Int) -> String:
    return String("#", i * 1000 + k + 1)


def _assert_empty[*Ts: TaskLike](mut rt: Runtime[*Ts]) raises:
    var st = rt.stats()
    assert_equal(st.objects, 0, String(st))
    assert_equal(st.results + st.located + st.errors, 0, String(st))


def test_run_gives_every_result_by_input_in_order() raises:
    var rt = Runtime[ItemsOf, AddOne, Label](workers=6)
    var out = Labels.run(rt, _inputs(40))
    assert_equal(len(out), 40 * ITEMS)
    for i in range(40):
        for k in range(ITEMS):
            assert_equal(out[i * ITEMS + k], _want(i, k))
    _assert_empty(rt)


def test_next_gives_each_input_once_in_the_reads_order() raises:
    var rt = Runtime[ItemsOf, AddOne, Label](workers=6)
    var run = Labels.start(rt, _inputs(40))
    var seen = List[Bool]()
    for _ in range(40):
        seen.append(False)
    var count = 0
    while True:
        var r = run.next(rt)
        if not r:
            break
        var done = r.take()
        assert_true(
            not seen[done.input], String("input ", done.input, " twice")
        )
        seen[done.input] = True
        count += 1
        assert_equal(len(done.refs), ITEMS)
        for k in range(ITEMS):
            assert_equal(rt.take(done.refs[k]), _want(done.input, k))
    assert_equal(count, 40)
    # Done, it stays done.
    assert_true(not run.next(rt))
    _assert_empty(rt)


def test_a_held_last_operator_stops_the_read() raises:
    comptime STREAMS = 2
    comptime WINDOW = 4
    comptime B1 = 3
    comptime B2 = 2
    comptime Bounded = Flow[
        Read[Counted, streams=STREAMS, window=WINDOW],
        Map[AddOne, budget=B1],
        Map[Held, budget=B2],
    ]
    var dir = String(
        getenv("TMPDIR", "/tmp"), "/dista-test-flow-", perf_counter_ns()
    )
    makedirs(dir)
    _ = setenv("FLOW_TEST_DIR", dir)
    # The last operator waits 3 s, long enough for the read to stop.
    with open(String(dir, "/deadline"), "w") as f:
        f.write(String(wall_ns() + 3_000_000_000))
    var rt = Runtime[Counted, AddOne, Held](workers=6)
    var out = Bounded.run(rt, _inputs(40))
    var put = _read_int(String(dir, "/snapshot"))
    print("  items put while the last operator was held:", put)
    # Each stream puts at most its window beyond what was taken, and what
    # was taken is in the two operators.
    assert_true(put <= STREAMS * WINDOW + B1 + B2, String(put, " items put"))
    assert_true(put >= STREAMS * WINDOW, String("only ", put, " items put"))
    assert_equal(len(out), 40 * ITEMS)
    for i in range(40):
        for k in range(ITEMS):
            assert_equal(out[i * ITEMS + k], String("#", i * 1000 + k + 1))
    _assert_empty(rt)


def test_a_raising_task_fails_the_flow_and_frees_everything() raises:
    comptime Checked = Flow[Read[ItemsOf], Map[CheckedAddOne], Map[Label]]
    var rt = Runtime[ItemsOf, AddOne, CheckedAddOne, Label](workers=6)
    var run = Checked.start(rt, _inputs(40))
    var msg = String()
    try:
        while True:
            var r = run.next(rt)
            if not r:
                break
            rt.release(r.value().refs)
    except e:
        msg = String(e)
    print("  ", msg)
    assert_true(
        msg.startswith("operator 1 (Map test_flow::checked_add_one)"), msg
    )
    assert_true(" failed on input 7: bad batch 7013" in msg, msg)
    # It stays failed.
    var again = String()
    try:
        _ = run.next(rt)
    except e:
        again = String(e)
    assert_equal(again, msg)
    _assert_empty(rt)
    # The runtime runs another flow.
    assert_equal(len(Labels.run(rt, _inputs(3))), 3 * ITEMS)


def test_a_raising_read_fails_the_flow_and_frees_everything() raises:
    comptime Failing = Flow[Read[ItemsOrFail], Map[AddOne], Map[Label]]
    var rt = Runtime[ItemsOrFail, AddOne, Label](workers=6)
    var msg = String()
    try:
        _ = Failing.run(rt, _inputs(40))
    except e:
        msg = String(e)
    print("  ", msg)
    assert_true(
        msg.startswith("operator 0 (Read test_flow::items_or_fail)"), msg
    )
    assert_true(" failed on input 5: read broke on 5" in msg, msg)
    _assert_empty(rt)


def test_no_inputs_an_empty_input_and_a_flow_run_twice() raises:
    comptime Counts = Flow[Read[CountTo], Map[AddOne], Map[Label]]
    var rt = Runtime[CountTo, ItemsOf, AddOne, Label](workers=6)
    assert_equal(len(Counts.run(rt, List[Int]())), 0)
    var none = Counts.start(rt, List[Int]())
    assert_true(not none.next(rt))
    # Input 1 puts nothing: it finishes with no results.
    var run = Counts.start(rt, [2, 0, 3])
    var got = 0
    while True:
        var r = run.next(rt)
        if not r:
            break
        var done = r.take()
        var want = [2, 0, 3][done.input]
        assert_equal(len(done.refs), want)
        for k in range(want):
            assert_equal(rt.take(done.refs[k]), String("#", k + 1))
        got += 1
    assert_equal(got, 3)
    var first = Labels.run(rt, _inputs(5))
    var second = Labels.run(rt, _inputs(5))
    assert_equal(len(first), 5 * ITEMS)
    for k in range(len(first)):
        assert_equal(first[k], second[k])
    _assert_empty(rt)


def test_an_unregistered_task_raises_at_start() raises:
    var rt = Runtime[ItemsOf, AddOne](workers=1)
    var msg = String()
    try:
        _ = Labels.start(rt, _inputs(1))
    except e:
        msg = String(e)
    assert_true(msg.endswith("is not registered with this runtime"), msg)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
