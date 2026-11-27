"""`dista.flow` in local mode: a `Read` and two `Map`s give exact results by
input and in the read's order, the read stops when the last operator is
held, an error a task or a stream raises fails the flow and leaves the
stores empty, and the edge cases: no inputs, an input with no items, and a
flow run twice. A task or a stream that kills its worker once makes its
input read again, with results still exact, and an input lost on every
attempt fails the flow after its retries. A `MapWith` in a gpu group runs
every batch on a gpu worker, each of which builds its state once.

The tasks that write files find their directory in `FLOW_TEST_DIR`, set
before a `Runtime` forks its workers, which inherit it."""

from std.ffi import c_int, c_uint, external_call
from std.os import getenv, listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_true
from std.time import perf_counter_ns, sleep

from dista import Emit, Runtime, StatefulTask, Task, TaskLike, TaskStream
from dista.flow import Flow, Map, MapWith, Read
from dista.loss import is_loss
from dista.socket import exit_now, wall_ns

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


def _first_time(name: String) raises -> Bool:
    """Whether this is the first call to ask for `name`: it leaves a file
    for the next one."""
    var path = String(_dir(), "/", name)
    if exists(path):
        return False
    with open(path, "w") as f:
        f.write("1")
    return True


def add_one_dying_once(x: Int) raises -> Int:
    """`add_one`, but the first call on input 3's item 20 kills its worker."""
    if x == 3020 and _first_time("died"):
        exit_now(9)
    return x + 1


def items_dying_once(i: Int, mut out: Emit[Int]) raises:
    """`items_of`, but input 4's first stream kills its worker once 10 items
    have reached the head."""
    for k in range(ITEMS):
        if i == 4 and k == 10 and _first_time("died"):
            sleep(0.5)
            exit_now(9)
        out.put(i * 1000 + k)


def items_dying_on_2(i: Int, mut out: Emit[Int]) raises:
    """`items_of`, but every stream of input 2 kills its worker once 10
    items have reached the head."""
    for k in range(ITEMS):
        if i == 2 and k == 10:
            sleep(0.5)
            exit_now(9)
        out.put(i * 1000 + k)


comptime ItemsOf = TaskStream[Int, Int, items_of]
comptime CountTo = TaskStream[Int, Int, count_to]
comptime ItemsOrFail = TaskStream[Int, Int, items_or_fail]
comptime Counted = TaskStream[Int, Int, counted]
comptime AddOne = Task[Int, Int, add_one]
comptime CheckedAddOne = Task[Int, Int, checked_add_one]
comptime Label = Task[Int, String, label]
comptime Held = Task[Int, String, held]
comptime AddOneDyingOnce = Task[Int, Int, add_one_dying_once]
comptime ItemsDyingOnce = TaskStream[Int, Int, items_dying_once]
comptime ItemsDyingOn2 = TaskStream[Int, Int, items_dying_on_2]

comptime Labels = Flow[Read[ItemsOf], Map[AddOne], Map[Label]]


@fieldwise_init
struct Doubler(Movable):
    """A state: the factor to multiply by, and the group it was built in."""

    var factor: Int
    var group: String


def new_doubler() raises -> Doubler:
    """Writes `build-<pid>` to `FLOW_TEST_DIR` the first time this process
    builds one, and `again-<pid>` after."""
    var g = getenv("DISTA_GROUP")
    var pid = Int(external_call["getpid", c_int]())
    var path = String(_dir(), "/build-", pid)
    if exists(path):
        path = String(_dir(), "/again-", pid)
    with open(path, "w") as f:
        f.write(g)
    return Doubler(2, g if g != "" else String("-"))


def double(mut d: Doubler, x: Int) raises -> String:
    """`x` times the factor, tagged with the state's group."""
    return String(d.group, ":", x * d.factor)


def untag(s: String) raises -> Int:
    """The number of a `double` result, which must come from a gpu worker."""
    if not s.startswith("gpu:"):
        raise Error("a MapWith batch ran outside gpu: ", s)
    return Int(s.split(":")[1])


comptime Double = StatefulTask[
    Doubler, Int, String, new_doubler, double, group="gpu"
]
comptime Untag = Task[String, Int, untag]
comptime Doubled = Flow[
    Read[ItemsOf], Map[AddOne], MapWith[Double, budget=8], Map[Untag]
]


def _inputs(n: Int) -> List[Int]:
    var out = List[Int](capacity=n)
    for i in range(n):
        out.append(i)
    return out^


def _want(i: Int, k: Int) -> String:
    return String("#", i * 1000 + k + 1)


def _fresh_dir() raises -> String:
    """A new directory for this test's files, in `FLOW_TEST_DIR`."""
    var dir = String(
        getenv("TMPDIR", "/tmp"), "/dista-test-flow-", perf_counter_ns()
    )
    makedirs(dir)
    _ = setenv("FLOW_TEST_DIR", dir)
    return dir^


def _assert_exact(got: List[String], inputs: Int) raises:
    assert_equal(len(got), inputs * ITEMS)
    for i in range(inputs):
        for k in range(ITEMS):
            assert_equal(got[i * ITEMS + k], _want(i, k))


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
    # A raised error isn't a loss: nothing was read again.
    assert_equal(run.stats().reruns(), 0)
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


def test_loss_errors_are_told_from_raised_ones() raises:
    assert_true(is_loss("worker lost while running this task"))
    assert_true(
        is_loss(
            "worker lost while running this stream, after some of its items"
            " were answered"
        )
    )
    assert_true(
        is_loss(
            "input failed: result 123 lost: its worker went away, and dista"
            " doesn't rebuild lost results; run the job again"
        )
    )
    assert_true(is_loss("input lost: the worker holding it (x) went away"))
    assert_true(is_loss("stream interrupted: the head restarted"))
    assert_true(not is_loss("cancelled"))
    assert_true(not is_loss("timed out: ran longer than DISTA_TASK_TIMEOUT"))
    assert_true(not is_loss("input failed: bad batch 7013"))
    assert_true(not is_loss("result x lost: its worker went away"))


def test_a_map_whose_worker_dies_reads_its_input_again() raises:
    """With `DISTA_TASK_RETRIES=0` the head doesn't run the killed task
    again, and fails it with a loss: the flow reads its input again."""
    _ = _fresh_dir()
    _ = setenv("DISTA_TASK_RETRIES", "0")
    var rt = Runtime[ItemsOf, AddOneDyingOnce, Label](workers=6)
    _ = unsetenv("DISTA_TASK_RETRIES")
    comptime Dying = Flow[Read[ItemsOf], Map[AddOneDyingOnce], Map[Label]]
    var run = Dying.start(rt, _inputs(10))
    var by_input = List[List[String]](length=10, fill=List[String]())
    while True:
        var r = run.next(rt)
        if not r:
            break
        var done = r.take()
        for k in range(len(done.refs)):
            by_input[done.input].append(rt.take(done.refs[k]))
    var st = run.stats()
    print("  ", st)
    assert_true(st.ops[1].reruns >= 1, String(st))
    assert_equal(st.inputs_done, 10)
    var out = List[String]()
    for i in range(10):
        out.extend(by_input[i].copy())
    _assert_exact(out, 10)
    _assert_empty(rt)


def test_a_read_whose_worker_dies_after_some_items_reads_its_input_again() raises:
    _ = _fresh_dir()
    var rt = Runtime[ItemsDyingOnce, AddOne, Label](workers=6)
    comptime Dying = Flow[Read[ItemsDyingOnce], Map[AddOne], Map[Label]]
    var run = Dying.start(rt, _inputs(10))
    var by_input = List[List[String]](length=10, fill=List[String]())
    while True:
        var r = run.next(rt)
        if not r:
            break
        var done = r.take()
        for k in range(len(done.refs)):
            by_input[done.input].append(rt.take(done.refs[k]))
    var st = run.stats()
    print("  ", st)
    assert_equal(st.ops[0].reruns, 1, String(st))
    assert_equal(st.ops[0].submitted, 11, String(st))
    var out = List[String]()
    for i in range(10):
        out.extend(by_input[i].copy())
    _assert_exact(out, 10)
    _assert_empty(rt)


def test_an_input_lost_on_every_attempt_fails_after_its_retries() raises:
    _ = _fresh_dir()
    var rt = Runtime[ItemsDyingOn2, AddOne, Label](workers=6)
    comptime Dying = Flow[Read[ItemsDyingOn2], Map[AddOne], Map[Label]]
    var msg = String()
    try:
        _ = Dying.run(rt, _inputs(6), retries=2)
    except e:
        msg = String(e)
    print("  ", msg)
    assert_true(
        msg.startswith(
            "operator 0 (Read test_flow::items_dying_on_2) failed on input 2"
            " after 3 attempts: worker lost while running this stream"
        ),
        msg,
    )
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


def test_a_map_with_runs_on_gpu_workers_with_a_state_each() raises:
    var dir = "build/flow-map-with"
    if not exists(dir):
        makedirs(dir)
    for name in listdir(dir):
        remove(String(dir, "/", name))
    _ = setenv("FLOW_TEST_DIR", dir)
    # Two workers of no group: the read opens one stream, which holds one of
    # them, and the other runs the `Map`s (#166).
    var rt = Runtime[ItemsOf, AddOne, Double, Untag](workers=2, groups="gpu=2")
    var out = Doubled.run(rt, _inputs(20))
    assert_equal(len(out), 20 * ITEMS)
    for i in range(20):
        for k in range(ITEMS):
            assert_equal(out[i * ITEMS + k], (i * 1000 + k + 1) * 2)
    var builds = 0
    for name in listdir(dir):
        assert_true(name.startswith("build-"), String("built twice: ", name))
        with open(String(dir, "/", name), "r") as f:
            assert_equal(f.read(), "gpu")
        builds += 1
    assert_true(builds >= 1 and builds <= 2, String(builds))
    _assert_empty(rt)


def _watchdog(seconds: Int):
    """Ends this process with SIGALRM after `seconds`, so a flow that hangs
    fails the test file rather than blocking it; 0 turns it off."""
    _ = external_call["alarm", c_uint](c_uint(seconds))


def test_a_read_with_more_streams_than_workers_finishes() raises:
    """Two workers of no group, a read of 4 streams and a `Map` in the same
    group (#166): the streams mustn't hold both workers."""
    var rt = Runtime[ItemsOf, AddOne, Label](workers=2)
    _watchdog(60)
    var out = Labels.run(rt, _inputs(10))
    _watchdog(0)
    _assert_exact(out, 10)
    _assert_empty(rt)


def test_a_read_on_one_worker_of_two_slots_finishes() raises:
    """The same with one worker of two slots: a stream holds a slot, not a
    worker."""
    _ = setenv("DISTA_WORKER_SLOTS", "2")
    var rt = Runtime[ItemsOf, AddOne, Label](workers=1)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    _watchdog(60)
    var out = Labels.run(rt, _inputs(10))
    _watchdog(0)
    _assert_exact(out, 10)
    _assert_empty(rt)


def test_a_read_whose_stream_would_hold_the_only_slot_raises() raises:
    """One worker of one slot: the read's one stream would hold it, and the
    `Map` would never run. The flow raises, at `start` or at a `next`,
    depending on whether the worker has said hello yet."""
    var rt = Runtime[ItemsOf, AddOne, Label](workers=1)
    _watchdog(60)
    var msg = String()
    try:
        _ = Labels.run(rt, _inputs(3))
    except e:
        msg = String(e)
    _watchdog(0)
    print("  ", msg)
    assert_true(
        msg.startswith(
            "operator 0 (Read test_flow::items_of) can't run: no group's"
            " workers have 1 slot"
        ),
        msg,
    )
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
