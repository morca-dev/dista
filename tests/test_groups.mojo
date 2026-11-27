"""Worker groups: a task runs only on a worker of its group (`group=` on its
type, `DISTA_GROUP` on the worker), and a task with no group only on a
worker with none. In local mode, `Runtime(groups=...)` forks the grouped
workers, which get `DISTA_GROUP` set; each task here returns its worker's
group, so where it ran is in its result.

The runtime tests run on one slot and on four (`DISTA_WORKER_SLOTS`, set
before the `Runtime` forks its workers). The wire tests drive a `Head`
directly over socket pairs, as tests/test_stream.mojo does."""

from std.ffi import c_int, external_call
from std.os import getenv, listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists
from std.testing import (
    TestSuite,
    assert_equal,
    assert_false,
    assert_raises,
    assert_true,
)
from std.time import perf_counter_ns, sleep

from dista import (
    Actor,
    Emit,
    ObjectRef,
    Runtime,
    Seq,
    Task,
    TaskMany,
    TaskStream,
)
from dista.codec import put_scalar
from dista.conn import Conn
from dista.frame import (
    KIND_FREE,
    KIND_GROUPS,
    KIND_HELLO,
    KIND_TASK,
    ROLE_DRIVER,
    ROLE_WORKER,
    Frame,
    encode_addr,
    encode_groups,
    parse_groups,
    read_frame,
)
from dista.head import DRIVER_SHIFT, Head
from dista.socket import Listener, Stream, poll_readable, stream_pair
from dista.task import check_group
from dista.worker import worker_hello

comptime MARKS = "build/groups-marks"


def _pid() -> Int:
    return Int(external_call["getpid", c_int]())


def _here() -> String:
    """This worker's group, or "-" for none."""
    var g = getenv("DISTA_GROUP")
    return g if g != "" else String("-")


def in_a(x: Int) -> String:
    return _here()


def in_b(x: Int) -> String:
    return _here()


def in_none(x: Int) -> String:
    return _here()


def in_c(x: Int) -> String:
    return _here()


def many_in_b(n: Int) -> Seq[String]:
    var out = Seq[String]()
    for _ in range(n):
        out.items.append(_here())
    return out^


def stream_in_a(n: Int, mut out: Emit[String]) raises:
    for _ in range(n):
        out.put(_here())


def marked_in_a(i: Int) raises -> String:
    """Marks that task `i` started on this worker, naps, then returns this
    worker's group and pid."""
    var n = 0
    while True:
        var path = String(MARKS, "/", i, "-", _pid(), "-", n)
        if not exists(path):
            with open(path, "w") as f:
                f.write("ran")
            break
        n += 1
    sleep(1.0)
    return String(_here(), ":", _pid())


def upper(x: Int) -> String:
    return _here()


def new_state(x: Int) -> Int:
    return x


comptime InA = Task[Int, String, in_a, group="a"]
comptime InB = Task[Int, String, in_b, group="b"]
comptime InNone = Task[Int, String, in_none]
comptime InC = Task[Int, String, in_c, group="c"]
comptime ManyInB = TaskMany[Int, String, many_in_b, group="b"]
comptime StreamInA = TaskStream[Int, String, stream_in_a, group="a"]
comptime MarkedInA = Task[Int, String, marked_in_a, group="a"]
comptime BadGroup = Task[Int, String, upper, group="GPU"]
comptime GroupedActor = Actor[Int, Int, new_state, group="gpu"]


def _slots(n: Int):
    if n == 1:
        _ = unsetenv("DISTA_WORKER_SLOTS")
    else:
        _ = setenv("DISTA_WORKER_SLOTS", String(n))


def _each_on_its_group(slots: Int) raises:
    """200 tasks in groups a and b and 50 in none: each ran on a worker of
    its own group, and none of the 50 on a grouped worker."""
    _slots(slots)
    var rt = Runtime[InA, InB, InNone](workers=2, groups="a=2,b=1")
    _slots(1)
    var a = List[ObjectRef[String]]()
    var b = List[ObjectRef[String]]()
    var none = List[ObjectRef[String]]()
    for i in range(100):
        a.append(rt.remote[InA](i))
        b.append(rt.remote[InB](i))
        if i % 2 == 0:
            none.append(rt.remote[InNone](i))
    for r in rt.take(a):
        assert_equal(r, "a")
    for r in rt.take(b):
        assert_equal(r, "b")
    for r in rt.take(none):
        assert_equal(r, "-")


def test_each_task_runs_in_its_group_on_one_slot() raises:
    _each_on_its_group(1)


def test_each_task_runs_in_its_group_on_four_slots() raises:
    _each_on_its_group(4)


def _many_and_stream(slots: Int) raises:
    """A `TaskMany` of group b and a stream of group a keep to their group,
    beside tasks of no group."""
    _slots(slots)
    var rt = Runtime[ManyInB, StreamInA, InNone](workers=1, groups="a=1,b=1")
    _slots(1)
    var none = rt.remote[InNone](0)
    var parts = rt.remote_many[ManyInB](5, 5)
    var s = rt.stream[StreamInA](20)
    var items = 0
    while True:
        var r = rt.next(s)
        if not r:
            break
        assert_equal(rt.take(r.value()), "a")
        items += 1
    assert_equal(items, 20)
    for p in rt.take(parts):
        assert_equal(p, "b")
    assert_equal(rt.take(none), "-")


def test_task_many_and_stream_keep_to_their_group_on_one_slot() raises:
    _many_and_stream(1)


def test_task_many_and_stream_keep_to_their_group_on_four_slots() raises:
    _many_and_stream(4)


def _no_worker(slots: Int) raises:
    """A task of a group no local worker is in fails at once; the others
    still run."""
    _slots(slots)
    var rt = Runtime[InC, InNone](workers=1)
    _slots(1)
    var t0 = perf_counter_ns()
    var c = rt.remote[InC](1)
    with assert_raises(contains="no worker of group c"):
        _ = rt.get(c, 10.0)
    assert_true(
        perf_counter_ns() - t0 < 5_000_000_000, "the task didn't fail at once"
    )
    assert_equal(rt.take(rt.remote[InNone](1)), "-")


def test_a_task_of_a_group_with_no_worker_fails_at_once_on_one_slot() raises:
    _no_worker(1)


def test_a_task_of_a_group_with_no_worker_fails_at_once_on_four_slots() raises:
    _no_worker(4)


def _clear() raises:
    if not exists(MARKS):
        makedirs(MARKS)
    for name in listdir(MARKS):
        remove(String(MARKS, "/", name))


def _started() raises -> List[List[Int]]:
    """For each of 4 marked tasks, the pids it started on."""
    var out = List[List[Int]]()
    for _ in range(4):
        out.append(List[Int]())
    for name in listdir(MARKS):
        var parts = name.split("-")
        out[Int(parts[0])].append(Int(parts[1]))
    return out^


def _killed_in_group(slots: Int) raises:
    """Two workers in group a and two in none; 4 slow tasks of group a run,
    and one of the two a workers is killed. Its tasks run again on the other
    a worker, never on one of no group."""
    _clear()
    _slots(slots)
    var rt = Runtime[MarkedInA, InNone](workers=2, pipeline=1, groups="a=2")
    _slots(1)
    var refs = List[ObjectRef[String]]()
    for i in range(4):
        refs.append(rt.remote[MarkedInA](i))
    # Wait until a task is running on a worker, and kill that worker.
    var victim = -1
    for _ in range(500):
        var m = _started()
        if len(m[0]) > 0:
            victim = m[0][0]
            break
        sleep(0.01)
    assert_true(victim > 0, "no task started")
    _ = external_call["kill", c_int](c_int(victim), c_int(9))
    var pids = List[Int]()
    for i in range(4):
        var r = rt.take(refs[i])
        assert_true(r.startswith("a:"), String("ran outside group a: ", r))
        var pid = Int(r.split(":")[1])
        assert_true(pid != victim, "a result came from the killed worker")
        pids.append(pid)
    # Task 0 ran on the victim first, and again on the other a worker.
    var m = _started()
    assert_true(len(m[0]) >= 2, "task 0 didn't run again")
    assert_equal(rt.take(rt.remote[InNone](0)), "-")
    _clear()


def test_a_killed_workers_tasks_run_again_in_its_group_on_one_slot() raises:
    _killed_in_group(1)


def test_a_killed_workers_tasks_run_again_in_its_group_on_four_slots() raises:
    _killed_in_group(4)


def test_a_bad_group_name_is_refused() raises:
    with assert_raises(contains="bad worker group name 'GPU'"):
        _ = Runtime[BadGroup](workers=1)
    with assert_raises(contains="bad worker group name"):
        _ = Runtime[InNone](workers=1, groups="Big=1")
    with assert_raises(contains="bad worker group name"):
        _ = Runtime[InNone](workers=1, groups="=1")
    check_group("gpu-2_x")
    check_group(String("a") * 32)
    with assert_raises(contains="bad worker group name"):
        check_group(String("a") * 33)
    with assert_raises(contains="bad worker group name"):
        check_group("")
    with assert_raises(contains="bad worker group name"):
        check_group("gpu.1")
    # The groups frame's names are checked too.
    var bad = encode_groups([UInt64(7)], [String("ok")])
    bad[len(bad) - 1] = UInt8(ord("K"))
    with assert_raises(contains="bad worker group name"):
        _ = parse_groups(bad)


def test_a_program_without_groups_sends_what_it_sent_before() raises:
    """No group: the hello is the one from before groups, and the registry
    has no groups frame to send. With one, the slots come first, then the
    group; the table goes through `parse_groups` unchanged."""
    var ip: Array[UInt8, 4] = [127, 0, 0, 1]
    var old: List[UInt8] = [ROLE_WORKER]
    put_scalar[DType.uint64](old, 0x5EED)
    old.extend(encode_addr(ip, 7000))
    assert_true(worker_hello(0x5EED, ip, 7000, 1) == old)
    var old4 = old.copy()
    put_scalar[DType.uint64](old4, 4)
    assert_true(worker_hello(0x5EED, ip, 7000, 4) == old4)
    var grouped = old.copy()
    put_scalar[DType.uint64](grouped, 1)
    grouped.append(3)
    grouped.extend("gpu".as_bytes())
    assert_true(worker_hello(0x5EED, ip, 7000, 1, "gpu") == grouped)
    assert_equal(len(Runtime[InNone, ManyInB]._groups_frame()), 18)
    assert_equal(len(Runtime[InNone]._groups_frame()), 0)
    var frame = Runtime[InNone, InA, StreamInA, GroupedActor]._groups_frame()
    var table = parse_groups(frame)
    assert_equal(len(table), 3)
    assert_equal(table[InA.id()], "a")
    assert_equal(table[StreamInA.id()], "a")
    assert_equal(table[GroupedActor.id()], "gpu")
    assert_equal(GroupedActor.group_name(), "gpu")
    assert_equal(InNone.group_name(), "")


comptime HASH: UInt64 = 0x5EED


def _connect(mut head: Head, mut peers: List[Stream]) raises -> Int:
    var pair = stream_pair()
    peers.append(pair.pop())
    return head._add_conn(Conn(pair.pop()))


def _sent(mut head: Head, w: Int, s: Stream) raises -> Optional[Frame]:
    """The next frame the head sent worker `w` within 0.2 s, skipping its
    capacity announcement (a free of object 0)."""
    head._conns[w].flush()
    while True:
        if not poll_readable([s.fd], 200)[0]:
            return None
        var f = read_frame(s)
        if not f:
            raise Error("the head hung up")
        var frame = f.take()
        if frame.kind == KIND_FREE and frame.object_id == 0:
            continue
        return frame^


def _worker(
    mut head: Head, mut peers: List[Stream], group: String
) raises -> Int:
    var w = _connect(head, peers)
    head._handle(
        w,
        Frame(
            KIND_HELLO, 0, 0, worker_hello(HASH, [127, 0, 0, 1], 7000, 1, group)
        ),
    )
    return w


def test_a_cluster_keeps_a_task_of_a_group_with_no_worker_queued() raises:
    """In a cluster the head sends a task only to a worker of its group:
    with none, it waits, and goes to the first that joins."""
    var head = Head(Listener(0), HASH, 1, False)
    var peers = List[Stream]()
    var d = _connect(head, peers)
    var hello: List[UInt8] = [ROLE_DRIVER]
    put_scalar[DType.uint64](hello, HASH)
    head._handle(d, Frame(KIND_HELLO, 0, 0, hello^))
    head._handle(
        d, Frame(KIND_GROUPS, 0, 0, encode_groups([UInt64(7)], ["gpu"]))
    )
    var base = UInt64(0)
    for entry in head._driver_conn.items():
        if entry.value == d:
            base = entry.key << DRIVER_SHIFT
    var plain = _worker(head, peers, "")
    var other = _worker(head, peers, "io")
    assert_equal(head._group[other], "io")
    head._handle(d, Frame(KIND_TASK, base | 1, 7, List[UInt8]()))
    head._handle(d, Frame(KIND_TASK, base | 2, 8, List[UInt8]()))
    head._release_waiting()
    head._dispatch()
    var f = _sent(head, plain, peers[1])
    assert_true(Bool(f) and f.value().object_id == base | 2)
    assert_false(Bool(_sent(head, plain, peers[1])), "no group to plain")
    assert_false(Bool(_sent(head, other, peers[2])), "gpu task to io worker")
    assert_equal(head._queued(), 1)
    var gpu = _worker(head, peers, "gpu")
    head._dispatch()
    f = _sent(head, gpu, peers[3])
    assert_true(Bool(f) and f.value().object_id == base | 1)
    assert_true("group gpu" in head._status())
    assert_true('"group":"gpu"' in head._status_json())
    _ = len(peers)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
