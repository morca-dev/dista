"""`StatefulTask` in local mode: each worker builds its state once however
many calls it serves, and the driver never builds one; a task of a group
runs on that group's workers, and a killed worker's calls finish on another,
which builds its own state; on a worker with 4 slots one type's calls never
overlap while a plain task runs beside them; a raising `new` fails the call,
and a later call builds the state once `new` can; a `Map` refuses a
`StatefulTask`.

The tasks find their directory in `STATEFUL_TEST_DIR`, set before a
`Runtime` forks its workers, which inherit it. Each build writes a file
`build-<pid>-<n>` there, so the test counts the builds and sees which
process ran them."""

from std.ffi import c_int, external_call
from std.os import getenv, listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists
from std.testing import TestSuite, assert_equal, assert_raises, assert_true
from std.time import sleep

from dista import ObjectRef, Runtime, StatefulTask, Task, TaskStream, Emit
from dista.flow import Flow, Map, Read
from dista.socket import wall_ns

comptime DIR = "build/stateful-test"


def _pid() -> Int:
    return Int(external_call["getpid", c_int]())


def _here() -> String:
    """This worker's group, or "-" for none."""
    var g = getenv("DISTA_GROUP")
    return g if g != "" else String("-")


struct Model(Movable):
    """A state that knows which build it is, and whether a call holds it."""

    var built: String
    """Its build's file name: `build-<pid>-<n>`."""
    var busy: Bool

    def __init__(out self, var built: String):
        self.built = built^
        self.busy = False


def load_model() raises -> Model:
    """Records the build in a file of its own, unless `fail-new` exists."""
    var dir = getenv("STATEFUL_TEST_DIR")
    if exists(String(dir, "/fail-new")):
        raise Error("no model yet")
    var n = 0
    while exists(String(dir, "/build-", _pid(), "-", n)):
        n += 1
    var name = String("build-", _pid(), "-", n)
    with open(String(dir, "/", name), "w") as f:
        f.write("built")
    return Model(name^)


def which(mut m: Model, x: Int) raises -> String:
    """The worker's group and the state's build, for call `x`."""
    return String(_here(), ":", m.built)


def slow_which(mut m: Model, x: Int) raises -> String:
    """`which`, after 50 ms."""
    sleep(0.05)
    return String(_here(), ":", m.built)


def guarded(mut m: Model, x: Int) raises -> String:
    """Holds the state for 50 ms, raising if another call holds it, and
    returns when it entered and left, in nanoseconds."""
    if m.busy:
        raise Error("two calls overlapped")
    m.busy = True
    var entered = wall_ns()
    sleep(0.05)
    var left = wall_ns()
    m.busy = False
    return String(entered, ":", left)


def plain_nap(x: Int) raises -> String:
    """A plain task's 300 ms, as when it entered and left."""
    var entered = wall_ns()
    sleep(0.3)
    return String(entered, ":", wall_ns())


def numbers(n: Int, mut out: Emit[Int]) raises:
    for k in range(n):
        out.put(k)


comptime Which = StatefulTask[Model, Int, String, load_model, which]
comptime GpuWhich = StatefulTask[
    Model, Int, String, load_model, slow_which, group="gpu"
]
comptime Guarded = StatefulTask[Model, Int, String, load_model, guarded]
comptime PlainNap = Task[Int, String, plain_nap]
comptime Numbers = TaskStream[Int, Int, numbers]


def _clear() raises:
    if not exists(DIR):
        makedirs(DIR)
    for name in listdir(DIR):
        remove(String(DIR, "/", name))
    _ = setenv("STATEFUL_TEST_DIR", DIR)


def _builds() raises -> List[String]:
    var out = List[String]()
    for name in listdir(DIR):
        if name.startswith("build-"):
            out.append(name)
    return out^


def _pid_of_build(build: String) raises -> Int:
    return Int(build.split("-")[1])


def test_new_runs_once_per_worker() raises:
    """3 workers serve 300 calls with at most 3 builds, none in the driver,
    and each result names one of them."""
    _clear()
    var rt = Runtime[Which](workers=3)
    var refs = List[ObjectRef[String]]()
    for i in range(300):
        refs.append(rt.remote[Which](i))
    var seen = Dict[String, Int]()
    for r in rt.take(refs):
        var build = String(r.split(":")[1])
        seen[build] = seen.get(build, 0) + 1
    var builds = _builds()
    assert_true(len(builds) >= 1 and len(builds) <= 3, String(len(builds)))
    assert_true(len(seen) <= len(builds), "a call named a build not on disk")
    for b in builds:
        assert_true(_pid_of_build(b) != _pid(), "the driver built a state")
    _clear()


def test_a_grouped_stateful_task_runs_on_its_group() raises:
    """With two gpu workers and one in no group, every call runs on a gpu
    worker, and only gpu workers build the state."""
    _clear()
    var rt = Runtime[GpuWhich](workers=1, groups="gpu=2")
    var refs = List[ObjectRef[String]]()
    for i in range(40):
        refs.append(rt.remote[GpuWhich](i))
    for r in rt.take(refs):
        assert_true(r.startswith("gpu:"), String("ran outside gpu: ", r))
    assert_true(len(_builds()) <= 2, String(len(_builds())))
    _clear()


def test_a_killed_workers_calls_finish_on_the_other() raises:
    """One of two gpu workers is killed once it has built its state: every
    call still finishes on a gpu worker, and the other worker built a state
    of its own."""
    _clear()
    var rt = Runtime[GpuWhich](workers=1, pipeline=1, groups="gpu=2")
    var refs = List[ObjectRef[String]]()
    for i in range(60):
        refs.append(rt.remote[GpuWhich](i))
    var victim = -1
    for _ in range(500):
        var builds = _builds()
        if len(builds) > 0:
            victim = _pid_of_build(builds[0])
            break
        sleep(0.01)
    assert_true(victim > 0, "no state was built")
    _ = external_call["kill", c_int](c_int(victim), c_int(9))
    var survivor_ran = 0
    for r in rt.take(refs):
        assert_true(r.startswith("gpu:"), String("ran outside gpu: ", r))
        if _pid_of_build(String(r.split(":")[1])) != victim:
            survivor_ran += 1
    assert_true(survivor_ran > 0, "no call ran on the other gpu worker")
    var others = 0
    for b in _builds():
        if _pid_of_build(b) != victim:
            others += 1
    assert_equal(others, 1)
    _clear()


def test_calls_never_overlap_on_four_slots() raises:
    """On one worker of 4 slots, 12 calls of one stateful task run one at a
    time, while two plain tasks run beside them."""
    _clear()
    _ = setenv("DISTA_WORKER_SLOTS", "4")
    var rt = Runtime[Guarded, PlainNap](workers=1, pipeline=8)
    _ = unsetenv("DISTA_WORKER_SLOTS")
    var plain = List[ObjectRef[String]]()
    plain.append(rt.remote[PlainNap](0))
    var calls = List[ObjectRef[String]]()
    for i in range(12):
        calls.append(rt.remote[Guarded](i))
    plain.append(rt.remote[PlainNap](1))
    var spans = List[Tuple[Int, Int]]()
    for r in rt.take(calls):
        var parts = r.split(":")
        spans.append((Int(parts[0]), Int(parts[1])))
    for i in range(len(spans)):
        for j in range(i + 1, len(spans)):
            var a = spans[i]
            var b = spans[j]
            assert_true(
                a[1] <= b[0] or b[1] <= a[0],
                String("calls ", i, " and ", j, " overlapped"),
            )
    var beside = False
    for r in rt.take(plain):
        var parts = r.split(":")
        var start = Int(parts[0])
        var end = Int(parts[1])
        for s in spans:
            if s[0] < end and start < s[1]:
                beside = True
    assert_true(beside, "no plain task ran beside a stateful call")
    assert_equal(len(_builds()), 1)
    _clear()


def test_a_raising_new_fails_the_call_and_a_later_one_builds() raises:
    """While `new` raises, a call fails with its error; once it can build,
    the next call succeeds, and the state was built once."""
    _clear()
    with open(String(DIR, "/fail-new"), "w") as f:
        f.write("")
    var rt = Runtime[Which](workers=1)
    with assert_raises(contains="no model yet"):
        _ = rt.take(rt.remote[Which](0))
    with assert_raises(contains="no model yet"):
        _ = rt.take(rt.remote[Which](1))
    assert_equal(len(_builds()), 0)
    remove(String(DIR, "/fail-new"))
    assert_true(rt.take(rt.remote[Which](2)).startswith("-:build-"))
    assert_true(rt.take(rt.remote[Which](3)).startswith("-:build-"))
    assert_equal(len(_builds()), 1)
    _clear()


def test_a_map_refuses_a_stateful_task() raises:
    """A `StatefulTask` goes in a `MapWith`: `Map` raises at `start`."""
    var rt = Runtime[Numbers, Which](workers=1)
    with assert_raises(contains="a StatefulTask goes in a MapWith"):
        _ = Flow[Read[Numbers], Map[Which]].start(rt, [3])


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
