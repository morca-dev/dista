"""The driver tests/flow_loss.sh runs: a flow whose worker holding finished
batches is killed.

Every batch is 100 KB, more than a worker sends back inline, so each stays
on the worker that made it. `widen`, the first `Map`, appends a line to
`FLOW_LOSS_DIR/made-<pid>` for each batch it makes. `seal`, the last, waits
for `FLOW_LOSS_DIR/go`, so the first `Map`'s results pile up on the workers,
finished and not yet taken. The script kills the worker that made the most
of them, by its pid, and then makes `go`. The batches it held are lost, and
the flow reads their inputs again: the results must be exact, with at least
one rerun counted.
"""

from std.ffi import c_int, external_call
from std.os import getenv
from std.os.path import exists
from std.time import sleep

from dista import Emit, Runtime, Task, TaskStream
from dista.flow import Flow, Map, Read

comptime INPUTS = 6
comptime ITEMS = 20
comptime SIZE = 100_000


def _dir() -> String:
    return getenv("FLOW_LOSS_DIR")


def batches(i: Int, mut out: Emit[String]) raises:
    """Input `i`'s batches: each names its input and item, padded to 100 KB."""
    for k in range(ITEMS):
        var head = String(i, ":", k, ":")
        out.put(head + String("x") * (SIZE - head.byte_length()))


def widen(b: String) raises -> String:
    """The batch with one more character, which leaves it on this worker."""
    var pid = external_call["getpid", c_int]()
    with open(String(_dir(), "/made-", pid), "a") as f:
        f.write("1\n")
    return b + "y"


def seal(b: String) raises -> String:
    """The batch's input and item and its length, once `go` exists."""
    var go = String(_dir(), "/go")
    for _ in range(600):
        if exists(go):
            break
        sleep(0.1)
    var parts = b.split(":")
    return String(parts[0], ":", parts[1], ":", b.byte_length())


comptime Batches = TaskStream[Int, String, batches]
comptime Widen = Task[String, String, widen]
comptime Seal = Task[String, String, seal]

comptime Sealed = Flow[
    Read[Batches, streams=2, window=4],
    Map[Widen, budget=8],
    Map[Seal, budget=2],
]


def main() raises:
    var rt = Runtime[Batches, Widen, Seal](workers=4)
    var inputs = List[Int]()
    for i in range(INPUTS):
        inputs.append(i)
    var run = Sealed.start(rt, inputs^)
    var by_input = List[List[String]](length=INPUTS, fill=List[String]())
    while True:
        var r = run.next(rt)
        if not r:
            break
        var done = r.take()
        for k in range(len(done.refs)):
            by_input[done.input].append(rt.take(done.refs[k]))
    var st = run.stats()
    print("stats:", st)
    for i in range(INPUTS):
        if len(by_input[i]) != ITEMS:
            raise Error("input ", i, " gave ", len(by_input[i]), " results")
        for k in range(ITEMS):
            var want = String(i, ":", k, ":", SIZE + 1)
            if by_input[i][k] != want:
                raise Error(
                    "result ", k, " of input ", i, " was ", by_input[i][k]
                )
    if st.reruns() < 1:
        raise Error("no input was read again")
    print(
        "PASS flow loss:",
        INPUTS * ITEMS,
        "results exact,",
        st.reruns(),
        "inputs read again",
    )
