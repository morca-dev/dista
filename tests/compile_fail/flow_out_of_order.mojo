"""Must not compile: a flow whose operators are out of order. `Label` makes
a `String`, and `AddOne` takes an `Int`, so `AddOne` can't come after it.
tests/compile_fail.sh checks the error says why."""

from dista import Emit, Runtime, Task, TaskStream
from dista.flow import Flow, Map, Read


def count_to(n: Int, mut out: Emit[Int]) raises:
    for k in range(n):
        out.put(k)


def label(x: Int) -> String:
    return String("#", x)


def add_one(x: Int) -> Int:
    return x + 1


comptime CountTo = TaskStream[Int, Int, count_to]
comptime Label = Task[Int, String, label]
comptime AddOne = Task[Int, Int, add_one]


def main() raises:
    var rt = Runtime[CountTo, Label, AddOne](workers=1)
    var inputs: List[Int] = [3]
    print(len(Flow[Read[CountTo], Map[Label], Map[AddOne]].run(rt, inputs^)))
