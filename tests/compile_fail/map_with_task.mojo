"""Must not compile: a `MapWith` over a plain `Task`. A `MapWith` takes a
`StatefulTask`, whose state each worker builds once; a `Task` goes in a
`Map`. tests/compile_fail.sh checks the error names the trait."""

from dista import Emit, Runtime, Task, TaskStream
from dista.flow import Flow, MapWith, Read


def numbers(n: Int, mut out: Emit[Int]) raises:
    for k in range(n):
        out.put(k)


def add_one(x: Int) -> Int:
    return x + 1


comptime Numbers = TaskStream[Int, Int, numbers]
comptime AddOne = Task[Int, Int, add_one]


def main() raises:
    var rt = Runtime[Numbers, AddOne](workers=1)
    var inputs: List[Int] = [3]
    print(len(Flow[Read[Numbers], MapWith[AddOne]].run(rt, inputs^)))
