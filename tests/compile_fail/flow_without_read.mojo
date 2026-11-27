"""Must not compile: a flow whose first operator isn't a `Read`. A flow's
inputs are the read's arguments, and each is read by one stream.
tests/compile_fail.sh checks the error says so."""

from dista import Runtime, Task
from dista.flow import Flow, Map


def add_one(x: Int) -> Int:
    return x + 1


comptime AddOne = Task[Int, Int, add_one]


def main() raises:
    var rt = Runtime[AddOne](workers=1)
    var inputs: List[Int] = [3]
    print(len(Flow[Map[AddOne], Map[AddOne]].run(rt, inputs^)))
