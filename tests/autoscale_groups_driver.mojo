"""The driver and worker of tests/autoscale.sh's two-group scenario. It
submits 60 tasks of group `b`, half a second each, at once, then runs 16
tasks of group `a` one at a time, so `a` never wants more than its one
worker while `b`'s burst grows `b`. Every result must come back exact.
"""

from std.time import sleep

from dista import ObjectRef, Runtime, Task

comptime NB = 60
comptime NA = 16


def slow_a(i: Int) -> Int:
    sleep(0.5)
    return i * i


def slow_b(i: Int) -> Int:
    sleep(0.5)
    return i * i + 1


comptime SlowA = Task[Int, Int, slow_a, group="a"]
comptime SlowB = Task[Int, Int, slow_b, group="b"]


def main() raises:
    var rt = Runtime[SlowA, SlowB](workers=0)
    var refs = List[ObjectRef[Int]](capacity=NB)
    for i in range(NB):
        refs.append(rt.remote[SlowB](i))
    var bad = 0
    for i in range(NA):
        if rt.get(rt.remote[SlowA](i)) != i * i:
            print("task a", i, "came back wrong")
            bad += 1
    for i in range(NB):
        if rt.get(refs[i]) != i * i + 1:
            print("task b", i, "came back wrong")
            bad += 1
    if bad > 0:
        raise Error(bad, " results wrong")
    print("PASS autoscale groups driver:", NA + NB, "results exact")
