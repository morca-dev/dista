"""Fuzz: the structured payloads a worker parses from the head, task-inputs
and locations, and the groups frame the head parses from a driver.

    pixi run -e fuzz fuzz-payloads
"""

from std.os import getenv

from mozz import FuzzConfig, fuzz

from dista.codec import put_scalar
from dista.frame import encode_groups, parse_groups
from dista.store import Location
from dista.worker import parse_task_inputs


def target(data: List[UInt8]) raises:
    with open("build/fuzz-payloads-last.bin", "w") as f:
        f.write_bytes(Span(data))
    if len(data) == 0:
        return
    var body = Span(data)[1:]
    try:
        if data[0] % 3 == 2:
            var table = parse_groups(body)
            for entry in table.items():
                var n = entry.value.byte_length()
                if n < 1 or n > 32:
                    raise Error("assertion failed: a bad group name passed")
        elif data[0] % 3 == 0:
            var spec = parse_task_inputs(body)
            for ref inp in spec.inputs:
                if inp.inline == Bool(inp.location):
                    raise Error("assertion failed: an input is both or neither")
        else:
            var pos = 0
            while pos < len(body):
                _ = Location.decode(body, pos)
                if pos > len(body):
                    raise Error("assertion failed: location ran past its input")
    except e:
        if "assertion failed" in String(e):
            raise e^


def main() raises:
    var replay = getenv("FUZZ_REPLAY")
    if replay != "":
        # Run one saved input, e.g. build/fuzz-<name>-last.bin after an abort.
        var data: List[UInt8]
        with open(replay, "r") as f:
            data = f.read_bytes()
        target(data)
        print("replayed", replay, "without a crash")
        return
    var seeds = List[List[UInt8]]()
    var loc = Location([127, 0, 0, 1], 7000, "node", "/dev/shm/x", 12345)
    var spec: List[UInt8] = [0, 1]  # selector, gather
    put_scalar[DType.uint64](spec, 2)
    spec.append(0)
    put_scalar[DType.uint64](spec, 5)
    put_scalar[DType.uint64](spec, 3)
    spec.append(1)
    spec.append(2)
    spec.append(3)
    spec.append(1)
    put_scalar[DType.uint64](spec, 6)
    spec.extend(loc.encode())
    seeds.append(spec^)
    # A stream's: the STREAM flag, its window, one input inline.
    var groups: List[UInt8] = [2]
    groups.extend(encode_groups([UInt64(7), UInt64(9)], ["gpu", "io-2"]))
    seeds.append(groups^)
    var stream: List[UInt8] = [0, 8]
    put_scalar[DType.uint64](stream, 16)
    put_scalar[DType.uint64](stream, 1)
    stream.append(0)
    put_scalar[DType.uint64](stream, 5)
    put_scalar[DType.uint64](stream, 1)
    stream.append(7)
    seeds.append(stream^)
    var locs: List[UInt8] = [1]
    locs.extend(loc.encode())
    locs.extend(loc.encode())
    seeds.append(locs^)
    fuzz(
        target,
        FuzzConfig(
            max_runs=Int(getenv("FUZZ_RUNS", "100000")),
            seed=0,
            crash_dir="build/fuzz-crashes/payloads",
            max_input_len=4096,
        ),
        seeds,
    )
