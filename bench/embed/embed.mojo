"""The embedding benchmark as a flow (bench/embed/README.md).

    ./build/bench-embed corpus     # write the corpus
    ./build/bench-embed run        # the corpus if missing, the flow, the check
    ./build/bench-embed encode     # the encoder alone, in this process
    ./build/bench-embed check --out=DIR   # any output directory, such as Ray's

The flow reads one corpus file per input and emits one batch of 32 texts per
item, embeds each batch in a `StatefulTask` of the group `embed` that holds
the model, and writes each batch's vectors to a Parquet file of its own:

    Read[TaskStream[String, Texts, scan_file]]            a file -> its batches
    MapWith[StatefulTask[EncoderState, Texts, Vectors,
                         load_encoder, encode_batch,
                         group="embed"]]                  a batch -> its vectors
    Map[Task[Vectors, Written, write_batch]]              vectors -> a file

`Texts` and `Vectors` are structs that dista's codec encodes field by field;
their lists are `Seq`s. A `Written` is the file's name and its row count.
An output file is named by its input file and batch index, so a rerun of an
input overwrites what its first attempt wrote, and the check reads only the
files the flow's results name.

Each run checks itself: one process embeds the same files in the same
batches with the same encoder, and every row's id must come back exactly
once, its vector within `TOLERANCE` of that process's. The `check` mode
does the same for any output directory of this layout, such as the one
`embed_ray.py` writes, matching rows by id.

The head and the workers are new processes of this binary (`Cluster`), not
the local mode's forks, in which the encoder's `parallelize` can't run.

Settings come from flags or the environment: `--texts` or EMBED_TEXTS,
`--rows` or EMBED_ROWS (texts per corpus file), `--corpus`, `--out`,
`--model`, `--stub=1` (the stub encoder), `--workers` (K, workers in no
group: the reads and the writes), `--embed-workers` or EMBED_GROUP_WORKERS
(E, workers in group `embed`), `--check=0` (no check) and `--corrupt=1`
(one wrong value, which the check must catch); each flag's variable is its
name in capitals after EMBED_. EMBED_PIN (Linux only) pins each embed worker
to a set of CPUs, such as `0-3/4-7` for E=2 (`parse_pin`).
"""

from std.ffi import c_int, external_call
from std.memory import bitcast
from std.math import sqrt
from std.os import getenv, listdir, makedirs, remove, setenv, unsetenv
from std.os.path import exists, isdir
from std.sys import argv, size_of
from std.sys.info import CompilationTarget
from std.time import perf_counter_ns, sleep

from arrow_mlake import RecordBatch
from arrow_mlake.arrow import AT_BINARY, AT_INT64, ArrayData, ArrowType
from encoder import Encoder
from parquet import ParquetReader

from dista import Emit, Runtime, Seq, StatefulTask, Task, TaskStream
from dista.flow import Flow, FlowStats, Map, MapWith, Read

from corpus import BATCH, corpus_files, make_corpus, write_file

comptime DIM = 384
"""The vector size of bge-small-en-v1.5, and of the stub."""
comptime TOLERANCE: Float32 = 1e-5
"""The largest difference allowed between a value of the flow's vectors and
the single process's. The vectors have unit length."""
comptime STUB_TOLERANCE: Float32 = 1e-6
"""`check`'s default with the stub, whose vectors embed_ray.py computes the
same way, to the bit."""
comptime CROSS_TOLERANCE: Float32 = 1e-5
"""`check`'s default with the model: the vectors may come from another
kernel, such as PyTorch's, whose largest difference from encoder.mojo's on
20,000 texts was 5.1e-7 (README, "Ray Data")."""


# ---------------------------------------------------------------- settings


def _flag(name: String) -> String:
    """`--name=value` from the command line, or "" if absent."""
    var args = argv()
    var prefix = String("--", name, "=")
    for i in range(len(args)):
        var a = String(args[i])
        if a.startswith(prefix):
            return String(a[byte = prefix.byte_length() :])
    return String()


def setting(env: String, flag: String, default: String) -> String:
    """A setting from its flag, else its environment variable, else
    `default`. A flag is also put in the environment, so that workers,
    forked later, see it."""
    var f = _flag(flag)
    if f.byte_length() > 0:
        _ = setenv(env, f)
        return f
    var e = getenv(env)
    if e.byte_length() > 0:
        return e
    _ = setenv(env, default)
    return default


@fieldwise_init
struct Settings(Copyable, Movable):
    var texts: Int
    var rows: Int
    var corpus: String
    var out: String
    var model: String
    var stub: Bool
    var workers: Int
    var group_workers: Int
    var check: Bool
    var corrupt: Bool


def settings() raises -> Settings:
    var stub = setting("EMBED_STUB", "stub", "0") == "1"
    return Settings(
        Int(setting("EMBED_TEXTS", "texts", "4000")),
        Int(setting("EMBED_ROWS", "rows", "512")),
        setting("EMBED_CORPUS", "corpus", "build/embed/corpus"),
        setting("EMBED_OUT", "out", "build/embed/out"),
        setting("EMBED_MODEL", "model", "build/embed/models/bge-small-en-v1.5"),
        stub,
        Int(setting("EMBED_WORKERS", "workers", "2")),
        Int(setting("EMBED_GROUP_WORKERS", "embed-workers", "1")),
        setting("EMBED_CHECK", "check", "1") != "0",
        setting("EMBED_CORRUPT", "corrupt", "0") == "1",
    )


# ---------------------------------------------------------------- the types


@fieldwise_init
struct Texts(Copyable, Movable):
    """One batch of a corpus file: its texts and their ids."""

    var file: String
    """The corpus file's name without `.parquet`."""
    var batch: Int
    """The batch's index in its file."""
    var ids: Seq[Int64]
    var texts: Seq[String]


@fieldwise_init
struct Vectors(Copyable, Movable):
    """A batch's vectors, row `i` at `values[i * dim : (i + 1) * dim]`."""

    var file: String
    var batch: Int
    var ids: Seq[Int64]
    var dim: Int
    var values: Seq[Float32]


@fieldwise_init
struct Written(Copyable, Movable):
    """An output file the flow wrote, and its rows."""

    var name: String
    var rows: Int


# ---------------------------------------------------------------- the encoders


def fnv64(data: Span[UInt8, _]) -> UInt64:
    var h: UInt64 = 0xCBF29CE484222325
    for b in data:
        h ^= UInt64(b)
        h *= 0x100000001B3
    return h


def stub_vectors(texts: List[String]) -> List[Float32]:
    """The stub encoder: per text, `DIM` values from an xorshift seeded by
    its FNV-1a hash, scaled to unit length."""
    var out = List[Float32](capacity=len(texts) * DIM)
    for t in texts:
        var x = fnv64(t.as_bytes()) | 1
        var start = len(out)
        var norm: Float32 = 0
        for _ in range(DIM):
            x ^= x << 13
            x ^= x >> 7
            x ^= x << 17
            var v = Float32(Int(x >> 40)) / Float32(1 << 23) - 1.0
            out.append(v)
            norm += v * v
        var scale = 1.0 / sqrt(norm)
        for k in range(start, start + DIM):
            out[k] *= scale
    return out^


struct EncoderState(Movable):
    """What an `embed` worker keeps: the model, or nothing for the stub."""

    var encoder: Optional[Encoder]

    def __init__(out self, var encoder: Optional[Encoder]):
        self.encoder = encoder^

    def encode(self, texts: List[String]) raises -> List[Float32]:
        if self.encoder:
            return self.encoder.value().encode(texts, BATCH)
        return stub_vectors(texts)


def _pid() -> Int:
    return Int(external_call["getpid", c_int]())


def open_encoder() raises -> EncoderState:
    """The stub if EMBED_STUB is 1, else the model in EMBED_MODEL."""
    if getenv("EMBED_STUB") == "1":
        return EncoderState(None)
    var dir = getenv("EMBED_MODEL")
    if not exists(String(dir, "/model.safetensors")):
        raise Error(
            "no model in ",
            dir,
            ": run `pixi run -e embed bench-embed-model`, or set EMBED_STUB=1",
        )
    return EncoderState(Encoder(dir))


def load_encoder() raises -> EncoderState:
    """The `StatefulTask`'s `new`: the encoder, and a file
    `loads/load-<pid>-<n>` in EMBED_OUT holding this worker's group and the
    seconds the load took, so the driver can tell which processes loaded
    one."""
    var t0 = perf_counter_ns()
    var state = open_encoder()
    var secs = _seconds(t0)
    var dir = String(getenv("EMBED_OUT"), "/loads")
    var n = 0
    while exists(String(dir, "/load-", _pid(), "-", n)):
        n += 1
    with open(String(dir, "/load-", _pid(), "-", n), "w") as f:
        f.write(getenv("DISTA_GROUP"), " ", secs)
    return state^


# ---------------------------------------------------------------- the tasks


def _stem(path: String) -> String:
    var name = String(path)
    var slash = path.rfind("/")
    if slash:
        name = String(path[byte = slash.value() + 1 :])
    if name.endswith(".parquet"):
        var stem = String(name[byte = : name.byte_length() - 8])
        name = stem^
    return name^


def read_batches(path: String) raises -> List[Texts]:
    """A corpus file's batches of `BATCH` texts, as the read emits them."""
    var out = List[Texts]()
    var r = ParquetReader.open(path)
    r.batch_size = BATCH
    var stem = _stem(path)
    var k = 0
    while r.has_next():
        var b = r.read_batch()
        out.append(
            Texts(
                stem,
                k,
                Seq(b.column_i64(0)[0].copy()),
                Seq(b.column_str(1)[0].copy()),
            )
        )
        k += 1
    return out^


def scan_file(path: String, mut out: Emit[Texts]) raises:
    """The read: one corpus file, a batch of `BATCH` texts at a time."""
    var r = ParquetReader.open(path)
    r.batch_size = BATCH
    var stem = _stem(path)
    var k = 0
    while r.has_next():
        var b = r.read_batch()
        out.put(
            Texts(
                stem,
                k,
                Seq(b.column_i64(0)[0].copy()),
                Seq(b.column_str(1)[0].copy()),
            )
        )
        k += 1


def encode_batch(mut state: EncoderState, batch: Texts) raises -> Vectors:
    """The embedding: one batch's texts through the encoder."""
    var v = state.encode(batch.texts.items)
    return Vectors(batch.file, batch.batch, batch.ids.copy(), DIM, Seq(v^))


def output_name(file: String, batch: Int) -> String:
    var digits = String(batch)
    while digits.byte_length() < 5:
        digits = String("0", digits)
    return String(file, "-b", digits, ".parquet")


def _put_le[dt: DType](mut buf: List[UInt8], v: Scalar[dt]):
    comptime n = size_of[Scalar[dt]]()
    var b = bitcast[DType.uint8, n](v)
    comptime for i in range(n):
        buf.append(b[i])


def write_batch(v: Vectors) raises -> Written:
    """The sink: a batch's ids and vectors to
    `EMBED_OUT/vectors/<file>-b<batch>.parquet`, written under a temporary
    name and moved into place. `id` is int64 and `vector` binary, `dim`
    little-endian float32 values. With EMBED_CORRUPT=1, batch 0 of
    `part-00000` gets one wrong value."""
    var n = len(v.ids)
    var ids = ArrayData(ArrowType(AT_INT64), String("id"))
    ids.nullable = False
    var vec = ArrayData(ArrowType(AT_BINARY), String("vector"))
    vec.nullable = False
    vec.offsets.append(0)
    var corrupt = (
        getenv("EMBED_CORRUPT") == "1"
        and v.file == "part-00000"
        and v.batch == 0
    )
    for i in range(n):
        _put_le[DType.int64](ids.values, v.ids.items[i])
        for k in range(i * v.dim, (i + 1) * v.dim):
            var x = v.values.items[k]
            if corrupt and k == 7:
                x += 0.01
            _put_le[DType.float32](vec.values, x)
        vec.offsets.append(Int32(len(vec.values)))
    ids.length = n
    vec.length = n
    var b = RecordBatch()
    b.roots.append(b.arena.add(ids^))
    b.roots.append(b.arena.add(vec^))
    b.num_rows = n
    var name = output_name(v.file, v.batch)
    var final = String(getenv("EMBED_OUT"), "/vectors/", name)
    var tmp = String(final, ".tmp-", _pid())
    write_file(tmp, b, BATCH)
    if (
        external_call["rename", c_int](
            tmp.as_c_string_span(), final.as_c_string_span()
        )
        != 0
    ):
        raise Error("could not move ", tmp, " into place")
    return Written(name^, n)


comptime ScanFile = TaskStream[String, Texts, scan_file]
comptime Embed = StatefulTask[
    EncoderState, Texts, Vectors, load_encoder, encode_batch, group="embed"
]
comptime WriteBatch = Task[Vectors, Written, write_batch]
comptime EmbedFlow = Flow[Read[ScanFile], MapWith[Embed], Map[WriteBatch]]


# ---------------------------------------------------------------- measuring


def _rusage_peak_mb(who: Int) -> Float64:
    """`getrusage`'s `ru_maxrss` in MB for `who` (0 this process, -1 its
    waited-for children: the largest of them), which follows two timevals
    and is KiB on Linux, bytes on macOS."""
    var usage = List[Int64](length=18, fill=0)
    _ = external_call["getrusage", c_int](
        c_int(who), usage.unsafe_ptr().unsafe_bitcast[NoneType]()
    )
    var peak = Float64(usage[4])
    comptime if CompilationTarget.is_linux():
        peak *= 1024
    return peak / 1e6


def _seconds(t0: Int) -> Float64:
    return Float64(perf_counter_ns() - t0) / 1e9


def _r(x: Float64, digits: Int) -> Float64:
    """`x` rounded to `digits` decimals, for printing."""
    var scale = 10.0**digits
    return Float64(Int(x * scale + 0.5)) / scale


@fieldwise_init
struct Proc(Copyable, Movable):
    """A process of the run, and its peak resident set."""

    var role: String
    var pid: Int
    var peak_mb: Float64


def _clear(dir: String) raises:
    if not exists(dir):
        makedirs(dir)
    for name in listdir(dir):
        remove(String(dir, "/", name))


@fieldwise_init
struct HeadPeaks(Copyable, Movable):
    """The largest of the head's counts (`Runtime.stats`), asked about once
    a second while the flow ran, and how often it was asked."""

    var objects: Int
    """Objects with a live reference."""
    var results: Int
    """Small results kept at the head."""
    var located: Int
    """Results the head knows live on a worker."""
    var pending: Int
    """Tasks queued or waiting on inputs."""
    var samples: Int


@fieldwise_init
struct FlowOutput(Movable):
    var written: List[Written]
    var stats: FlowStats
    var seconds: Float64
    var procs: List[Proc]
    """The head's and the workers' peaks, when `Cluster` started them."""
    var head: HeadPeaks


def parse_pin(spec: String) raises -> List[List[Int]]:
    """EMBED_PIN's CPU sets, one per embed worker, separated by `/`; each is
    CPUs and ranges separated by commas, such as `0-3/4-7` or
    `0,1,8,9/2,3,10,11`. An empty `spec` pins nothing."""
    var out = List[List[Int]]()
    if spec.byte_length() == 0:
        return out^
    for part in spec.split("/"):
        var cpus = List[Int]()
        for item in String(part).split(","):
            var r = String(item).split("-")
            var lo = Int(String(r[0]))
            var hi = Int(String(r[len(r) - 1]))
            if lo < 0 or hi < lo or hi >= 1024:
                raise Error("EMBED_PIN: bad CPU range ", item)
            for c in range(lo, hi + 1):
                cpus.append(c)
        out.append(cpus^)
    return out^


def _spawn(log: String, cpus: List[Int] = List[Int]()) raises -> Int:
    """Starts this binary again, as a new process with this one's
    environment, its output to `log`; its pid. With `cpus` (Linux only) the
    child is pinned to them with `sched_setaffinity` before `exec`, so the
    Mojo runtime sizes its CPU pool to them when the new process starts.
    The child runs only async-signal-safe calls between `fork` and `exec`,
    so it doesn't matter that this process has threads."""
    # A cpu_set_t of 1,024 CPUs, as glibc's.
    var mask = List[UInt64](length=16, fill=0)
    for c in cpus:
        mask[c // 64] |= UInt64(1) << UInt64(c % 64)
    var prog = String(argv()[0])
    var cprog = prog.as_c_string_span()
    var path = log.copy()
    var clog = path.as_c_string_span()
    var args = List[Int](length=2, fill=0)
    args[0] = Int(prog.unsafe_ptr())
    var pid = Int(external_call["fork", c_int]())
    if pid < 0:
        raise Error("fork failed")
    if pid == 0:
        # O_WRONLY | O_CREAT | O_TRUNC, which differ between the two.
        var flags = 0x601 if CompilationTarget.is_macos() else 0x241
        var fd = external_call["open", c_int, num_fixed_args=2](
            clog, c_int(flags), c_int(0o644)
        )
        _ = external_call["dup2", c_int](fd, c_int(1))
        _ = external_call["dup2", c_int](fd, c_int(2))
        comptime if CompilationTarget.is_linux():
            if len(cpus) > 0:
                _ = external_call["sched_setaffinity", c_int](
                    c_int(0), Int(128), mask.unsafe_ptr()
                )
        _ = external_call["execv", c_int](cprog, args.unsafe_ptr())
        external_call["_exit", NoneType](c_int(127))
    return pid


def _head_port(log: String) raises -> Int:
    """The port in the head's `dista head listening on port N` line."""
    for _ in range(300):
        if exists(log):
            with open(log, "r") as f:
                var text = f.read()
                var at = text.find("listening on port ")
                if at:
                    var rest = String(text[byte = at.value() + 18 :])
                    var line = String(rest.split("\n")[0])
                    if rest.find("\n"):
                        return Int(line)
        sleep(0.01)
    raise Error("the head never said its port; see ", log)


struct Cluster(Movable):
    """A head and K + E workers on this machine, each a new process of this
    binary, as in a cluster. A worker forked from a process the Mojo runtime
    has started in can't run `parallelize`, which the encoder uses (README,
    "Local mode and the encoder"), so the local mode's forks won't do."""

    var pids: List[Int]
    var roles: List[String]

    def __init__(out self):
        self.pids = List[Int]()
        self.roles = List[String]()

    def start(mut self, s: Settings) raises:
        """Starts the head, then the workers. Each process finds its role in
        the environment, as `main` reads it first. EMBED_CHILD marks them,
        so that one that somehow has no role stops instead of starting a
        cluster of its own."""
        _ = setenv("EMBED_CHILD", "1")
        var logs = String(s.out, "/logs")
        _clear(logs)
        _ = setenv("DISTA_ROLE", "head")
        _ = setenv("DISTA_PORT", "0")
        var head_log = String(logs, "/head.log")
        self.pids.append(_spawn(head_log))
        self.roles.append("head")
        var head = String("127.0.0.1:", _head_port(head_log))
        var pin = parse_pin(getenv("EMBED_PIN"))
        if len(pin) > 0:
            comptime if not CompilationTarget.is_linux():
                raise Error("EMBED_PIN: CPU affinity is set on Linux only")
            if len(pin) != s.group_workers:
                raise Error(
                    "EMBED_PIN names ",
                    len(pin),
                    " CPU sets, for E=",
                    s.group_workers,
                    " embed workers",
                )
        _ = setenv("DISTA_ROLE", "worker")
        _ = setenv("DISTA_HEAD", head)
        for k in range(s.workers + s.group_workers):
            var group = String("embed") if k >= s.workers else String()
            _ = setenv("DISTA_GROUP", group)
            var cpus = List[Int]()
            if k >= s.workers and len(pin) > 0:
                cpus = pin[k - s.workers].copy()
            self.pids.append(_spawn(String(logs, "/worker-", k, ".log"), cpus))
            self.roles.append(
                String("worker in group embed") if k
                >= s.workers else String("worker in no group")
            )
        _ = unsetenv("DISTA_ROLE")
        _ = unsetenv("DISTA_GROUP")
        _ = unsetenv("DISTA_PORT")

    def stop(mut self) -> List[Proc]:
        """Kills every process and waits for it with `wait4`, which gives
        each one's peak resident set (`ru_maxrss`, as `_rusage_peak_mb`
        reads it)."""
        for pid in self.pids:
            _ = external_call["kill", c_int](c_int(pid), c_int(9))
        var status = List[c_int](length=1, fill=0)
        var usage = List[Int64](length=18, fill=0)
        var out = List[Proc]()
        for k in range(len(self.pids)):
            usage[4] = 0
            _ = external_call["wait4", c_int](
                c_int(self.pids[k]),
                status.unsafe_ptr(),
                c_int(0),
                usage.unsafe_ptr().unsafe_bitcast[NoneType](),
            )
            var peak = Float64(usage[4])
            comptime if CompilationTarget.is_linux():
                peak *= 1024
            out.append(Proc(self.roles[k], self.pids[k], peak / 1e6))
        self.pids.clear()
        self.roles.clear()
        return out^


def _wait_for_workers(
    mut rt: Runtime[ScanFile, Embed, WriteBatch], s: Settings
) raises:
    for _ in range(3000):
        var plain = rt.client().group_slots("")
        var embed = rt.client().group_slots("embed")
        if not plain or not embed:
            raise Error("the head is older than group_slots")
        if (
            plain.value().slots >= s.workers
            and embed.value().slots >= s.group_workers
        ):
            return
        sleep(0.01)
    raise Error("the workers never joined; see ", s.out, "/logs")


def run_flow(s: Settings, var files: List[String]) raises -> FlowOutput:
    """Runs the flow over `files`, on processes of its own (`Cluster`)
    unless EMBED_LAUNCH is `local`, which forks them with
    `Runtime(workers=K, groups="embed=E")` and works with the stub only.
    Every process has exited when this returns."""
    if getenv("EMBED_LAUNCH") == "local":
        var rt = Runtime[ScanFile, Embed, WriteBatch](
            workers=s.workers, groups=String("embed=", s.group_workers)
        )
        return _drive(rt, files^)
    var cluster = Cluster()
    try:
        cluster.start(s)
        var rt = Runtime[ScanFile, Embed, WriteBatch]()
        _wait_for_workers(rt, s)
        var out = _drive(rt, files^)
        # The runtime is closed after its last use, above.
        out.procs = cluster.stop()
        return out^
    except e:
        _ = cluster.stop()
        raise e^


def _drive(
    mut rt: Runtime[ScanFile, Embed, WriteBatch], var files: List[String]
) raises -> FlowOutput:
    var t0 = perf_counter_ns()
    var run = EmbedFlow.start(rt, files^)
    var written = List[Written]()
    var head = HeadPeaks(0, 0, 0, 0, 0)
    var asked = t0
    while True:
        var r = run.next(rt)
        if not r:
            break
        var refs = r.take().take_refs()
        for k in range(len(refs)):
            written.append(rt.take(refs[k]))
        if perf_counter_ns() - asked >= 1_000_000_000:
            asked = perf_counter_ns()
            var h = rt.stats()
            head.objects = max(head.objects, h.objects)
            head.results = max(head.results, h.results)
            head.located = max(head.located, h.located)
            head.pending = max(head.pending, h.pending)
            head.samples += 1
    var secs = _seconds(t0)
    return FlowOutput(written^, run.stats(), secs, List[Proc](), head^)


# ---------------------------------------------------------------- the check


def _vector_file(path: String) raises -> Tuple[List[Int64], List[Float32]]:
    """An output file's ids and vectors, flattened."""
    var r = ParquetReader.open(path)
    var t = r.read_table()
    var ids = List[Int64]()
    var values = List[Float32]()
    for b in t.batches:
        ids.extend(b.column_i64(0)[0].copy())
        ref a = b.column(1)
        var bytes = Span(a.values)
        for k in range(0, len(a.values), 4):
            var bits = (
                UInt32(bytes[k])
                | (UInt32(bytes[k + 1]) << 8)
                | (UInt32(bytes[k + 2]) << 16)
                | (UInt32(bytes[k + 3]) << 24)
            )
            values.append(bitcast[DType.float32](bits))
    return (ids^, values^)


def check(s: Settings, files: List[String], written: List[Written]) raises:
    """Embeds `files` in this process, batch by batch as the flow does, and
    compares with the files the flow's results name. Raises on the first
    difference."""
    var t0 = perf_counter_ns()
    var named = Dict[String, Int]()
    for w in written:
        if w.name in named:
            raise Error("check: ", w.name, " is named twice in the results")
        named[w.name] = w.rows
    var state = open_encoder()
    var seen = Dict[Int, Bool]()
    var batches = 0
    var worst: Float32 = 0
    for path in files:
        for t in read_batches(path):
            var name = output_name(t.file, t.batch)
            if name not in named:
                raise Error("check: no result names ", name)
            var got = _vector_file(String(s.out, "/vectors/", name))
            if named[name] != len(t.ids) or len(got[0]) != len(t.ids):
                raise Error(
                    "check: ",
                    name,
                    " has ",
                    len(got[0]),
                    " rows (result says ",
                    named[name],
                    "), its batch ",
                    len(t.ids),
                )
            var want = state.encode(t.texts.items)
            if len(got[1]) != len(want):
                raise Error("check: ", name, " has vectors of another size")
            for i in range(len(t.ids)):
                var id = t.ids.items[i]
                if got[0][i] != id:
                    raise Error(
                        "check: ",
                        name,
                        " row ",
                        i,
                        " has id ",
                        got[0][i],
                        ", wanted ",
                        id,
                    )
                if Int(id) in seen:
                    raise Error("check: id ", id, " came back twice")
                seen[Int(id)] = True
                for k in range(i * DIM, (i + 1) * DIM):
                    var d = abs(got[1][k] - want[k])
                    worst = max(worst, d)
                    if d > TOLERANCE:
                        raise Error(
                            "check: id ",
                            id,
                            " value ",
                            k - i * DIM,
                            " is ",
                            got[1][k],
                            ", one process makes ",
                            want[k],
                            " (difference ",
                            d,
                            ", tolerance ",
                            TOLERANCE,
                            ")",
                        )
            batches += 1
    if batches != len(named):
        raise Error(
            "check: the results name ",
            len(named),
            " files, the corpus has ",
            batches,
            " batches",
        )
    if len(seen) != s.texts:
        raise Error("check: ", len(seen), " ids, wanted ", s.texts)
    print(
        t"check: passed, {len(seen)} ids once each in {batches} files,"
        t" largest difference {worst} (tolerance {TOLERANCE}),"
        t" {_r(_seconds(t0), 2)} s"
    )


def _parquet_under(dir: String, mut out: List[String]) raises:
    """Every `*.parquet` file under `dir`, at any depth, in name order."""
    var names = List[String]()
    for name in listdir(dir):
        names.append(name)
    sort(names)
    for name in names:
        var path = String(dir, "/", name)
        if isdir(path):
            _parquet_under(path, out)
        elif name.endswith(".parquet") and not name.startswith("."):
            out.append(path^)


def _column(b: RecordBatch, name: String) raises -> Int:
    for i in range(b.num_columns()):
        if b.name(i) == name:
            return i
    raise Error("no column ", name)


def _output_file(path: String) raises -> Tuple[List[Int64], List[Float32]]:
    """An output file's `id` and `vector` columns, found by name, the
    vectors flattened. Any writer's file will do: each row's offsets are
    followed, and each vector must be `DIM` float32 values."""
    var r = ParquetReader.open(path)
    var t = r.read_table()
    var ids = List[Int64]()
    var values = List[Float32]()
    for b in t.batches:
        ids.extend(b.column_i64(_column(b, "id"))[0].copy())
        ref a = b.column(_column(b, "vector"))
        if a.type.id != AT_BINARY or a.null_count > 0:
            raise Error(path, ": `vector` isn't a binary column without nulls")
        var bytes = Span(a.values)
        for i in range(a.length):
            var start = Int(a.offsets[i])
            if Int(a.offsets[i + 1]) - start != DIM * 4:
                raise Error(
                    path, ": row ", i, " isn't ", DIM, " float32 values"
                )
            for k in range(start, start + DIM * 4, 4):
                var bits = (
                    UInt32(bytes[k])
                    | (UInt32(bytes[k + 1]) << 8)
                    | (UInt32(bytes[k + 2]) << 16)
                    | (UInt32(bytes[k + 3]) << 24)
                )
                values.append(bitcast[DType.float32](bits))
    return (ids^, values^)


def check_mode(s: Settings) raises:
    """Checks any output directory (`--out`) against one process embedding
    the corpus (`--corpus`) a row group at a time, with encoder.mojo or the
    stub: every `*.parquet` file under it, whatever its name, is read, every
    corpus id must appear exactly once and nothing else, and every value
    must be within `--tolerance` of the single process's. The default
    tolerance is `STUB_TOLERANCE` with the stub and `CROSS_TOLERANCE` with
    the model, which allows for another kernel such as PyTorch's. Prints
    the largest difference and the smallest cosine; raises on the first
    value out of tolerance."""
    var tol_flag = setting(
        "EMBED_TOLERANCE",
        "tolerance",
        String(STUB_TOLERANCE) if s.stub else String(CROSS_TOLERANCE),
    )
    var tolerance = Float32(atof(tol_flag))
    var t0 = perf_counter_ns()
    var state = open_encoder()
    var want = List[Float32]()
    var row = Dict[Int, Int]()
    for path in corpus_files(s.corpus):
        for t in read_batches(path):
            for id in t.ids.items:
                if Int(id) in row:
                    raise Error("check: the corpus has id ", id, " twice")
                row[Int(id)] = len(row)
            want.extend(state.encode(t.texts.items))
    var embed_s = _seconds(t0)
    var outputs = List[String]()
    _parquet_under(s.out, outputs)
    if len(outputs) == 0:
        raise Error("check: no .parquet files under ", s.out)
    var seen = List[Bool](length=len(row), fill=False)
    var count = 0
    var worst: Float32 = 0
    var least_cos: Float64 = 1.0
    for path in outputs:
        var got = _output_file(path)
        ref ids = got[0]
        ref values = got[1]
        for i in range(len(ids)):
            var id = Int(ids[i])
            if id not in row:
                raise Error(
                    "check: ", path, " has id ", id, ", not in the corpus"
                )
            var j = row[id]
            if seen[j]:
                raise Error("check: id ", id, " comes back twice")
            seen[j] = True
            count += 1
            var dot: Float64 = 0
            var na: Float64 = 0
            var nb: Float64 = 0
            for k in range(DIM):
                var g = values[i * DIM + k]
                var w = want[j * DIM + k]
                var d = abs(g - w)
                worst = max(worst, d)
                if d > tolerance:
                    raise Error(
                        "check: id ",
                        id,
                        " value ",
                        k,
                        " is ",
                        g,
                        " in ",
                        path,
                        ", one process makes ",
                        w,
                        " (difference ",
                        d,
                        ", tolerance ",
                        tolerance,
                        ")",
                    )
                dot += Float64(g) * Float64(w)
                na += Float64(g) * Float64(g)
                nb += Float64(w) * Float64(w)
            least_cos = min(least_cos, dot / sqrt(na * nb))
    if count != len(row):
        for e in row.items():
            if not seen[e.value]:
                raise Error(
                    "check: ",
                    len(row) - count,
                    " of the corpus's ",
                    len(row),
                    " ids are missing, id ",
                    e.key,
                    " among them",
                )
    print(
        t"check: passed, {count} ids once each in {len(outputs)} files under"
        t" {s.out}, largest difference {worst}, smallest cosine {least_cos}"
        t" (tolerance {tolerance}, {'stub' if s.stub else 'model'});"
        t" {_r(embed_s, 2)} s to embed the corpus, {_r(_seconds(t0), 2)} s"
        t" in all"
    )


# ---------------------------------------------------------------- the modes


def corpus_mode(s: Settings, tokens: Bool) raises:
    """Makes the corpus, and with `tokens` counts its first file's tokens,
    which loads the tokenizer and the model in this process. A run mustn't:
    see `run_mode`."""
    var t0 = perf_counter_ns()
    var made = make_corpus(s.corpus, s.texts, s.rows)
    var files = corpus_files(s.corpus)
    var bytes = 0
    for f in files:
        with open(f, "r") as h:
            bytes += len(h.read_bytes())
    print(
        t"corpus: {s.texts} texts in {len(files)} files of {s.rows} rows,"
        t" {_r(Float64(bytes) / 1e6, 2)} MB in {s.corpus}"
        t" ({'written' if made else 'already there'}, {_r(_seconds(t0), 2)} s)"
    )
    var first = read_batches(files[0])
    print(t"corpus: text 0 is \"{first[0].texts.items[0]}\"")
    var vocab = String(s.model, "/vocab.txt")
    if tokens and exists(vocab):
        var enc = Encoder(s.model)
        var tokens = 0
        var lo = 1 << 30
        var hi = 0
        var n = 0
        for t in read_batches(files[0]):
            for text in t.texts.items:
                var k = len(enc.tokenizer.encode(text))
                tokens += k
                lo = min(lo, k)
                hi = max(hi, k)
                n += 1
        print(
            t"corpus: tokens per text in {files[0]}: {lo} to {hi},"
            t" {_r(Float64(tokens) / Float64(n), 1)} on average"
        )


def run_mode(s: Settings) raises:
    if s.workers < 2 or s.group_workers < 1:
        # With one worker in no group the read's stream would hold its only
        # slot, which the writes need, and a cluster's head waits for
        # another worker rather than fail (DESIGN.md, "Streams that hold
        # every slot").
        raise Error("bench-embed: K must be at least 2 and E at least 1")
    if not s.stub and not exists(String(s.model, "/model.safetensors")):
        raise Error(
            "no model in ",
            s.model,
            ": run `pixi run -e embed bench-embed-model` first, or use",
            " `pixi run -e embed bench-embed-stub`",
        )
    corpus_mode(s, False)
    _clear(String(s.out, "/vectors"))
    _clear(String(s.out, "/loads"))
    var files = corpus_files(s.corpus)
    var out = run_flow(s, files.copy())
    var driver = _rusage_peak_mb(0)
    var children = _rusage_peak_mb(-1)
    var rows = 0
    for w in out.written:
        rows += w.rows
    print(
        t"embed: {rows} texts, {'stub' if s.stub else s.model},"
        t" K={s.workers} workers + E={s.group_workers} in group embed:"
        t" {_r(out.seconds, 2)} s, {_r(Float64(rows) / out.seconds, 1)}"
        t" texts/s (the model's load included)"
    )
    var names: List[String] = ["Read", "MapWith", "Map"]
    for k in range(len(out.stats.ops)):
        print(t"  operator {k} ({names[k]}): {out.stats.ops[k]}")
    print(
        t"  head, largest of {out.head.samples} samples about a second apart:"
        t" {out.head.objects} live objects, {out.head.results} results kept,"
        t" {out.head.located} located on workers, {out.head.pending} tasks"
        t" pending"
    )
    print(
        t"  peak RSS (getrusage, before the check): driver {_r(driver, 1)} MB,"
        t" largest child {_r(children, 1)} MB"
    )
    var total = driver
    for p in out.procs:
        print(t"    {p.role} (pid {p.pid}): {_r(p.peak_mb, 1)} MB")
        total += p.peak_mb
    if len(out.procs) > 0:
        print(
            t"    sum over the driver and its {len(out.procs)} processes:"
            t" {_r(total, 1)} MB"
        )
    var loads = List[String]()
    for name in listdir(String(s.out, "/loads")):
        loads.append(name)
    sort(loads)
    for name in loads:
        with open(String(s.out, "/loads/", name), "r") as f:
            var parts = f.read().split(" ")
            var group = String(parts[0])
            var pid = name.split("-")[1]
            print(
                t"  state loaded: pid {pid}, DISTA_GROUP={group},"
                t" in {_r(Float64(String(parts[1])), 2)} s"
            )
            if group != "embed":
                raise Error("an encoder was loaded outside group embed")
    if s.check:
        check(s, files, out.written)


def encode_mode(s: Settings) raises:
    """The encoder alone in this process over the corpus, for measuring its
    threads (README, "Threads")."""
    corpus_mode(s, False)
    var state = open_encoder()
    var batches = List[Texts]()
    for f in corpus_files(s.corpus):
        batches.extend(read_batches(f))
    _ = state.encode(batches[0].texts.items)  # warm-up
    var t0 = perf_counter_ns()
    var n = 0
    for t in batches:
        _ = state.encode(t.texts.items)
        n += len(t.texts)
    var secs = _seconds(t0)
    print(
        t"encode: {n} texts in one process: {_r(secs, 2)} s,"
        t" {_r(Float64(n) / secs, 1)} texts/s"
    )


def main() raises:
    var role = getenv("DISTA_ROLE")
    if role.byte_length() > 0:
        # A head or a worker that `Cluster` started: it never returns.
        _ = Runtime[ScanFile, Embed, WriteBatch]()
        return
    if getenv("EMBED_CHILD") == "1":
        raise Error("bench-embed: started by a run, but with no DISTA_ROLE")
    var args = argv()
    var mode = String(args[1]) if len(args) > 1 else String("run")
    var s = settings()
    if mode == "corpus":
        corpus_mode(s, True)
    elif mode == "run":
        run_mode(s)
    elif mode == "encode":
        encode_mode(s)
    elif mode == "check":
        check_mode(s)
    else:
        raise Error(
            "bench-embed: the mode is corpus, run, encode or check, not ", mode
        )
