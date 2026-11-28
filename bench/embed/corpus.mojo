"""The embedding benchmark's corpus: texts shaped like the census's
repository descriptions, made from a fixed seed, in Parquet files.

A text is what the census's `repo_input` builds from a repository: its name
with `-` and `_` as spaces, its description, and its topics, as in
"mojo sqlite: SQLite bindings for Mojo. Topics: mojo, sqlite". Text `i`
depends only on the seed and `i`, so a corpus of any size or file layout
holds the same texts in the same order, and its row `i` has id `i`.

Each file holds `rows` texts (the last one fewer) in two columns, `id`
(int64) and `text` (utf8), in row groups of 32 rows, the inference batch.
"""

from std.os import listdir, makedirs, remove
from std.os.path import exists

from arrow_mlake import RecordBatch
from arrow_mlake.arrow import AT_INT64, AT_UTF8, ArrayData, ArrowType
from parquet import ParquetWriter, WriterOptions

comptime SEED: UInt64 = 20261127
"""The corpus's seed."""
comptime VERSION = 2
"""The generator's version, in each corpus's manifest: a change to the
texts changes it, so an older corpus is written again."""
comptime BATCH = 32
"""Texts per inference batch, and rows per row group of a corpus file."""


def _mix(x: UInt64) -> UInt64:
    """splitmix64's finaliser."""
    var z = x + 0x9E3779B97F4A7C15
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
    z = (z ^ (z >> 27)) * 0x94D049BB133111EB
    return z ^ (z >> 31)


struct _Rng:
    var state: UInt64

    def __init__(out self, seed: UInt64):
        self.state = seed

    def next(mut self) -> UInt64:
        self.state = _mix(self.state)
        return self.state

    def below(mut self, n: Int) -> Int:
        return Int(self.next() % UInt64(n))


def _pick(mut rng: _Rng, words: List[String]) -> String:
    return words[rng.below(len(words))].copy()


def corpus_text(i: Int) -> String:
    """Text `i` of the corpus."""
    var names: List[String] = [
        "mojo",
        "fast",
        "json",
        "http",
        "tensor",
        "sqlite",
        "arrow",
        "vision",
        "bench",
        "kit",
        "lab",
        "core",
        "nn",
        "audio",
        "graph",
        "parser",
        "lite",
        "db",
        "ml",
        "simd",
        "llama",
        "max",
        "async",
        "regex",
    ]
    var adjectives: List[String] = [
        "A fast",
        "A simple",
        "An experimental",
        "A pure Mojo",
        "A minimal",
        "A GPU-accelerated",
        "A zero-copy",
        "A type-safe",
        "A small",
        "A high-performance",
        "A work-in-progress",
        "An idiomatic",
    ]
    var subjects: List[String] = [
        "inference engine",
        "SQLite binding",
        "package manager",
        "web server",
        "game engine",
        "data frame library",
        "tokenizer",
        "HTTP client",
        "linear algebra kernel collection",
        "image processing toolkit",
        "audio DSP library",
        "CLI parser",
        "testing framework",
        "async runtime",
        "JSON parser",
        "compression codec",
        "Parquet reader",
        "vector database",
        "ray tracer",
        "chess engine",
    ]
    var clauses: List[String] = [
        "with examples and benchmarks",
        "built on MAX",
        "supporting Linux and macOS",
        "written from scratch",
        "with Python interop",
        "using SIMD everywhere",
        "for GPUs",
        "with no dependencies",
        "ported from Rust",
        "following the spec",
        "tested against the reference implementation",
        "for learning Mojo",
    ]
    var topics: List[String] = [
        "mojo",
        "machine-learning",
        "simd",
        "gpu",
        "parser",
        "database",
        "web",
        "cli",
        "inference",
        "bindings",
        "data",
        "compression",
        "performance",
        "llm",
        "graphics",
        "networking",
    ]
    var rng = _Rng(SEED ^ _mix(UInt64(i)))
    var out = _pick(rng, names)
    for _ in range(rng.below(3)):
        out += " "
        out += _pick(rng, names)
    out += ": "
    out += _pick(rng, adjectives)
    out += " "
    out += _pick(rng, subjects)
    out += " for Mojo"
    var n_clauses = rng.below(3)
    for k in range(n_clauses):
        out += ", " if k > 0 else " "
        out += _pick(rng, clauses)
    out += "."
    var n_topics = 1 + rng.below(3)
    out += " Topics: "
    for k in range(n_topics):
        if k > 0:
            out += ", "
        out += _pick(rng, topics)
    return out^


def _put_i64(mut buf: List[UInt8], v: Int64):
    var u = UInt64(v)
    for k in range(8):
        buf.append(UInt8((u >> UInt64(8 * k)) & 0xFF))


def corpus_batch(start: Int, end: Int) raises -> RecordBatch:
    """Rows `start` to `end - 1` as a batch of `id` and `text`."""
    var ids = ArrayData(ArrowType(AT_INT64), String("id"))
    ids.nullable = False
    var texts = ArrayData(ArrowType(AT_UTF8), String("text"))
    texts.nullable = False
    texts.offsets.append(0)
    for i in range(start, end):
        _put_i64(ids.values, Int64(i))
        texts.values.extend(corpus_text(i).as_bytes())
        texts.offsets.append(Int32(len(texts.values)))
    ids.length = end - start
    texts.length = end - start
    var b = RecordBatch()
    b.roots.append(b.arena.add(ids^))
    b.roots.append(b.arena.add(texts^))
    b.num_rows = end - start
    return b^


def write_file(path: String, batch: RecordBatch, row_group: Int) raises:
    """Writes `batch` to `path` in row groups of `row_group` rows."""
    var opts = WriterOptions()
    opts.row_group_size = row_group
    opts.use_dictionary = False
    var w = ParquetWriter(opts^)
    w.write_batch(batch.arena, batch.roots)
    var bytes = w^.finish()
    with open(path, "w") as f:
        f.write_bytes(Span(bytes))


def file_name(k: Int) -> String:
    var digits = String(k)
    while digits.byte_length() < 5:
        digits = String("0", digits)
    return String("part-", digits, ".parquet")


def _manifest(texts: Int, rows: Int) -> String:
    return String(
        "version ",
        VERSION,
        " seed ",
        SEED,
        " texts ",
        texts,
        " rows ",
        rows,
        "\n",
    )


def corpus_files(dir: String) raises -> List[String]:
    """The corpus files in `dir`, as paths, in order."""
    var names = List[String]()
    for name in listdir(dir):
        if name.startswith("part-") and name.endswith(".parquet"):
            names.append(name)
    sort(names)
    var out = List[String]()
    for name in names:
        out.append(String(dir, "/", name))
    return out^


def make_corpus(dir: String, texts: Int, rows: Int) raises -> Bool:
    """Writes a corpus of `texts` texts in files of `rows` to `dir`, unless
    it holds that corpus already; whether it wrote one."""
    if texts < 1 or rows < 1:
        raise Error("a corpus needs at least one text and one row per file")
    var manifest = String(dir, "/manifest.txt")
    if exists(manifest):
        with open(manifest, "r") as f:
            if f.read() == _manifest(texts, rows):
                return False
    if not exists(dir):
        makedirs(dir)
    for name in listdir(dir):
        if name.startswith("part-") or name == "manifest.txt":
            remove(String(dir, "/", name))
    var k = 0
    var start = 0
    while start < texts:
        var end = min(start + rows, texts)
        write_file(
            String(dir, "/", file_name(k)), corpus_batch(start, end), BATCH
        )
        start = end
        k += 1
    with open(manifest, "w") as f:
        f.write(_manifest(texts, rows))
    return True
