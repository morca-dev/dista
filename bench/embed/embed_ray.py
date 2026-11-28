"""The embedding benchmark on Ray Data (bench/embed/README.md, "Ray Data").

    python bench/embed/embed_ray.py           # run: the job on Ray Data
    python bench/embed/embed_ray.py encode    # the encoder alone, in this process

The same job as `embed.mojo`'s flow, over the same corpus and the same model
files:

    read_parquet(corpus files)
      -> map_batches(Embed, batch_size=32, compute=ActorPoolStrategy(size=E),
                     num_cpus=T)
      -> write_parquet(OUT/vectors)

`Embed` loads bge-small-en-v1.5 once per actor, with PyTorch on the CPU and
T threads, and returns `id` (int64) and `vector` (binary: 384 little-endian
float32 values), the columns `embed.mojo` writes. The output is checked by
`./build/bench-embed check --out=OUT` in the embed environment, which
compares it with one process running encoder.mojo, or the stub.

The corpus comes from the embed environment (`pixi run -e embed
bench-embed-corpus`, or any `bench-embed run`); this script only reads it.

Settings are flags or environment variables, as in `embed.mojo`: `--corpus`
or EMBED_CORPUS (default build/embed/corpus), `--out` or EMBED_OUT
(build/embed/out-ray), `--model` or EMBED_MODEL, `--stub=1` (the stub
encoder, the same vectors as `embed.mojo`'s), `--embed-workers` or
EMBED_GROUP_WORKERS (E, embed actors, default 1), `--threads` or
EMBED_THREADS (T, PyTorch threads and Ray CPUs per actor, default 4),
`--cpus` or EMBED_CPUS (Ray's CPUs, default E * T + 2: two more for the
reads and the writes, as `embed.mojo` has K = 2 workers for them),
`--batch` or EMBED_BATCH (32), `--ray-tmp` or EMBED_RAY_TMP (Ray's temp
directory; its socket paths must stay under about 104 bytes on macOS),
`--corrupt=1` (one wrong value, which the check must catch) and `--pin` or
EMBED_PIN (Linux only: one CPU set per actor, as `embed.mojo` takes it,
such as `0-3/4-7`; each actor takes a free set when it starts and pins
every thread of its process to it, before PyTorch starts its threads).
"""

import glob
import logging
import os
import resource
import sys
import time
import warnings

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DIM = 384
"""The vector size of bge-small-en-v1.5, and of the stub."""


# ---------------------------------------------------------------- settings


def _flag(name):
    prefix = f"--{name}="
    for a in sys.argv[1:]:
        if a.startswith(prefix):
            return a[len(prefix) :]
    return None


def setting(env, flag, default):
    """A setting from its flag, else its environment variable, else
    `default`."""
    f = _flag(flag)
    if f is not None:
        return f
    return os.environ.get(env) or default


class Settings:
    def __init__(self):
        self.corpus = setting("EMBED_CORPUS", "corpus", "build/embed/corpus")
        self.out = setting("EMBED_OUT", "out", "build/embed/out-ray")
        self.model = os.path.abspath(
            setting("EMBED_MODEL", "model", "build/embed/models/bge-small-en-v1.5")
        )
        self.stub = setting("EMBED_STUB", "stub", "0") == "1"
        self.embed_workers = int(setting("EMBED_GROUP_WORKERS", "embed-workers", "1"))
        self.threads = int(setting("EMBED_THREADS", "threads", "4"))
        cpus = setting("EMBED_CPUS", "cpus", "")
        self.cpus = int(cpus) if cpus else self.embed_workers * self.threads + 2
        self.batch = int(setting("EMBED_BATCH", "batch", "32"))
        self.ray_tmp = setting("EMBED_RAY_TMP", "ray-tmp", "")
        self.corrupt = setting("EMBED_CORRUPT", "corrupt", "0") == "1"
        self.pin = parse_pin(setting("EMBED_PIN", "pin", ""))
        if self.pin and len(self.pin) != self.embed_workers:
            sys.exit(
                f"embed_ray: EMBED_PIN names {len(self.pin)} CPU sets, for"
                f" E={self.embed_workers} actors"
            )
        if self.pin and not hasattr(os, "sched_setaffinity"):
            sys.exit("embed_ray: EMBED_PIN: CPU affinity is set on Linux only")


def parse_pin(spec):
    """CPU sets separated by `/`, each CPUs and ranges separated by commas,
    as `embed.mojo`'s `parse_pin` reads them; [] for ""."""
    out = []
    for part in filter(None, spec.split("/")):
        cpus = []
        for item in part.split(","):
            lo, _, hi = item.partition("-")
            cpus.extend(range(int(lo), int(hi or lo) + 1))
        out.append(cpus)
    return out


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def claim_cpus(pin, out):
    """Takes the first CPU set in `pin` that no live actor holds (a file
    OUT/slots/slot-<k> holding its pid, so that an actor restarted after a
    kill takes the set its dead one held), and pins every thread of this
    process to it. Returns the set's index and its CPUs."""
    dir = os.path.join(out, "slots")
    me = str(os.getpid())
    for _ in range(100):
        for k in range(len(pin)):
            path = os.path.join(dir, f"slot-{k}")
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
            except FileExistsError:
                with open(path) as f:
                    holder = f.read().strip()
                if holder and _alive(int(holder)):
                    continue
                tmp = f"{path}.{me}"
                with open(tmp, "w") as f:
                    f.write(me)
                os.replace(tmp, path)
            else:
                os.write(fd, me.encode())
                os.close(fd)
            for tid in os.listdir("/proc/self/task"):
                os.sched_setaffinity(int(tid), pin[k])
            return k, pin[k]
        time.sleep(0.1)
    raise RuntimeError(f"embed_ray: no free CPU set in {pin}")


def corpus_files(dir):
    files = sorted(glob.glob(os.path.join(dir, "part-*.parquet")))
    if not files:
        sys.exit(
            f"embed_ray: no corpus in {dir}: make it with `pixi run -e embed"
            " bench-embed-corpus` (or `bench-embed-stub` for the stub's)"
        )
    return [os.path.abspath(f) for f in files]


def row_groups(files):
    """Each corpus row group as (first id, rows): the batches `embed.mojo`
    embeds."""
    out = set()
    for f in files:
        p = pq.ParquetFile(f)
        for k in range(p.metadata.num_row_groups):
            ids = p.read_row_group(k, columns=["id"]).column(0)
            out.add((ids[0].as_py(), len(ids)))
    return out


def _quiet():
    """Keeps transformers' and Ray's notices out of the output."""
    os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
    os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
    os.environ.setdefault("HF_HUB_OFFLINE", "1")
    os.environ.setdefault("RAY_DEDUP_LOGS", "0")
    warnings.filterwarnings("ignore")


def _peak_mb():
    """This process's peak resident set (`ru_maxrss`: KiB on Linux, bytes
    on macOS), in MB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak * 1024 / 1e6


# ---------------------------------------------------------------- the encoders


def _fnv64(data):
    h = 0xCBF29CE484222325
    for b in data:
        h = ((h ^ b) * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return h


def stub_vectors(texts):
    """`embed.mojo`'s stub encoder: per text, `DIM` values from an xorshift
    seeded by its FNV-1a hash, scaled to unit length, in float32 as there,
    so the vectors are the same to the bit."""
    x = np.array([_fnv64(t.encode()) | 1 for t in texts], dtype=np.uint64)
    v = np.empty((len(texts), DIM), dtype=np.float32)
    for k in range(DIM):
        x ^= x << np.uint64(13)
        x ^= x >> np.uint64(7)
        x ^= x << np.uint64(17)
        v[:, k] = (x >> np.uint64(40)).astype(np.float32) / np.float32(
            1 << 23
        ) - np.float32(1.0)
    # The norm is summed in order, as the stub's loop does, and each step is
    # one fused multiply-add, as the Mojo compiler makes `norm += v * v`
    # (float32 values square exactly in float64, so rounding once to float32
    # is the fused result). Summed any other way, about one value in ten
    # differs from embed.mojo's in its last bit.
    norm = np.zeros(len(texts), dtype=np.float32)
    for k in range(DIM):
        sq = v[:, k].astype(np.float64) ** 2
        norm = (sq + norm.astype(np.float64)).astype(np.float32)
    scale = np.float32(1.0) / np.sqrt(norm)
    return v * scale[:, None]


def load_model(dir, threads):
    """bge-small-en-v1.5 from the files `get_model.sh` downloads: the
    transformer, then the pooling its `1_Pooling/config.json` names (CLS),
    then normalising in `encode`. That directory has no `modules.json`, and
    without one `SentenceTransformer(dir)` would pool by the mean."""
    import torch
    import transformers
    from sentence_transformers import SentenceTransformer, models

    transformers.utils.logging.disable_progress_bar()
    torch.set_num_threads(threads)
    if not os.path.exists(os.path.join(dir, "model.safetensors")):
        raise SystemExit(
            f"no model in {dir}: run `pixi run -e embed bench-embed-model`,"
            " or use --stub=1"
        )
    transformer = models.Transformer(dir, max_seq_length=512)
    pooling = models.Pooling.load(os.path.join(dir, "1_Pooling"))
    return SentenceTransformer(modules=[transformer, pooling], device="cpu")


def encode(model, texts):
    if model is None:
        return stub_vectors(texts)
    return model.encode(
        texts,
        batch_size=len(texts),
        normalize_embeddings=True,
        convert_to_numpy=True,
        show_progress_bar=False,
    ).astype(np.float32)


def vector_column(vectors):
    """Rows of float32 as a binary column of their little-endian bytes."""
    data = np.ascontiguousarray(vectors, dtype="<f4")
    n = data.shape[0]
    offsets = np.arange(n + 1, dtype=np.int32) * (DIM * 4)
    return pa.Array.from_buffers(
        pa.binary(), n, [None, pa.py_buffer(offsets), pa.py_buffer(data.tobytes())]
    )


class Embed:
    """The actor: the model, loaded once, with T threads. Each call appends
    its batch's first id, size, whether its ids are consecutive and this
    process's peak RSS to OUT/batches/<pid>.txt, so the driver can compare
    the batches with the corpus's row groups and report the actors' peaks;
    the load writes OUT/loads/load-<pid> with its seconds."""

    def __init__(self, model_dir, threads, stub, out, corrupt, pin):
        _quiet()
        t0 = time.perf_counter()
        slot, cpus = claim_cpus(pin, out) if pin else (-1, [])
        self.model = None if stub else load_model(model_dir, threads)
        self.corrupt = corrupt
        self.log = os.path.join(out, "batches", f"{os.getpid()}.txt")
        with open(os.path.join(out, "loads", f"load-{os.getpid()}"), "w") as f:
            f.write(f"{time.perf_counter() - t0}")
            if pin:
                f.write(f" {slot} {','.join(map(str, cpus))}")

    def __call__(self, batch):
        ids = batch.column("id").to_numpy()
        texts = batch.column("text").to_pylist()
        v = encode(self.model, texts)
        if self.corrupt and ids[0] == 0:
            v[0, 7] += np.float32(0.01)
        consecutive = bool(np.all(np.diff(ids) == 1))
        with open(self.log, "a") as f:
            f.write(f"{ids[0]} {len(ids)} {int(consecutive)} {_peak_mb()}\n")
        return pa.table({"id": pa.array(ids, pa.int64()), "vector": vector_column(v)})


# ---------------------------------------------------------------- the modes


def _clear(dir):
    os.makedirs(dir, exist_ok=True)
    for name in os.listdir(dir):
        os.remove(os.path.join(dir, name))


def run_mode(s):
    import ray
    import ray.data

    files = corpus_files(s.corpus)
    if not s.stub and not os.path.exists(os.path.join(s.model, "model.safetensors")):
        sys.exit(
            f"no model in {s.model}: run `pixi run -e embed bench-embed-model`,"
            " or use --stub=1"
        )
    out = os.path.abspath(s.out)
    for sub in ("vectors", "batches", "loads", "slots"):
        _clear(os.path.join(out, sub))
    texts = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    groups = row_groups(files)
    _quiet()

    t0 = time.perf_counter()
    init = dict(
        num_cpus=s.cpus,
        include_dashboard=False,
        logging_level=logging.WARNING,
        configure_logging=True,
    )
    if s.ray_tmp:
        init["_temp_dir"] = os.path.abspath(s.ray_tmp)
    ray.init(**init)
    try:
        ctx = ray.data.DataContext.get_current()
        ctx.enable_progress_bars = False
        ctx.print_on_execution_start = False
        logging.getLogger("ray.data").setLevel(logging.WARNING)
        init_s = time.perf_counter() - t0

        t1 = time.perf_counter()
        ds = ray.data.read_parquet(files, columns=["id", "text"])
        # `compute=ActorPoolStrategy(size=E)` is what `concurrency=E` meant
        # before Ray 2.51 deprecated it: E actors, each holding T CPUs.
        ds = ds.map_batches(
            Embed,
            fn_constructor_args=(s.model, s.threads, s.stub, out, s.corrupt, s.pin),
            batch_size=s.batch,
            batch_format="pyarrow",
            compute=ray.data.ActorPoolStrategy(size=s.embed_workers),
            num_cpus=s.threads,
        )
        ds.write_parquet(os.path.join(out, "vectors"))
        secs = time.perf_counter() - t1
    finally:
        ray.shutdown()

    rows = sum(
        pq.ParquetFile(f).metadata.num_rows
        for f in glob.glob(os.path.join(out, "vectors", "*.parquet"))
    )
    what = "stub" if s.stub else s.model
    print(
        f"embed_ray: {rows} texts of {texts}, {what}, Ray Data with E={s.embed_workers}"
        f" actors x T={s.threads} threads, {s.cpus} CPUs: {secs:.2f} s,"
        f" {rows / secs:.1f} texts/s (the model's load included;"
        f" ray.init {init_s:.2f} s before it, not included)"
    )
    batches, odd, peaks, distinct = 0, 0, {}, set()
    for log in sorted(glob.glob(os.path.join(out, "batches", "*.txt"))):
        pid = os.path.basename(log)[:-4]
        with open(log) as f:
            for line in f:
                first, n, consecutive, peak = line.split()
                batches += 1
                distinct.add((int(first), int(n)))
                if consecutive != "1" or (int(first), int(n)) not in groups:
                    odd += 1
                peaks[pid] = max(peaks.get(pid, 0.0), float(peak))
    print(
        f"  batches: {batches}, of which {batches - odd} are corpus row groups"
        f" and {odd} are not (the corpus has {len(groups)}); {len(distinct)}"
        f" distinct, so {batches - len(distinct)} embedded again"
    )
    for name in sorted(os.listdir(os.path.join(out, "loads"))):
        with open(os.path.join(out, "loads", name)) as f:
            parts = f.read().split()
        pinned = f", CPU set {parts[1]}: {parts[2]}" if len(parts) > 2 else ""
        print(f"  state loaded: pid {name[5:]}, in {float(parts[0]):.2f} s{pinned}")
    print(f"  peak RSS (getrusage): driver {_peak_mb():.1f} MB")
    for pid, peak in sorted(peaks.items()):
        print(f"    embed actor (pid {pid}): {peak:.1f} MB")
    print(
        "    (Ray's own processes and its read and write workers aren't"
        " counted here)"
    )
    print(f"  output: {out}/vectors; check it with `./build/bench-embed check"
          f" --out={s.out} --corpus={s.corpus}{' --stub=1' if s.stub else ''}`")


def encode_mode(s):
    """The encoder alone in this process over the corpus, a row group at a
    time, as `bench-embed encode` runs encoder.mojo."""
    _quiet()
    import torch

    batches = []
    for f in corpus_files(s.corpus):
        p = pq.ParquetFile(f)
        for k in range(p.metadata.num_row_groups):
            batches.append(p.read_row_group(k, columns=["text"]).column(0).to_pylist())
    t0 = time.perf_counter()
    model = None if s.stub else load_model(s.model, s.threads)
    load = time.perf_counter() - t0
    encode(model, batches[0])  # warm-up
    t1 = time.perf_counter()
    n = 0
    for b in batches:
        encode(model, b)
        n += len(b)
    secs = time.perf_counter() - t1
    print(
        f"encode: {n} texts in one process, PyTorch {torch.__version__} with"
        f" {torch.get_num_threads()} threads: {secs:.2f} s, {n / secs:.1f} texts/s"
        f" (load {load:.2f} s, not included)"
    )


def main():
    modes = [a for a in sys.argv[1:] if not a.startswith("--")]
    mode = modes[0] if modes else "run"
    s = Settings()
    if mode == "run":
        run_mode(s)
    elif mode == "encode":
        encode_mode(s)
    else:
        sys.exit(f"embed_ray: the mode is run or encode, not {mode}")


if __name__ == "__main__":
    main()
