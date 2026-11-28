# The embedding gate: a plan

November 27, 2026 · a plan for step 5 of morca's
[layers.md](https://github.com/morca-dev/morca/blob/main/docs/layers.md);
PR 1 is built (see "Running it" and "PR 1 against the plan")

The gate for `dista.flow` is an embedding job as a flow, beside the same job
on Ray Data: read text in batches, embed each batch with a sentence
encoder, write the vectors. This says what is measured, on what, and in
which PRs.

## What the census's own job tells us

The gate names the census's `mojo_embed` step. Read as it is today, it
can't be the benchmark unchanged:

- **Its corpus is about 2,000 short texts**, under 1 MB. It embeds them in
  one call in the step's own process, in about 2 s, and uses no dista.
- **It pins the dista before flows**, until morca is released on dista
  0.7.0 and the census moves both pins.

So the benchmark takes the census's model and the shape of its texts, and a
corpus made for it. Moving `mojo_embed` itself onto a flow is a small change
that comes after, and it isn't what the gate measures.

## The job

| | dista | Ray Data |
|---|---|---|
| Program | `bench/embed/embed.mojo` | `bench/embed/embed_ray.py` |
| Read | `Read[TaskStream]`: one corpus file per input, one batch of texts per item | `read_parquet` |
| Embed | `MapWith[StatefulTask]` holding an `Encoder` (encoder.mojo), in the group `embed` | `map_batches` with a class holding a `SentenceTransformer` |
| Write | `Map[Task]`: one Parquet file per batch, returning its name and row count | `write_parquet` |
| Model | `BAAI/bge-small-en-v1.5` at one pinned revision, the same files for both | the same |

- **The corpus is generated**, with a fixed seed: texts shaped like the
  census's repository descriptions (15 to 40 tokens), in Parquet files of
  a fixed number of rows. A generator makes any size, so nothing is
  downloaded but the model.
- **Batches are the inference batch** (32 texts), since a flow doesn't
  rebatch. The sink returns a name and a count, so the flow holds nothing
  large for an unfinished input.
- **The answer is checked.** Every run's vectors are compared with one
  process embedding the same files: the same rows, each vector within a
  small tolerance. dista against Ray is checked the same way; the encoder
  already matches sentence-transformers to eleven places on its fixtures.

## What is measured

| Bar (layers.md) | How | Where |
|---|---|---|
| Speed: rows per second per core at least Ray Data's | Both at the same number of embed processes and threads per process, on the same machine, best of three | `sai` (16 threads, Linux) and the Mac |
| Memory: peak under a fixed budget as the corpus grows | Peak memory of every process of the job, sampled, at 1, 2 and 4 times a base corpus; and dista's count of live objects | `sai` |
| Recovery: a worker killed mid-run costs its batches, the output is exact | Kill an embed worker by pid part way; the run finishes, reports reruns, and passes the same check | Both |
| Groups: the model runs only on its group's workers | Each state records its pid and group when it loads; only `embed` workers appear | Both |
| Sharing: two runs share a pool's machines | Not here: it needs pools with groups, step 4 in morca | Later |

Three things about these numbers that the write-up will state with them:

- **Embedding is slow, so the corpus can't be larger than the machine's
  memory.** At a few hundred texts a second, a corpus of several GB takes
  days. The model runs use 100,000, 200,000 and 400,000 texts, and a second
  run of the same flow with a stub in place of the model (a hash of the
  text, the same vector size) goes over a corpus several times larger
  than a memory limit fixed for the job. The stub run is what shows the
  bound.
- **"The same model" means the same weights, not the same kernels.** dista
  runs encoder.mojo and Ray runs PyTorch. The comparison is of the two
  systems as someone would use them, and a per-process rate of each
  encoder alone is reported beside it, so the framework's share is visible.
- **Memory is the sum over the job's processes**, measured the same way
  for both (proportional set size on Linux, so Ray's shared object store
  is counted once).

## Threads

The encoder uses every core of its process, and a `StatefulTask` runs one
call at a time per worker. Several embed workers on one machine would each
try to use all of it. The plan asked whether the encoder's threads can be
limited from outside.

**Result (PR 1, on the Mac): they can't.** encoder.mojo calls max-core's
`parallelize` with no worker count, which runs on the Mojo runtime's CPU
pool. That pool's size isn't read from any setting: the runtime libraries
in the `embed` environment (`libAsyncRTRuntimeGlobals.so`,
`libKGENCompilerRTShared.dylib`, `libMDriver.dylib`) name only
`MODULAR_ENABLE_AFFINITY` and `MODULAR_THREAD_BUSY_WAIT_US` among their
thread settings, and on macOS the pool is sized from
`hw.perflevel0.physicalcpu`, the performance cores. On this Mac (4
performance and 6 efficiency cores) an encoder process has 6 threads: the
main one, 4 that compute and one idle.

The encoder alone (`bench-embed encode`, 8,000 texts, `/usr/bin/time -l`;
CPU is user + sys over real):

| Setting | texts/s | real s | user s | CPU |
|---|---|---|---|---|
| none | 661 | 12.2 | 33.8 | 2.8 cores |
| `MODULAR_ENABLE_AFFINITY=1` | 663 | 12.2 | 33.8 | 2.8 |
| `MODULAR_NUM_THREADS=1` (not a setting: a control) | 660 | 12.2 | 33.9 | 2.8 |
| `OMP_NUM_THREADS=1` (a control) | 655 | 12.3 | 34.1 | 2.8 |
| `MODULAR_THREAD_BUSY_WAIT_US=0` | 777 | 10.4 | 21.0 | 2.0 |

None of them changes the thread count. `MODULAR_THREAD_BUSY_WAIT_US=0`
stops the pool's threads spinning between tasks: a third less CPU and 17%
more texts a second, here.

So what E embed workers give on this Mac, as whole flows (8,000 texts,
K = 2 workers in no group, no check, the model's load included):

| E | texts/s | with `MODULAR_THREAD_BUSY_WAIT_US=0` | CPU (user over real) | peak RSS, sum of all processes |
|---|---|---|---|---|
| 1 | 659 | 766 | 2.8 / 2.0 cores | 350 MB |
| 2 | 865 | 996 | 4.6 / 3.5 | 641 MB |
| 4 | 1,043 | 1,288 | 7.2 / 6.1 | 1,223 MB |

More embed processes help here because one process uses only the 4
performance cores and the others are idle; each embed process adds about
290 MB. The choice the plan named is still open: add a thread limit to
encoder.mojo (`parallelize` takes a worker count), or compare one embed
process with every core on each side.

On Linux the choice isn't needed: the pool follows the process's CPU
affinity, so `taskset` limits an embed process from outside. The encoder
alone on `sai` (16 threads, x86-64), 4,000 texts, single runs on
2026-11-27:

| `taskset -c` | texts/s |
|---|---|
| `0-15` | 165 |
| `0-3` | 130 |
| `0` | 48 |
| `0-15`, `MODULAR_THREAD_BUSY_WAIT_US=0` | 179 |

One process gains little past 4 cores there, so the runs on `sai` will use
several embed processes pinned to a few cores each, and Ray's side the same
number of actors with as many CPUs and PyTorch threads. The whole flow on
`sai` with one unpinned embed worker passes its check at 160 texts/s, with
295 MB in the embed worker and 403 MB over all five processes.

### Local mode and the encoder

A worker that `Runtime(workers=K, groups="embed=E")` forks can't run the
encoder. The Mojo runtime starts its CPU pool when the process starts, and
`fork` copies only the calling thread, so in the child `parallelize` waits
for ever for threads that aren't there (on macOS it crashes in
`_dispatch_semaphore_signal` or hangs in `_dispatch_semaphore_wait`). This
holds for any task that uses `parallelize`, not only the encoder. So
`embed.mojo` starts the head and the workers as new processes of the same
binary, as in a cluster (`DISTA_ROLE`, `DISTA_HEAD`, `DISTA_GROUP`), and
drives them with `Runtime()`. `EMBED_LAUNCH=local` uses the local mode's
forks, which works with the stub only.

## PRs

| # | What | Done when |
|---|---|---|
| 1 | **(done)** The corpus generator, `embed.mojo` as a flow, the single-process check, the stub encoder; an `embed` environment in `pixi.toml` with encoder.mojo, outside the default one; the answer on threads | `pixi run -e embed bench-embed` passes its check on the Mac and on `sai` |
| 2 | `embed_ray.py` and a `ray` environment (Ray Data, PyTorch for CPU, sentence-transformers), checked against the same reference | The Ray run passes the same check on both machines |
| 3 | The runs: a script that samples memory, runs the sizes, kills a worker, and writes a results folder as `bench/cloud` does | The table above is filled in for both systems |
| 4 | The write-up: this file becomes the results, with what the gate passed, what it didn't and what is still unmeasured | Merged with the numbers |

Neither environment is part of the default one or of CI's test jobs. The
plan had CI build `embed.mojo` and run it on a few hundred texts with the
stub, so it can't rot; PR 1 leaves that out, since the `embed` environment
is a large install (see "PR 1 against the plan").

## Running it

```sh
pixi run -e embed bench-embed-model    # bge-small-en-v1.5 at its pinned revision, 133 MB
pixi run -e embed bench-embed-corpus   # the corpus alone, and its token counts
pixi run -e embed bench-embed          # 4,000 texts through the flow, then the check
pixi run -e embed bench-embed-stub     # 640 texts with the stub, no model needed
./build/bench-embed run --texts=20000 --workers=2 --embed-workers=2
./build/bench-embed encode --texts=8000   # the encoder alone, in one process
```

`bench-embed-build` first checks out parquet.mojo and its pure-Mojo tins
under `build/embed-deps` (`bench/embed/deps.sh`), at the revisions of CI's
data job. Every setting is an environment variable or the same name as a
flag: `EMBED_TEXTS` (`--texts`, default 4,000), `EMBED_ROWS` (`--rows`,
texts per corpus file, 512), `EMBED_CORPUS` (`build/embed/corpus`),
`EMBED_OUT` (`build/embed/out`), `EMBED_MODEL`
(`build/embed/models/bge-small-en-v1.5`), `EMBED_STUB` (`--stub=1`),
`EMBED_WORKERS` (`--workers`, K, default 2), `EMBED_GROUP_WORKERS`
(`--embed-workers`, E, default 1), `EMBED_CHECK` (`--check=0` skips it) and
`EMBED_CORRUPT` (`--corrupt=1` writes one wrong value, which the check must
catch). Without the model, `bench-embed` says how to get it and exits
non-zero. A run prints the texts, the wall seconds and texts a second, each
operator's `FlowStats`, the peak RSS of the driver and of every process it
started, and each state's pid, group and load time.

### First numbers (single runs from development, not the gate's results)

On an Apple M4 Mac mini (4 performance and 6 efficiency cores, 24 GB,
macOS 27.2.1), November 27, 2026, with the default K = 2:

| Run | texts | E | texts/s | peak RSS |
|---|---|---|---|---|
| `bench-embed` (model, check passed, largest difference 0.0) | 4,000 | 1 | 665 | 291 MB the embed worker, 350 MB all five processes |
| model, no check | 8,000 | 1, 2, 4 | 659, 865, 1,043 | 350, 641, 1,223 MB all processes (see "Threads") |
| stub, check passed | 250,000 | 1 | 97,700 | 90 MB all processes |
| stub, check passed | 1,000,000 | 1 | 97,500 | 106 MB |
| stub, check passed | 2,000,000 (136 MB of corpus, 3.3 GB of vectors) | 1 | 96,900 | 124 MB |

In the stub runs the workers stay at 14 to 17 MB whatever the corpus. The
head and the driver grow with the number of batches: from 20 to 38 MB and
from 21 to 37 MB between 7,813 and 62,500 batches, about 0.3 KB a batch
each. The driver's part is partly the bench's own list of the files
written, which the check needs; the head's is unexplained so far, and the
memory bar in PR 3 will have to look at it.

### PR 1 against the plan

- **The types.** The read emits a `Texts` per batch: the corpus file's
  name, the batch's index, its ids as a `Seq[Int64]` and its texts as a
  `Seq[String]`. The `StatefulTask` returns a `Vectors`: the same name and
  index, the ids, the dimension and the values as one `Seq[Float32]`, row
  after row. The sink returns a `Written`, the file's name and its row
  count. All three are plain structs, which dista's codec encodes field by
  field; a `RecordBatch` would have worked too, but needs building twice.
- **The corpus.** Text `i` depends only on the seed (20261127) and `i`,
  and has id `i`, so any size or layout holds the same texts. Each is a
  name of one to three words, a description and one to three topics, as
  the census's `repo_input` builds them, for example "nn: A fast tokenizer
  for Mojo for GPUs, supporting Linux and macOS. Topics: llm, gpu, data";
  15 to 39 tokens, 25 on average, on the first file. Files hold `id`
  (int64) and `text` (utf8) in row groups of 32, and the read asks for
  batches of 32 too. A corpus is written again only if its
  `manifest.txt` names other sizes or another version of the generator.
- **The output.** One file per batch, `EMBED_OUT/vectors/<file>-b<batch>.parquet`,
  with `id` (int64) and `vector` (binary: 384 little-endian float32
  values, as the census stores them), written under a temporary name and
  moved into place. A rerun of an input overwrites its first attempt's
  files, and the check reads only the files the results name.
- **The check** embeds the same files in the driver, batch by batch as the
  flow does, so padding is the same, with the same encoder. Every id must
  come back exactly once, in the file and row it was read from, and every
  value within 1e-5 of the single process's. On every run so far the
  largest difference was 0.0. A mismatch raises with the id, the value and
  both numbers, and the program exits non-zero.
- **The stub** gives each text 384 values from an xorshift seeded by the
  text's FNV-1a hash, scaled to unit length.
- **The groups bar.** Each state writes `EMBED_OUT/loads/load-<pid>-<n>`
  with its `DISTA_GROUP` when it loads; the run prints them and fails if
  one isn't `embed`.
- **Processes, not forks**, for the reason in "Local mode and the
  encoder". `embed.mojo` starts the head and the K + E workers itself, and
  kills them by pid when the flow ends, reading each one's peak RSS with
  `wait4`.
- **parquet.mojo from checkouts of its own.** The data environment
  compiles parquet.mojo from checkouts next to this one, which on a
  developer's machine can be at any revision, and the parquet-mojo package
  pins another arrow-mlake than dista. The `embed` environment takes
  dista's tins as the default environment does, and `deps.sh` checks out
  parquet.mojo, thrift, hashes, snappy, deflate and threads' compat shim
  under `build/embed-deps` at the revisions CI's data job names, which it
  reads from `ci.yml`.
- **No CI step.** CI installs one environment per job, and the `embed`
  environment brings max-core (443 MB installed on macOS). Running the
  stub in CI needs that install in the data job or a job of its own, so it
  is left for a decision.

## Not in this plan

- The sharing bar, and groups on a pool of several machines.
- A GPU: `sai` has none now and the encoder has no GPU path.
- Runs on AWS. They follow if the one-machine numbers leave a question a
  cluster would answer.
- Moving the census's `mojo_embed` onto a flow.

## Defaults (confirmed 2026-11-27)

1. The benchmark lives in dista's `bench/embed`, on a generated corpus,
   with the census's model. The census's own step isn't changed.
2. Ray's side uses sentence-transformers on PyTorch with the same weights.
3. The numbers come from `sai` and this Mac, not from AWS.
4. The memory bound is shown by the stub run over a large corpus, and the
   model runs stop at 400,000 texts.
5. The sharing bar waits for step 4.
