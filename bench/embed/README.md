# The embedding gate: a plan

November 27, 2026 · a plan for step 5 of morca's
[layers.md](https://github.com/morca-dev/morca/blob/main/docs/layers.md),
nothing here is built

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
try to use all of it. The first PR finds out whether the encoder's threads
can be limited from outside. If they can, the runs use E embed workers of T
threads each, with Ray set to the same E and T. If they can't, the choice is
between adding a limit to encoder.mojo and comparing one embed process with
all cores on each side; that comes back as a question before any numbers.

## PRs

| # | What | Done when |
|---|---|---|
| 1 | The corpus generator, `embed.mojo` as a flow, the single-process check, the stub encoder; an `embed` environment in `pixi.toml` with encoder.mojo, outside the default one; the answer on threads | `pixi run -e embed bench-embed` passes its check on the Mac and on `sai` |
| 2 | `embed_ray.py` and a `ray` environment (Ray Data, PyTorch for CPU, sentence-transformers), checked against the same reference | The Ray run passes the same check on both machines |
| 3 | The runs: a script that samples memory, runs the sizes, kills a worker, and writes a results folder as `bench/cloud` does | The table above is filled in for both systems |
| 4 | The write-up: this file becomes the results, with what the gate passed, what it didn't and what is still unmeasured | Merged with the numbers |

Neither environment is part of the default one or of CI's test jobs. CI
builds `embed.mojo` and runs it on a few hundred texts with the stub, so it
can't rot.

## Not in this plan

- The sharing bar, and groups on a pool of several machines.
- A GPU: `sai` has none now and the encoder has no GPU path.
- Runs on AWS. They follow if the one-machine numbers leave a question a
  cluster would answer.
- Moving the census's `mojo_embed` onto a flow.

## Defaults to confirm

1. The benchmark lives in dista's `bench/embed`, on a generated corpus,
   with the census's model. The census's own step isn't changed.
2. Ray's side uses sentence-transformers on PyTorch with the same weights.
3. The numbers come from `sai` and this Mac, not from AWS.
4. The memory bound is shown by the stub run over a large corpus, and the
   model runs stop at 400,000 texts.
5. The sharing bar waits for step 4.
