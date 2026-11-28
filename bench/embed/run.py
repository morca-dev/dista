"""One measured run of the embedding benchmark (bench/embed/README.md, PR 3).

    pixi run -e ray python bench/embed/run.py --system=dista --texts=100000 \\
        --embed-workers=2 --threads=4 --pin=0-3/4-7 --results=bench/embed/results/X

Runs one system (`dista`: `build/bench-embed` in the embed environment;
`ray`: `embed_ray.py` in the ray environment) in one configuration, as
`run` (the job) or `encode` (the encoder alone, in one process), and:

- samples the memory of every process of the job about once a second: on
  Linux the sum of PSS (`/proc/<pid>/smaps_rollup`) over the job's process
  tree, so Ray's shared object store counts once; on macOS the sum of RSS
  (`ps`), which counts shared pages once per process;
- measures the CPU the job used, user + sys: the largest value sampled per
  process (`/proc/<pid>/stat`, `ps`) summed, and `wait4`'s for the job's
  root and what it waited for; the larger of the two is reported;
- with `--kill-after=S`, kills one embed worker (dista) or embed actor (Ray)
  by its pid, S seconds after the first one loaded the model;
- checks the output: `--check=full` runs `bench-embed check` (one process
  embedding the corpus with encoder.mojo, or the stub), `--check=ref:DIR`
  compares with DIR, an earlier output that passed `full`, by id, every id
  once and every value within 1e-5 (texts are the same at every corpus size,
  so a larger reference covers a smaller corpus);
- appends one row to RESULTS/runs.tsv, with the machine, the commit and the
  exact command, keeps the program's output in RESULTS/logs/<id>.log, every
  process's peak, first and last memory in RESULTS/logs/<id>.procs.tsv, and
  writes RESULTS/meta.txt (machine and versions) if it is missing.

Only processes this script started, and their descendants, are sampled or
killed, by pid. After the run it checks that none of them is left.
"""

import argparse
import datetime
import glob
import json
import os
import platform
import re
import resource
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time

LINUX = sys.platform.startswith("linux")
TICK = os.sysconf("SC_CLK_TCK") if LINUX else 100
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


# ---------------------------------------------------------------- processes


def _proc_table():
    """{pid: (ppid, cpu seconds, name, start)} of every process."""
    out = {}
    if LINUX:
        for d in os.listdir("/proc"):
            if not d.isdigit():
                continue
            try:
                with open(f"/proc/{d}/stat") as f:
                    s = f.read()
            except OSError:
                continue
            name = s[s.index("(") + 1 : s.rindex(")")]
            f = s[s.rindex(")") + 2 :].split()
            # After the name: state ppid ... utime(14) stime(15) ... starttime(22).
            out[int(d)] = (int(f[1]), (int(f[11]) + int(f[12])) / TICK, name, f[19])
        return out
    ps = subprocess.run(
        ["ps", "-A", "-o", "pid=,ppid=,time=,lstart=,comm="],
        capture_output=True,
        text=True,
    ).stdout
    for line in ps.splitlines():
        p = line.split(None, 8)
        if len(p) < 9:
            continue
        out[int(p[0])] = (int(p[1]), _ps_time(p[2]), p[8], " ".join(p[3:8]))
    return out


def _ps_time(t):
    """`ps`'s `time`, [dd-][hh:]mm:ss.ss, in seconds."""
    days = 0
    if "-" in t:
        d, t = t.split("-", 1)
        days = int(d)
    secs = 0.0
    for part in t.split(":"):
        secs = secs * 60 + float(part)
    return days * 86400 + secs


def _tree(table, root):
    """`root` and its descendants in `table`."""
    kids = {}
    for pid, (ppid, *_rest) in table.items():
        kids.setdefault(ppid, []).append(pid)
    out, todo = [], [root]
    while todo:
        p = todo.pop()
        if p in table:
            out.append(p)
            todo.extend(kids.get(p, []))
    return out


def _mem_mb(pids):
    """{pid: MB}: PSS on Linux, RSS on macOS."""
    out = {}
    if LINUX:
        for p in pids:
            try:
                with open(f"/proc/{p}/smaps_rollup") as f:
                    for line in f:
                        if line.startswith("Pss:"):
                            out[p] = int(line.split()[1]) * 1024 / 1e6
                            break
            except OSError:
                pass
        return out
    if not pids:
        return out
    ps = subprocess.run(
        ["ps", "-o", "pid=,rss=", "-p", ",".join(map(str, pids))],
        capture_output=True,
        text=True,
    ).stdout
    for line in ps.splitlines():
        p = line.split()
        if len(p) == 2:
            out[int(p[0])] = int(p[1]) * 1024 / 1e6
    return out


class Sampler(threading.Thread):
    """Samples the job's process tree about once a second until `stop`."""

    def __init__(self, root, t0):
        super().__init__(daemon=True)
        self.root, self.t0 = root, t0
        self.done = threading.Event()
        self.peak_sum = 0.0
        self.peak_at = 0.0
        self.samples = 0
        self.procs = {}  # pid -> dict(name, start, first, last, peak, cpu, ...)
        self.series = []  # (t, sum MB, processes)

    metrics_port = 0
    retried = None

    def run(self):
        while not self.done.is_set():
            self.sample()
            if self.metrics_port:
                self.ask_metrics()
            self.done.wait(1.0)

    def ask_metrics(self):
        """dista's head: `dista_tasks_retried_total`, the tasks sent again
        because their worker went away."""
        import urllib.request

        try:
            url = f"http://127.0.0.1:{self.metrics_port}/metrics"
            with urllib.request.urlopen(url, timeout=0.5) as f:
                text = f.read().decode()
        except OSError:
            return
        m = re.search(r"^dista_tasks_retried_total (\d+)", text, re.M)
        if m:
            self.retried = int(m[1])

    def sample(self):
        table = _proc_table()
        pids = _tree(table, self.root)
        mem = _mem_mb(pids)
        t = time.monotonic() - self.t0
        total = 0.0
        for p in pids:
            ppid, cpu, name, start = table[p]
            m = mem.get(p)
            r = self.procs.get(p)
            if r is None or r["start"] != start:
                r = self.procs[p] = dict(
                    pid=p, ppid=ppid, name=name, start=start, first=None,
                    first_t=None, last=0.0, last_t=t, peak=0.0, cpu=0.0,
                )
            r["name"] = name
            r["cpu"] = max(r["cpu"], cpu)
            if m is not None:
                total += m
                if r["first"] is None:
                    r["first"], r["first_t"] = m, t
                r["last"], r["last_t"] = m, t
                r["peak"] = max(r["peak"], m)
        self.samples += 1
        self.series.append((round(t, 1), round(total, 1), len(pids)))
        if total > self.peak_sum:
            self.peak_sum, self.peak_at = total, t

    def stop(self):
        self.done.set()
        self.join()


def leftovers(procs):
    """The sampled processes still alive (the same pid and start)."""
    table = _proc_table()
    return [
        r for r in procs.values()
        if r["pid"] in table and table[r["pid"]][3] == r["start"]
    ]


# ---------------------------------------------------------------- environments


_envs = {}


def pixi_env(name):
    """The environment `pixi run -e name` gives a command."""
    if name not in _envs:
        raw = subprocess.run(
            ["pixi", "run", "-e", name, "--", "env", "-0"],
            cwd=ROOT, capture_output=True, check=True,
        ).stdout.decode()
        env = {}
        for item in raw.split("\0"):
            if "=" in item:
                k, v = item.split("=", 1)
                env[k] = v
        _envs[name] = env
    return dict(_envs[name])


def versions():
    out = []
    for env, names in (
        ("embed", ("mojo-compiler", "max-core", "encoder-mojo")),
        ("ray", ("python", "ray-data", "ray-core", "pytorch", "sentence-transformers",
                 "transformers", "libblas", "numpy", "pyarrow")),
    ):
        listing = subprocess.run(
            ["pixi", "list", "-e", env], cwd=ROOT, capture_output=True, text=True
        ).stdout
        for line in listing.splitlines():
            p = line.split()
            if p and p[0] in names:
                out.append(f"{env}: {p[0]} {p[1]} {p[2] if len(p) > 2 else ''}".strip())
    return out


def machine():
    if LINUX:
        cpu = subprocess.run(["lscpu"], capture_output=True, text=True).stdout
        model = re.search(r"Model name:\s*(.*)", cpu).group(1)
        mem = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    else:
        model = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
        ).stdout.strip()
        mem = int(subprocess.run(
            ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True
        ).stdout) / 2**30
    return f"{model}, {os.cpu_count()} logical CPUs, {mem:.0f} GiB, {platform.platform()}"


def commit():
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", "bench/embed/embed.mojo",
         "bench/embed/embed_ray.py", "bench/embed/corpus.mojo", "bench/embed/run.py",
         "pixi.toml", "pixi.lock"],
        cwd=ROOT, capture_output=True, text=True,
    ).stdout.strip()
    return head + ("+dirty" if dirty else "")


# ---------------------------------------------------------------- the check


def compare(out, ref, corpus, tolerance=1e-5):
    """Every corpus id once in OUT's parquet files, each vector within
    `tolerance` of REF's (an output that passed `bench-embed check`)."""
    import numpy as np
    import pyarrow.parquet as pq

    def load(dir):
        ids, vecs = [], []
        for f in sorted(glob.glob(os.path.join(dir, "**", "*.parquet"), recursive=True)):
            t = pq.read_table(f, columns=["id", "vector"])
            ids.append(t.column("id").to_numpy())
            for chunk in t.column("vector").chunks:
                offsets = np.frombuffer(chunk.buffers()[1], dtype=np.int32)
                start = int(offsets[chunk.offset])
                if int(offsets[chunk.offset + len(chunk)]) - start != len(chunk) * 384 * 4:
                    raise SystemExit(f"compare: {f}: a vector isn't 384 float32 values")
                vecs.append(np.frombuffer(chunk.buffers()[2], dtype="<f4",
                            count=len(chunk) * 384, offset=start).reshape(-1, 384))
        return np.concatenate(ids), np.concatenate(vecs)

    want_ids = np.concatenate([
        pq.read_table(f, columns=["id"]).column(0).to_numpy()
        for f in sorted(glob.glob(os.path.join(corpus, "part-*.parquet")))
    ])
    rid, rvec = load(ref)
    order = np.full(rid.max() + 1, -1, dtype=np.int64)
    order[rid] = np.arange(len(rid))
    gid, gvec = load(out)
    if len(np.unique(gid)) != len(gid):
        return False, f"compare: {len(gid) - len(np.unique(gid))} ids twice"
    if set(gid.tolist()) != set(want_ids.tolist()):
        return False, (f"compare: {len(gid)} ids, the corpus has {len(want_ids)};"
                       " the sets differ")
    if want_ids.max() > rid.max() or (order[want_ids] < 0).any():
        return False, "compare: the reference lacks some of the corpus's ids"
    worst = float(np.abs(gvec - rvec[order[gid]]).max())
    ok = worst <= tolerance
    return ok, (f"compare: {'passed' if ok else 'FAILED'}, {len(gid)} ids once each,"
                f" largest difference from {ref} {worst:.3g} (tolerance {tolerance})")


# ---------------------------------------------------------------- one run


def build_command(a, corpus, out):
    """The command and its environment."""
    if a.system == "dista":
        env = pixi_env("embed")
        cmd = [os.path.join(ROOT, "build", "bench-embed"), a.mode,
               f"--texts={a.texts}", f"--rows={a.rows}", f"--corpus={corpus}"]
        if a.mode == "run":
            cmd += [f"--out={out}", f"--workers={a.workers}",
                    f"--embed-workers={a.embed_workers}", "--check=0"]
        if a.stub:
            cmd.append("--stub=1")
        if a.pin and a.mode == "run":
            env["EMBED_PIN"] = a.pin
        if a.busy_wait_0:
            env["MODULAR_THREAD_BUSY_WAIT_US"] = "0"
        if a.mode == "run":
            with socket.socket() as sk:
                sk.bind(("127.0.0.1", 0))
                env["EMBED_METRICS_PORT"] = str(sk.getsockname()[1])
    else:
        env = pixi_env("ray")
        cmd = ["python", os.path.join(ROOT, "bench", "embed", "embed_ray.py"), a.mode,
               f"--corpus={corpus}", f"--threads={a.threads}"]
        if a.mode == "run":
            cmd += [f"--out={out}", f"--embed-workers={a.embed_workers}",
                    f"--ray-tmp={a.ray_tmp}"]
            if a.pin:
                cmd.append(f"--pin={a.pin}")
        if a.stub:
            cmd.append("--stub=1")
    if a.mode == "encode" and a.pin:
        if not LINUX:
            sys.exit("run.py: --pin is Linux only")
        cmd = ["taskset", "-c", a.pin.split("/")[0]] + cmd
    return cmd, env


def make_corpus(a, corpus):
    env = pixi_env("embed")
    cmd = [os.path.join(ROOT, "build", "bench-embed"), "corpus", f"--texts={a.texts}",
           f"--rows={a.rows}", f"--corpus={corpus}"]
    if a.stub:
        cmd.append("--stub=1")
    subprocess.run(cmd, cwd=ROOT, env=env, check=True, stdout=subprocess.DEVNULL)


def kill_one(a, out, proc, t_start, tree_root, note):
    """Kills one embed process by pid, `a.kill_after` s after the first one
    loaded the model."""
    loads = os.path.join(out, "loads")
    while proc.poll() is None:
        names = sorted(os.listdir(loads)) if os.path.isdir(loads) else []
        if names:
            break
        time.sleep(0.2)
    else:
        return
    t_loaded = time.monotonic()
    while proc.poll() is None and time.monotonic() - t_loaded < a.kill_after:
        time.sleep(0.2)
    if proc.poll() is not None:
        note["kill"] = "the run ended before the kill"
        return
    pid = int(sorted(os.listdir(loads))[0].split("-")[1])
    if pid not in _tree(_proc_table(), tree_root):
        note["kill"] = f"pid {pid} isn't in the job's tree; not killed"
        return
    os.kill(pid, signal.SIGKILL)
    note["kill"] = f"SIGKILL to embed pid {pid} at {time.monotonic() - t_start:.1f} s"
    note["killed_pid"] = pid


def parse(system, mode, text):
    """What the program printed: seconds, texts/s, reruns, groups, ..."""
    r = {}
    if mode == "encode":
        m = re.search(r"encode: (\d+) texts.*?: ([\d.]+) s, ([\d.]+) texts/s", text)
        if m:
            r.update(texts_done=int(m[1]), secs=float(m[2]), rate=float(m[3]))
        return r
    if system == "dista":
        m = re.search(r"embed: (\d+) texts.*?: ([\d.]+) s, ([\d.]+) texts/s", text)
        if m:
            r.update(texts_done=int(m[1]), secs=float(m[2]), rate=float(m[3]))
        r["reruns"] = sum(int(x) for x in re.findall(r"reruns (\d+)", text))
        m = re.search(r"(\d+) live objects, (\d+) results kept, (\d+) located on workers,"
                      r" (\d+) tasks pending", text)
        if m:
            r["head_objects_max"] = int(m[1])
            r["head_results_max"] = int(m[2])
            r["head_located_max"] = int(m[3])
        r["roles"] = {int(p): role for role, p in re.findall(r"    (.+?) \(pid (\d+)\):", text)}
        r["loads"] = re.findall(r"state loaded: pid (\d+), DISTA_GROUP=(\S*),", text)
    else:
        m = re.search(r"embed_ray: (\d+) texts of (\d+).*?: ([\d.]+) s, ([\d.]+) texts/s", text)
        if m:
            r.update(texts_done=int(m[1]), secs=float(m[3]), rate=float(m[4]))
        m = re.search(r"(\d+) embedded again", text)
        if m:
            r["reruns"] = int(m[1])
        r["loads"] = re.findall(r"state loaded: pid (\d+), in", text)
    return r


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--system", choices=["dista", "ray"], required=True)
    p.add_argument("--mode", choices=["run", "encode"], default="run")
    p.add_argument("--texts", type=int, required=True)
    p.add_argument("--rows", type=int, default=512)
    p.add_argument("--embed-workers", type=int, default=1, help="E")
    p.add_argument("--threads", type=int, default=4, help="T (Ray; dista's follow --pin)")
    p.add_argument("--workers", type=int, default=2, help="K, dista's workers in no group")
    p.add_argument("--pin", default="", help="CPU sets, one per embed process: 0-3/4-7")
    p.add_argument("--busy-wait-0", action="store_true",
                   help="MODULAR_THREAD_BUSY_WAIT_US=0 for dista")
    p.add_argument("--stub", action="store_true")
    p.add_argument("--kill-after", type=float, default=0.0)
    p.add_argument("--check", default="none", help="full, ref:DIR or none")
    p.add_argument("--work", default=os.path.join(ROOT, "build", "embed", "runs"))
    p.add_argument("--ray-tmp", default=os.environ.get("EMBED_RAY_TMP", ""))
    p.add_argument("--results", required=True)
    p.add_argument("--tag", default="", help="a note for the row")
    p.add_argument("--keep", default="", help="move the output here afterwards")
    p.add_argument("--timeout", type=float, default=7200, help="seconds; 0 for none")
    a = p.parse_args()

    results = os.path.abspath(a.results)
    os.makedirs(os.path.join(results, "logs"), exist_ok=True)
    host = socket.gethostname().split(".")[0]
    meta = os.path.join(results, "meta.txt")
    if not os.path.exists(meta):
        with open(meta, "w") as f:
            f.write(f"host: {host}\nmachine: {machine()}\n")
            f.write(f"memory measured: {'PSS (smaps_rollup)' if LINUX else 'RSS (ps)'}\n")
            for v in versions():
                f.write(v + "\n")

    kind = "stub" if a.stub else "model"
    corpus = os.path.join(a.work, f"corpus-{kind}-{a.texts}-{a.rows}")
    out = os.path.join(a.work, f"out-{a.system}")
    if not a.ray_tmp:
        a.ray_tmp = os.path.join(a.work, "rt")
    make_corpus(a, corpus)
    if a.mode == "run" and os.path.isdir(out):
        subprocess.run(["rm", "-rf", out], check=True)
    cmd, env = build_command(a, corpus, out)

    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    rid = f"{stamp}-{a.system}-{a.mode}-{kind}-{a.texts}-E{a.embed_workers}T{a.threads}"
    log_path = os.path.join(results, "logs", rid + ".log")
    load0 = os.getloadavg()
    note = {}
    t0 = time.monotonic()
    with open(log_path, "w") as log:
        log.write("$ " + " ".join(shlex.quote(c) for c in cmd) + "\n")
        log.flush()
        proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        sampler = Sampler(proc.pid, t0)
        sampler.metrics_port = int(env.get("EMBED_METRICS_PORT", 0))
        sampler.start()
        killer = None
        if a.kill_after > 0:
            killer = threading.Thread(
                target=kill_one, args=(a, out, proc, t0, proc.pid, note), daemon=True)
            killer.start()
        while True:
            pid, status, usage = os.wait4(proc.pid, os.WNOHANG)
            if pid:
                break
            if a.timeout and time.monotonic() - t0 > a.timeout:
                # Every process of the job, by the pids sampled from its tree.
                note["kill"] = (note.get("kill", "") + f" timed out after {a.timeout} s").strip()
                for pid in _tree(_proc_table(), proc.pid):
                    os.kill(pid, signal.SIGKILL)
            time.sleep(0.2)
        proc.returncode = os.waitstatus_to_exitcode(status)
        wall = time.monotonic() - t0
        sampler.stop()
        if killer:
            killer.join()
    load1 = os.getloadavg()
    with open(log_path) as f:
        text = f.read()
    r = parse(a.system, a.mode, text)

    # What is left of the job: wait a little for Ray's processes to go.
    left = leftovers(sampler.procs)
    for _ in range(20):
        if not left:
            break
        time.sleep(0.5)
        left = leftovers(sampler.procs)
    for x in left:
        os.kill(x["pid"], signal.SIGKILL)

    cpu_sampled = sum(x["cpu"] for x in sampler.procs.values())
    cpu_wait4 = usage.ru_utime + usage.ru_stime
    cpu = max(cpu_sampled, cpu_wait4)

    check = "none"
    if proc.returncode == 0 and a.mode == "run" and a.check != "none":
        if a.check == "full":
            ccmd = [os.path.join(ROOT, "build", "bench-embed"), "check", f"--out={out}",
                    f"--corpus={corpus}", f"--texts={a.texts}", f"--rows={a.rows}"]
            if a.stub:
                ccmd.append("--stub=1")
            c = subprocess.run(ccmd, cwd=ROOT, env=pixi_env("embed"), capture_output=True,
                               text=True)
            lines = (c.stdout + c.stderr).strip().splitlines()
            line = lines[-1] if lines else ""
            check = ("passed: " if c.returncode == 0 else "FAILED: ") + line
            with open(log_path, "a") as f:
                f.write("$ " + " ".join(shlex.quote(x) for x in ccmd) + "\n")
                f.write(c.stdout + c.stderr)
        elif a.check.startswith("ref:"):
            ok, line = compare(out, a.check[4:], corpus)
            check = ("passed: " if ok else "FAILED: ") + line
            with open(log_path, "a") as f:
                f.write(line + "\n")

    # Groups: which processes loaded the model.
    groups = ""
    if a.mode == "run" and "loads" in r:
        if a.system == "dista":
            bad = [pid for pid, g in r["loads"] if g != "embed"]
            groups = (f"{len(r['loads'])} loads, all DISTA_GROUP=embed" if not bad
                      else f"loads outside embed: {bad}")
        else:
            names = {x["pid"]: x["name"] for x in sampler.procs.values()}
            groups = "; ".join(f"{pid} {names.get(int(pid), '?')}" for pid in r["loads"])

    # Every process's memory, and the head's growth for dista.
    procs_path = os.path.join(results, "logs", rid + ".procs.tsv")
    roles = r.get("roles", {})
    if a.system == "dista" and a.mode == "run":
        roles.setdefault(proc.pid, "driver")
    with open(procs_path, "w") as f:
        f.write("pid\tppid\tname\trole\tcpu_s\tpeak_mb\tfirst_mb\tfirst_t\tlast_mb\tlast_t\n")
        for x in sorted(sampler.procs.values(), key=lambda x: x["pid"]):
            f.write(f"{x['pid']}\t{x['ppid']}\t{x['name']}\t{roles.get(x['pid'], '')}"
                    f"\t{x['cpu']:.1f}\t{x['peak']:.1f}\t{x['first'] or 0:.1f}"
                    f"\t{x['first_t'] or 0:.0f}\t{x['last']:.1f}\t{x['last_t']:.0f}\n")
    with open(os.path.join(results, "logs", rid + ".series.tsv"), "w") as f:
        f.write("t_s\tsum_mb\tprocesses\n")
        for t, m, n in sampler.series:
            f.write(f"{t}\t{m}\t{n}\n")
    head = ""
    for x in sampler.procs.values():
        if roles.get(x["pid"]) == "head" and x["first"] is not None:
            head = f"{x['first']:.1f}->{x['last']:.1f} MB ({x['first_t']:.0f}->{x['last_t']:.0f} s)"

    secs = r.get("secs") or wall
    row = dict(
        id=rid, date=stamp, host=host, system=a.system, mode=a.mode, encoder=kind,
        texts=a.texts, E=a.embed_workers if a.mode == "run" else 1,
        T=a.threads if a.system == "ray" or a.pin else "",
        K=a.workers if a.system == "dista" and a.mode == "run" else "",
        pin=a.pin, busy_wait_0=int(a.busy_wait_0), exit=proc.returncode,
        texts_done=r.get("texts_done", ""), secs=f"{secs:.2f}",
        rate=f"{r['rate']:.1f}" if "rate" in r else "",
        wall_s=f"{wall:.1f}", cpu_s=f"{cpu:.1f}", cpu_sampled_s=f"{cpu_sampled:.1f}",
        cpu_wait4_s=f"{cpu_wait4:.1f}", cpu_cores=f"{cpu / wall:.2f}",
        mem=("pss" if LINUX else "rss"), peak_mb=f"{sampler.peak_sum:.1f}",
        peak_at_s=f"{sampler.peak_at:.0f}", processes=len(sampler.procs),
        samples=sampler.samples, head_mb=head,
        head_objects_max=r.get("head_objects_max", ""),
        reruns=r.get("reruns", ""),
        tasks_retried="" if sampler.retried is None else sampler.retried, kill=note.get("kill", ""), groups=groups,
        check=check, leftover=len(left),
        load_before=" ".join(f"{x:.2f}" for x in load0),
        load_after=" ".join(f"{x:.2f}" for x in load1),
        commit=commit(), tag=a.tag,
        command=" ".join(shlex.quote(c) for c in cmd)
        + "".join(f" [{k}={env[k]}]" for k in ("EMBED_PIN", "MODULAR_THREAD_BUSY_WAIT_US")
                  if k in env),
    )
    tsv = os.path.join(results, "runs.tsv")
    rows, cols = [], list(row)
    if os.path.exists(tsv):
        with open(tsv) as f:
            lines = f.read().splitlines()
        head = lines[0].split("\t")
        rows = [dict(zip(head, line.split("\t"))) for line in lines[1:]]
        # A column added since: every row gets it, empty where it wasn't kept.
        cols = head + [c for c in row if c not in head]
    rows.append({k: str(v).replace("\t", " ") for k, v in row.items()})
    with open(tsv + ".tmp", "w") as f:
        f.write("\t".join(cols) + "\n")
        for x in rows:
            f.write("\t".join(x.get(c, "") for c in cols) + "\n")
    os.replace(tsv + ".tmp", tsv)
    print(json.dumps({k: row[k] for k in (
        "id", "exit", "rate", "secs", "cpu_cores", "peak_mb", "reruns", "tasks_retried", "kill",
        "check", "groups", "head_mb", "head_objects_max", "leftover", "load_before")}))
    if a.keep and a.mode == "run":
        subprocess.run(["rm", "-rf", a.keep], check=True)
        os.rename(out, a.keep)
    # Ray's session directories are this run's; the next run starts its own.
    for d in glob.glob(os.path.join(a.ray_tmp, "session_*")):
        subprocess.run(["rm", "-rf", d])
    if os.path.islink(os.path.join(a.ray_tmp, "session_latest")):
        os.remove(os.path.join(a.ray_tmp, "session_latest"))
    sys.exit(0 if proc.returncode == 0 and not check.startswith("FAILED") else 1)


if __name__ == "__main__":
    main()
