"""Flows: a chain of operators over batches, run by an executor in the driver.

```mojo
from dista import Runtime, Task, TaskStream
from dista.flow import Flow, Map, Read

comptime ScanFile = TaskStream[String, RecordBatch, scan_file]  # batch by batch
comptime Tokenize = Task[RecordBatch, RecordBatch, tokenize]
comptime Append = Task[RecordBatch, String, append_file]  # the file it wrote

comptime Embed = Flow[Read[ScanFile], Map[Tokenize], Map[Append]]

var rt = Runtime[ScanFile, Tokenize, Append](workers=6)
var files = Embed.run(rt, paths)  # every file written, by input
```

Each input is read by one stream, the `Read`, and each of its items goes
through every `Map` in turn, as a task on the result of the one before. A
`MapWith` is a `Map` whose task is a `StatefulTask`, whose state each worker
builds once, such as a model; a `Map` refuses one at `start`. The
last operator's results are the flow's. `run` returns them all as values,
by input and then in the read's order. `start` returns a `FlowRun`, whose
`next` returns one finished input at a time: its position in the inputs and
the refs of its results, in the read's order.

The executor keeps only object IDs, and runs inside `run` and `next`, on the
driver's thread. Each `Map` has a budget: its tasks in flight plus its
results the next operator hasn't taken. It submits downstream first, and
waits with `rt.wait` for any task in flight or any stream's next item or
end. It reads a stream's next item only when the first `Map` has room, so a
full operator stops the one before it, back to the read, whose streams then
stop at their window. A `Read` runs `streams` inputs at once, so the
objects in flight stay within `streams * window` plus the budgets, besides
the results held for inputs not yet finished. An input's ref is released as
soon as the task given it is submitted.

An error a task or a stream raises fails the flow: everything in flight is
cancelled, everything held is released, and `next` raises the error with
the operator and the input it came from. A loss dista reports instead (a
result lost with its worker, a stream whose worker went away after an item,
a stream a head restart interrupted: see `dista.loss`) reads the input
again: everything made from it is cancelled and released, and its stream
starts over, up to `retries` times an input. An input is handed out only
once its last attempt is through, so the results stay exact. `stats` counts
what each operator has done. See DESIGN.md, "Flows".
"""

from std.reflection import reflect

from dista.client import Client, ObjectRef
from dista.loss import is_loss
from dista.runtime import Runtime
from dista.stream import StreamLike, StreamRef
from dista.task import StatefulLike, TaskLike


trait OpLike:
    """An operator of a flow: a task, the type it takes and the type it
    makes, and how much it may hold."""

    comptime In: Movable & Deinitable
    """What it takes: a `Read`'s input, or the batch a `Map` is given."""
    comptime Out: Movable & Deinitable
    """What it makes: a `Read`'s items, or a `Map`'s results."""
    comptime IS_READ: Bool
    """Whether it's a `Read`, which only the first operator is."""
    comptime KIND: StaticString
    """Its kind as an error names it: "Read", "Map" or "MapWith"."""
    comptime BUDGET: Int
    """A `Map`'s budget, or a `Read`'s streams at once."""
    comptime WINDOW: Int
    """A `Read`'s window; 0 for a `Map`."""
    comptime Task: TaskLike
    """Its task."""

    @staticmethod
    def open(mut c: Client, arg: Self.In) raises -> UInt64:
        """A `Read`: starts a stream on `arg` and returns its ID."""
        ...

    @staticmethod
    def submit(mut c: Client, input: UInt64) raises -> UInt64:
        """A `Map`: submits its task on object `input` and returns the
        result's ID."""
        ...


struct Read[T: StreamLike, streams: Int = 4, window: Int = 16](OpLike):
    """The first operator: a `TaskStream` run on each input, `streams`
    inputs at once, each with this `window`."""

    comptime In = Self.T.A
    comptime Out = Self.T.Item
    comptime IS_READ = True
    comptime KIND = "Read"
    comptime BUDGET = Self.streams
    comptime WINDOW = Self.window
    comptime Task = Self.T

    @staticmethod
    def open(mut c: Client, arg: Self.In) raises -> UInt64:
        return c.stream[Self.T](arg, Self.window).id

    @staticmethod
    def submit(mut c: Client, input: UInt64) raises -> UInt64:
        raise Error("a Read takes no batch")


struct Map[T: TaskLike, budget: Int = 16](OpLike):
    """An operator after the first: a `Task` run on each batch, with at most
    `budget` tasks in flight and results not yet taken by the next
    operator."""

    comptime In = Self.T.A
    comptime Out = Self.T.R
    comptime IS_READ = False
    comptime KIND = "Map"
    comptime BUDGET = Self.budget
    comptime WINDOW = 0
    comptime Task = Self.T

    @staticmethod
    def open(mut c: Client, arg: Self.In) raises -> UInt64:
        raise Error("a Map reads no input")

    @staticmethod
    def submit(mut c: Client, input: UInt64) raises -> UInt64:
        return c.remote[Self.T](ObjectRef[Self.T.A](input)).id


struct MapWith[T: StatefulLike, budget: Int = 16](OpLike):
    """A `Map` whose task is a `StatefulTask`: each worker that runs it
    builds the task's state once and keeps it for every batch it is given.
    In the executor it is a `Map`. A plain `Task` doesn't compile here."""

    comptime In = Self.T.A
    comptime Out = Self.T.R
    comptime IS_READ = False
    comptime KIND = "MapWith"
    comptime BUDGET = Self.budget
    comptime WINDOW = 0
    comptime Task = Self.T

    @staticmethod
    def open(mut c: Client, arg: Self.In) raises -> UInt64:
        raise Error("a MapWith reads no input")

    @staticmethod
    def submit(mut c: Client, input: UInt64) raises -> UInt64:
        return c.remote[Self.T](ObjectRef[Self.T.A](input)).id


struct FlowResult[T: AnyType](Movable):
    """One finished input, from `FlowRun.next`."""

    var input: Int
    """Its position in the inputs list."""
    var refs: List[ObjectRef[Self.T]]
    """The refs of its results, in the read's order; the caller's to take or
    release."""

    def __init__(out self, input: Int, var refs: List[ObjectRef[Self.T]]):
        self.input = input
        self.refs = refs^

    def take_refs(deinit self) -> List[ObjectRef[Self.T]]:
        """Its refs, moved out."""
        return self.refs^


@fieldwise_init
struct _Batch(ImplicitlyCopyable, Movable):
    """A batch in flight: its input, its item's position in its input's
    stream, and its object."""

    var input: Int
    var item: Int
    var id: UInt64


struct Flow[*Ops: OpLike]:
    """A chain of operators: a `Read`, then `Map`s and `MapWith`s, each
    taking what the one before it makes. See the module docstring."""

    comptime Out = Self.Ops[Self.Ops.length - 1].Out
    """What the flow returns: the last operator's results."""

    @staticmethod
    def _check():
        comptime assert Self.Ops.length >= 1, "a flow needs an operator"
        comptime assert Self.Ops[0].IS_READ, "a flow's first operator is a Read"
        comptime for i in range(Self.Ops.length):
            comptime assert (
                Self.Ops[i].BUDGET >= 1
            ), "an operator's budget, or a Read's streams, must be at least 1"
            comptime if i > 0:
                comptime assert not Self.Ops[
                    i
                ].IS_READ, "only a flow's first operator is a Read"
                comptime assert (
                    reflect[Self.Ops[i].In].name()
                    == reflect[Self.Ops[i - 1].Out].name()
                ), "an operator's input must be the output of the one before it"

    @staticmethod
    def start[
        *Ts: TaskLike
    ](
        mut rt: Runtime[*Ts], var inputs: List[Self.Ops[0].In], retries: Int = 3
    ) raises -> FlowRun[*Self.Ops]:
        """Starts the flow on `inputs`, the read's arguments, and returns the
        run to read with `next`. An input dista loses part of is read again
        up to `retries` times. It raises if an operator's task isn't
        registered with `rt`, or a `Map`'s isn't a `Task` of one argument,
        or is a `StatefulTask`, which goes in a `MapWith`.
        """
        Self._check()
        if retries < 0:
            raise Error("a flow's retries can't be negative, got ", retries)
        comptime for i in range(Self.Ops.length):
            comptime T = Self.Ops[i].Task
            rt._check_registered[T]()
            if i > 0 and (
                T.is_stream() or T.is_many() or T.is_actor() or T.arity() != 1
            ):
                raise Error(T.name(), ": a Map takes a Task of one argument")
            comptime if Self.Ops[i].KIND == "Map" and conforms_to(
                T, StatefulLike
            ):
                raise Error(T.name(), ": a StatefulTask goes in a MapWith")
        var run = FlowRun[*Self.Ops](inputs^, retries)
        return run^

    @staticmethod
    def run[
        *Ts: TaskLike
    ](
        mut rt: Runtime[*Ts], var inputs: List[Self.Ops[0].In], retries: Int = 3
    ) raises -> List[Self.Out]:
        """Runs the flow on `inputs` to the end and returns every result as
        a value: by input, then in the read's order. Each input's results
        are taken as it is handed out, so a worker lost later can't lose
        them."""
        var n = len(inputs)
        var run = Self.start(rt, inputs^, retries)
        var by_input = List[List[Self.Out]](capacity=n)
        for _ in range(n):
            by_input.append(List[Self.Out]())
        while True:
            var r = run.next(rt)
            if not r:
                break
            var done = r.take()
            var at = done.input
            var refs = done^.take_refs()
            try:
                for k in range(len(refs)):
                    by_input[at].append(rt.take(refs[k]))
            except e:
                for k in range(len(by_input[at]), len(refs)):
                    rt.release(refs[k])
                run._fail(rt.client(), String(e))
                raise e^
        var out = List[Self.Out]()
        for k in range(n):
            var bucket = List[Self.Out]()
            swap(bucket, by_input[k])
            out.extend(bucket^)
        return out^


@fieldwise_init
struct OpStats(ImplicitlyCopyable, Movable, Writable):
    """What an operator has done, from `FlowRun.stats`. For a `Read`, a
    task is a stream and a finished task an item read."""

    var submitted: Int
    """Tasks submitted, or a `Read`'s streams opened."""
    var finished: Int
    """Tasks that finished, or a `Read`'s items read, those of an attempt
    given up since included."""
    var in_flight: Int
    """Tasks running, or a `Read`'s streams being read."""
    var reruns: Int
    """Inputs read again after a loss seen at this operator."""

    def write_to(self, mut writer: Some[Writer]):
        writer.write(
            "submitted ",
            self.submitted,
            ", finished ",
            self.finished,
            ", in flight ",
            self.in_flight,
            ", reruns ",
            self.reruns,
        )


struct FlowStats(Copyable, Movable, Writable):
    """A flow's counters, from `FlowRun.stats`."""

    var ops: List[OpStats]
    """Per operator, the read first."""
    var inputs_done: Int
    """Inputs handed out by `next`."""

    def __init__(out self, var ops: List[OpStats], inputs_done: Int):
        self.ops = ops^
        self.inputs_done = inputs_done

    def reruns(self) -> Int:
        """Inputs read again, at every operator."""
        var n = 0
        for o in self.ops:
            n += o.reruns
        return n

    def write_to(self, mut writer: Some[Writer]):
        writer.write("inputs done ", self.inputs_done)
        for k in range(len(self.ops)):
            writer.write("; operator ", k, ": ", self.ops[k])


struct FlowRun[*Ops: OpLike](Movable):
    """A flow running, from `Flow.start`: `next` returns its inputs as they
    finish. Nothing moves between two calls but the tasks already sent.

    Read it to the end, or until it raises: a run dropped before then keeps
    what it holds until the runtime closes.
    """

    comptime N = Self.Ops.length
    comptime Out = Self.Ops[Self.Ops.length - 1].Out

    var _inputs: List[Self.Ops[0].In]
    var _opened: Int
    """How many inputs have had their first stream started."""
    var _again: List[Int]
    """Inputs to read again after a loss, before any new one."""
    var _retries: Int
    """How many times an input may be read again."""
    var _attempts: List[Int]
    """Per input, how many times it has been read again."""
    var _streams: List[StreamRef[Self.Ops[0].Out]]
    """The streams being read, at most the read's `streams`."""
    var _stream_input: List[Int]
    """Each stream's input."""
    var _running: List[List[_Batch]]
    """Per operator, its tasks in flight; empty for the read."""
    var _ready: List[List[_Batch]]
    """Per operator, its results the next one hasn't taken."""
    var _results: List[List[UInt64]]
    """Per input, its results from the last operator, by item."""
    var _items: List[Int]
    """Per input, how many items its stream put; -1 until it has ended."""
    var _through: List[Int]
    """Per input, how many of its batches the last operator has finished."""
    var _finished: List[Int]
    """Finished inputs, in the order they finished."""
    var _handed: Int
    """How many of `_finished` `next` has returned."""
    var _dropped: List[StreamRef[Self.Ops[0].Out]]
    """Streams of an attempt given up, cancelled and read to their end."""
    var _abandoned: List[UInt64]
    """Tasks of an attempt given up, cancelled and released once they
    settle. Nothing else refers to them, so the next attempt's tasks can't
    be taken for them."""
    var _submitted: List[Int]
    var _done: List[Int]
    var _reruns: List[Int]
    """Per operator, the counters `stats` gives."""
    var _failure: String
    """Why the flow failed, once it has; "" until then."""

    def __init__(out self, var inputs: List[Self.Ops[0].In], retries: Int):
        var n = len(inputs)
        self._inputs = inputs^
        self._opened = 0
        self._again = List[Int]()
        self._retries = retries
        self._attempts = List[Int](length=n, fill=0)
        self._streams = List[StreamRef[Self.Ops[0].Out]]()
        self._stream_input = List[Int]()
        self._running = List[List[_Batch]]()
        self._ready = List[List[_Batch]]()
        for _ in range(Self.N):
            self._running.append(List[_Batch]())
            self._ready.append(List[_Batch]())
        self._results = List[List[UInt64]](capacity=n)
        for _ in range(n):
            self._results.append(List[UInt64]())
        self._items = List[Int](length=n, fill=-1)
        self._through = List[Int](length=n, fill=0)
        self._finished = List[Int]()
        self._handed = 0
        self._dropped = List[StreamRef[Self.Ops[0].Out]]()
        self._abandoned = List[UInt64]()
        self._submitted = List[Int](length=Self.N, fill=0)
        self._done = List[Int](length=Self.N, fill=0)
        self._reruns = List[Int](length=Self.N, fill=0)
        self._failure = String()

    def next[
        *Ts: TaskLike
    ](mut self, mut rt: Runtime[*Ts]) raises -> Optional[FlowResult[Self.Out]]:
        """Runs the flow until an input has finished, and returns it, or None
        once every input has been returned.

        An error a task or a stream raised fails the flow: what's in flight
        is cancelled, what's held is released, and this raises the error
        with the operator and the input it came from, as it does on every
        call after. So does a loss on an input already read again `retries`
        times.
        """
        if self._failure != "":
            raise Error(self._failure)
        ref c = rt.client()
        var input: Int
        try:
            while True:
                while self._handed == len(self._finished):
                    if self._handed == len(self._inputs):
                        self._settle_abandoned(c)
                        return None
                    self._advance(c)
                input = self._finished[self._handed]
                # A result held for the input may have been lost since it
                # came: the input is read again rather than handed out.
                var lost = String()
                for id in self._results[input]:
                    var err = c.error_of(id)
                    if err:
                        lost = err.value()
                        break
                if lost == "":
                    break
                _ = self._finished.pop(self._handed)
                if not is_loss(lost):
                    raise Error(
                        Self._describe(Self.N - 1),
                        " failed on input ",
                        input,
                        ": ",
                        lost,
                    )
                self._rerun(c, input, Self.N - 1, lost)
        except e:
            self._fail(c, String(e))
            raise e^
        self._handed += 1
        var ids = self._results[input].copy()
        self._results[input].clear()
        var refs = List[ObjectRef[Self.Out]](capacity=len(ids))
        for id in ids:
            refs.append(ObjectRef[Self.Out](id))
        return FlowResult[Self.Out](input, refs^)

    def stats(self) -> FlowStats:
        """What each operator has done so far, and how many inputs `next`
        has handed out."""
        var ops = List[OpStats](capacity=Self.N)
        for k in range(Self.N):
            var in_flight = len(self._running[k])
            if k == 0:
                in_flight = len(self._streams)
            ops.append(
                OpStats(
                    self._submitted[k],
                    self._done[k],
                    in_flight,
                    self._reruns[k],
                )
            )
        return FlowStats(ops^, self._handed)

    # ── the executor ─────────────────────────────────────────────────────

    @staticmethod
    def _submit(mut c: Client, op: Int, input: UInt64) raises -> UInt64:
        comptime for i in range(1, Self.N):
            if op == i:
                return Self.Ops[i].submit(c, input)
        raise Error("no operator ", op)

    @staticmethod
    def _budget(op: Int) -> Int:
        comptime for i in range(Self.N):
            if op == i:
                return Self.Ops[i].BUDGET
        return 0

    @staticmethod
    def _describe(op: Int) -> String:
        """The operator as an error names it: its position, kind and task."""
        comptime for i in range(Self.N):
            if op == i:
                var kind = Self.Ops[i].KIND
                # The task's linkage name, without its argument types.
                var full = Self.Ops[i].Task.name()
                var paren = full.find("(").or_else(-1)
                var name = String(full[byte=:paren]) if paren > 0 else String(
                    full
                )
                return String("operator ", i, " (", kind, " ", name, ")")
        return String("operator ", op)

    def _room(self, op: Int) -> Int:
        """How many more batches operator `op` may be given."""
        if op >= Self.N:
            return Int.MAX  # the flow's results: held per input
        var held = len(self._running[op])
        if op < Self.N - 1:
            held += len(self._ready[op])
        return Self._budget(op) - held

    def _finish_one(mut self, input: Int):
        """A batch of `input` is through the last operator, or the input's
        stream has ended: hands the input out if both are done."""
        if (
            self._items[input] >= 0
            and self._through[input] == self._items[input]
        ):
            self._finished.append(input)

    def _record(mut self, b: _Batch):
        """`b` is a result of the last operator."""
        self._results[b.input][b.item] = b.id
        self._through[b.input] += 1
        self._finish_one(b.input)

    def _open(mut self, mut c: Client, input: Int) raises:
        """Starts a stream reading `input`."""
        var id = Self.Ops[0].open(c, self._inputs[input])
        self._streams.append(StreamRef[Self.Ops[0].Out](id, Self.Ops[0].WINDOW))
        self._stream_input.append(input)
        self._submitted[0] += 1

    def _advance(mut self, mut c: Client) raises:
        """One step: submits what has room, downstream first, reads the
        streams' settled items into the first `Map`, starts streams for
        inputs to read again and then new ones, and waits for anything in
        flight to finish. A loss reads its input again."""
        self._sweep(c)
        var op = Self.N - 1
        while op >= 2:
            while self._room(op) > 0 and len(self._ready[op - 1]) > 0:
                var b = self._ready[op - 1].pop(0)
                var id = Self._submit(c, op, b.id)
                c.release(ObjectRef[NoneType](b.id))
                self._running[op].append(_Batch(b.input, b.item, id))
                self._submitted[op] += 1
            op -= 1
        self._read(c)
        while len(self._streams) < Self.Ops[0].BUDGET and (
            len(self._again) > 0 or self._opened < len(self._inputs)
        ):
            if len(self._again) > 0:
                self._open(c, self._again.pop(0))
            else:
                self._opened += 1
                self._open(c, self._opened - 1)
        if self._handed < len(self._finished):
            return
        var ids = List[UInt64]()
        for k in range(1, Self.N):
            for b in self._running[k]:
                ids.append(b.id)
        if self._room(1) > 0:
            for s in self._streams:
                ids.append(s.next_id())
                ids.append(s.id)
        for id in self._abandoned:
            ids.append(id)
        for s in self._dropped:
            ids.append(s.next_id())
            ids.append(s.id)
        if len(ids) == 0:
            raise Error("a flow has nothing in flight and inputs not done")
        var done = Dict[UInt64, Bool]()
        for p in c.wait(ids):
            done[ids[p]] = True
        # An error fails the flow before anything moves on; a loss reads
        # its input again.
        var lost_input = List[Int]()
        var lost_op = List[Int]()
        var lost_why = List[String]()
        for k in range(1, Self.N):
            for b in self._running[k]:
                if b.id in done:
                    var err = c.error_of(b.id)
                    if not err:
                        continue
                    if not is_loss(err.value()):
                        raise Error(
                            Self._describe(k),
                            " failed on input ",
                            b.input,
                            ": ",
                            err.value(),
                        )
                    if b.input not in lost_input:
                        lost_input.append(b.input)
                        lost_op.append(k)
                        lost_why.append(err.value())
        for j in range(len(lost_input)):
            self._rerun(c, lost_input[j], lost_op[j], lost_why[j])
        for k in range(1, Self.N):
            var still = List[_Batch]()
            for i in range(len(self._running[k])):
                var b = self._running[k][i]
                if b.id not in done:
                    still.append(b)
                    continue
                self._done[k] += 1
                if k == Self.N - 1:
                    self._record(b)
                else:
                    self._ready[k].append(b)
            self._running[k] = still^

    def _read(mut self, mut c: Client) raises:
        """Takes the streams' settled items while the first `Map` has room,
        each submitted to it at once, and drops the streams that ended. A
        stream's loss reads its input again."""
        var k = 0
        while k < len(self._streams):
            var input = self._stream_input[k]
            var ended = False
            var lost = String()
            while self._room(1) > 0:
                ref s = self._streams[k]
                var probe: List[UInt64] = [s.next_id(), s.id]
                if len(c.wait(probe, 1, 0.0)) == 0:
                    break
                var item = len(self._results[input])
                var r: Optional[ObjectRef[Self.Ops[0].Out]]
                try:
                    r = c.next(s)
                except e:
                    if not is_loss(String(e)):
                        raise Error(
                            Self._describe(0),
                            " failed on input ",
                            input,
                            ": ",
                            e,
                        )
                    lost = String(e)
                    break
                if not r:
                    self._items[input] = item
                    self._finish_one(input)
                    ended = True
                    break
                var id = r.value().id
                self._done[0] += 1
                # The slot holds the item until the last operator's result.
                self._results[input].append(0)
                var b = _Batch(input, item, id)
                comptime if Self.N == 1:
                    self._record(b)
                else:
                    var t = Self._submit(c, 1, id)
                    c.release(ObjectRef[NoneType](id))
                    self._running[1].append(_Batch(input, item, t))
                    self._submitted[1] += 1
            if lost != "":
                # Takes the stream out of the list: `k` is the next one now.
                self._rerun(c, input, 0, lost)
            elif ended:
                _ = self._streams.pop(k)
                _ = self._stream_input.pop(k)
            else:
                k += 1

    def _rerun(
        mut self, mut c: Client, input: Int, op: Int, why: String
    ) raises:
        """A loss at operator `op` on `input`: cancels and lets go of
        everything made from it, and reads it again, or raises once it has
        been read again `retries` times."""
        if self._attempts[input] >= self._retries:
            raise Error(
                Self._describe(op),
                " failed on input ",
                input,
                " after ",
                self._attempts[input] + 1,
                " attempts: ",
                why,
            )
        self._attempts[input] += 1
        self._reruns[op] += 1
        var k = 0
        while k < len(self._streams):
            if self._stream_input[k] != input:
                k += 1
                continue
            var s = self._streams.pop(k)
            _ = self._stream_input.pop(k)
            if not s._ended:  # a stream whose end failed has nothing left
                c.cancel(s)
                self._dropped.append(s^)
        for j in range(1, Self.N):
            var still = List[_Batch]()
            for b in self._running[j]:
                if b.input != input:
                    still.append(b)
                    continue
                c.cancel(ObjectRef[NoneType](b.id))
                self._abandoned.append(b.id)
            self._running[j] = still^
            var kept = List[_Batch]()
            for b in self._ready[j]:
                if b.input == input:
                    c.release(ObjectRef[NoneType](b.id))
                else:
                    kept.append(b)
            self._ready[j] = kept^
        for id in self._results[input]:
            if id != 0:
                c.release(ObjectRef[NoneType](id))
        self._results[input].clear()
        self._items[input] = -1
        self._through[input] = 0
        self._again.append(input)

    def _sweep(mut self, mut c: Client) raises:
        """Releases the abandoned tasks that have settled, and the items of
        dropped streams that have come, without waiting."""
        if len(self._abandoned) > 0:
            var settled = c.wait(self._abandoned, len(self._abandoned), 0.0)
            var gone = Dict[Int, Bool]()
            for p in settled:
                gone[p] = True
                c.release(ObjectRef[NoneType](self._abandoned[p]))
            var still = List[UInt64]()
            for p in range(len(self._abandoned)):
                if p not in gone:
                    still.append(self._abandoned[p])
            self._abandoned = still^
        var k = 0
        while k < len(self._dropped):
            if Self._drain(c, self._dropped[k], 0.0):
                _ = self._dropped.pop(k)
            else:
                k += 1

    @staticmethod
    def _drain(
        mut c: Client, mut s: StreamRef[Self.Ops[0].Out], timeout: Float64
    ) -> Bool:
        """Releases cancelled stream `s`'s items as they come, and returns
        whether it has ended: at once with `timeout` 0, or once it has with
        a negative one."""
        try:
            while not s._ended:
                if timeout >= 0.0:
                    var probe: List[UInt64] = [s.next_id(), s.id]
                    if len(c.wait(probe, 1, timeout)) == 0:
                        return False
                var r = c.next(s)
                if not r:
                    break
                c.release(r.value())
        except:
            pass  # its end: "cancelled", or the loss
        return True

    def _settle_abandoned(mut self, mut c: Client) raises:
        """Once every input is out: waits for the abandoned tasks and the
        dropped streams to settle, and releases them, so the run leaves
        nothing behind."""
        if len(self._abandoned) > 0:
            _ = c.wait(self._abandoned, len(self._abandoned))
        for id in self._abandoned:
            c.release(ObjectRef[NoneType](id))
        self._abandoned.clear()
        for k in range(len(self._dropped)):
            _ = Self._drain(c, self._dropped[k], -1.0)
        self._dropped.clear()

    def _fail(mut self, mut c: Client, why: String):
        """Fails the run with `why`: every later `next` raises it, and
        everything in flight is cancelled and released."""
        self._failure = why
        self._abandon(c)

    def _abandon(mut self, mut c: Client):
        """After a failure: cancels the streams and the tasks in flight,
        waits for them to settle, and releases everything held."""
        for s in self._streams:
            try:
                c.cancel(s)
            except:
                pass
        var ids = self._abandoned.copy()
        self._abandoned.clear()
        for k in range(1, Self.N):
            for b in self._running[k]:
                ids.append(b.id)
                try:
                    c.cancel(ObjectRef[NoneType](b.id))
                except:
                    pass
        # A cancelled stream's items answered before the cancel still come,
        # and then its end, "cancelled".
        for k in range(len(self._streams)):
            _ = Self._drain(c, self._streams[k], -1.0)
        for k in range(len(self._dropped)):
            _ = Self._drain(c, self._dropped[k], -1.0)
        try:
            if len(ids) > 0:
                _ = c.wait(ids, len(ids))
        except:
            pass
        for id in ids:
            try:
                c.release(ObjectRef[NoneType](id))
            except:
                pass
        for k in range(Self.N):
            for b in self._ready[k]:
                try:
                    c.release(ObjectRef[NoneType](b.id))
                except:
                    pass
        # Every input not yet handed out: its results so far, and a
        # finished one's all.
        for k in range(len(self._results)):
            for id in self._results[k]:
                if id != 0:
                    try:
                        c.release(ObjectRef[NoneType](id))
                    except:
                        pass
        self._streams.clear()
        self._stream_input.clear()
        self._dropped.clear()
        for k in range(Self.N):
            self._running[k].clear()
            self._ready[k].clear()
        for k in range(len(self._results)):
            self._results[k].clear()
