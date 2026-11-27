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
through every `Map` in turn, as a task on the result of the one before. The
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
the operator and the input it came from. See DESIGN.md, "Flows".
"""

from std.reflection import reflect

from dista.client import Client, ObjectRef
from dista.runtime import Runtime
from dista.stream import StreamLike, StreamRef
from dista.task import TaskLike


trait OpLike:
    """An operator of a flow: a task, the type it takes and the type it
    makes, and how much it may hold."""

    comptime In: Movable & Deinitable
    """What it takes: a `Read`'s input, or the batch a `Map` is given."""
    comptime Out: Movable & Deinitable
    """What it makes: a `Read`'s items, or a `Map`'s results."""
    comptime IS_READ: Bool
    """Whether it's a `Read`, which only the first operator is."""
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
    comptime BUDGET = Self.budget
    comptime WINDOW = 0
    comptime Task = Self.T

    @staticmethod
    def open(mut c: Client, arg: Self.In) raises -> UInt64:
        raise Error("a Map reads no input")

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
    """A chain of operators: a `Read`, then `Map`s, each taking what the one
    before it makes. See the module docstring."""

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
    ](mut rt: Runtime[*Ts], var inputs: List[Self.Ops[0].In]) raises -> FlowRun[
        *Self.Ops
    ]:
        """Starts the flow on `inputs`, the read's arguments, and returns the
        run to read with `next`. It raises if an operator's task isn't
        registered with `rt`, or a `Map`'s isn't a `Task` of one argument.
        """
        Self._check()
        comptime for i in range(Self.Ops.length):
            comptime T = Self.Ops[i].Task
            rt._check_registered[T]()
            if i > 0 and (
                T.is_stream() or T.is_many() or T.is_actor() or T.arity() != 1
            ):
                raise Error(T.name(), ": a Map takes a Task of one argument")
        var run = FlowRun[*Self.Ops](inputs^)
        return run^

    @staticmethod
    def run[
        *Ts: TaskLike
    ](mut rt: Runtime[*Ts], var inputs: List[Self.Ops[0].In]) raises -> List[
        Self.Out
    ]:
        """Runs the flow on `inputs` to the end and returns every result as
        a value: by input, then in the read's order."""
        var n = len(inputs)
        var run = Self.start(rt, inputs^)
        var by_input = List[List[ObjectRef[Self.Out]]](capacity=n)
        for _ in range(n):
            by_input.append(List[ObjectRef[Self.Out]]())
        try:
            while True:
                var r = run.next(rt)
                if not r:
                    break
                var done = r.take()
                var at = done.input
                by_input[at] = done^.take_refs()
        except e:
            # The run released what it held; the inputs it handed out are
            # here.
            for k in range(n):
                rt.release(by_input[k])
            raise e^
        var out = List[Self.Out]()
        for k in range(n):
            for r in by_input[k]:
                out.append(rt.take(r))
        return out^


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
    """How many inputs have had their stream started."""
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
    var _failure: String
    """Why the flow failed, once it has; "" until then."""

    def __init__(out self, var inputs: List[Self.Ops[0].In]):
        var n = len(inputs)
        self._inputs = inputs^
        self._opened = 0
        self._streams = List[StreamRef[Self.Ops[0].Out]]()
        self._stream_input = List[Int]()
        self._running = List[List[_Batch]]()
        self._ready = List[List[_Batch]]()
        for _ in range(Self.N):
            self._running.append(List[_Batch]())
            self._ready.append(List[_Batch]())
        self._results = List[List[UInt64]](capacity=n)
        self._items = List[Int](capacity=n)
        self._through = List[Int](capacity=n)
        for _ in range(n):
            self._results.append(List[UInt64]())
            self._items.append(-1)
            self._through.append(0)
        self._finished = List[Int]()
        self._handed = 0
        self._failure = String()

    def next[
        *Ts: TaskLike
    ](mut self, mut rt: Runtime[*Ts]) raises -> Optional[FlowResult[Self.Out]]:
        """Runs the flow until an input has finished, and returns it, or None
        once every input has been returned.

        An error a task or a stream raised fails the flow: what's in flight
        is cancelled, what's held is released, and this raises the error
        with the operator and the input it came from, as it does on every
        call after.
        """
        if self._failure != "":
            raise Error(self._failure)
        ref c = rt.client()
        try:
            while self._handed == len(self._finished):
                if self._handed == len(self._inputs):
                    return None
                self._advance(c)
        except e:
            self._failure = String(e)
            self._abandon(c)
            raise e^
        var input = self._finished[self._handed]
        self._handed += 1
        var ids = self._results[input].copy()
        self._results[input].clear()
        var refs = List[ObjectRef[Self.Out]](capacity=len(ids))
        for id in ids:
            refs.append(ObjectRef[Self.Out](id))
        return FlowResult[Self.Out](input, refs^)

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
                var kind = "Read" if Self.Ops[i].IS_READ else "Map"
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

    def _advance(mut self, mut c: Client) raises:
        """One step: submits what has room, downstream first, reads the
        streams' settled items into the first `Map`, starts streams for new
        inputs, and waits for anything in flight to finish."""
        var op = Self.N - 1
        while op >= 2:
            while self._room(op) > 0 and len(self._ready[op - 1]) > 0:
                var b = self._ready[op - 1].pop(0)
                var id = Self._submit(c, op, b.id)
                c.release(ObjectRef[NoneType](b.id))
                self._running[op].append(_Batch(b.input, b.item, id))
            op -= 1
        self._read(c)
        while len(self._streams) < Self.Ops[0].BUDGET and self._opened < len(
            self._inputs
        ):
            var input = self._opened
            self._opened += 1
            var id = Self.Ops[0].open(c, self._inputs[input])
            self._streams.append(
                StreamRef[Self.Ops[0].Out](id, Self.Ops[0].WINDOW)
            )
            self._stream_input.append(input)
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
        if len(ids) == 0:
            raise Error("a flow has nothing in flight and inputs not done")
        var done = Dict[UInt64, Bool]()
        for p in c.wait(ids):
            done[ids[p]] = True
        # An error fails the flow before anything moves on.
        for k in range(1, Self.N):
            for b in self._running[k]:
                if b.id in done:
                    var err = c.error_of(b.id)
                    if err:
                        raise Error(
                            Self._describe(k),
                            " failed on input ",
                            b.input,
                            ": ",
                            err.value(),
                        )
        for k in range(1, Self.N):
            var still = List[_Batch]()
            for i in range(len(self._running[k])):
                var b = self._running[k][i]
                if b.id not in done:
                    still.append(b)
                elif k == Self.N - 1:
                    self._record(b)
                else:
                    self._ready[k].append(b)
            self._running[k] = still^

    def _read(mut self, mut c: Client) raises:
        """Takes the streams' settled items while the first `Map` has room,
        each submitted to it at once, and drops the streams that ended."""
        var k = 0
        while k < len(self._streams):
            var input = self._stream_input[k]
            var ended = False
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
                    raise Error(
                        Self._describe(0), " failed on input ", input, ": ", e
                    )
                if not r:
                    self._items[input] = item
                    self._finish_one(input)
                    ended = True
                    break
                var id = r.value().id
                # The slot holds the item until the last operator's result.
                self._results[input].append(0)
                var b = _Batch(input, item, id)
                comptime if Self.N == 1:
                    self._record(b)
                else:
                    var t = Self._submit(c, 1, id)
                    c.release(ObjectRef[NoneType](id))
                    self._running[1].append(_Batch(input, item, t))
            if ended:
                _ = self._streams.pop(k)
                _ = self._stream_input.pop(k)
            else:
                k += 1

    def _abandon(mut self, mut c: Client):
        """After a failure: cancels the streams and the tasks in flight,
        waits for them to settle, and releases everything held."""
        for s in self._streams:
            try:
                c.cancel(s)
            except:
                pass
        var ids = List[UInt64]()
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
            try:
                while True:
                    var r = c.next(self._streams[k])
                    if not r:
                        break
                    c.release(r.value())
            except:
                pass
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
        for k in range(Self.N):
            self._running[k].clear()
            self._ready[k].clear()
        for k in range(len(self._results)):
            self._results[k].clear()
