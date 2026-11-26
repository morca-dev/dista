"""Streamed results: a task that hands over each object as it makes it.

```mojo
def read_lines(path: String, mut out: Emit[RecordBatch]) raises:
    for batch in batches_of(path):
        out.put(batch)

comptime ReadLines = TaskStream[String, RecordBatch, read_lines]

var s = rt.stream[ReadLines](path)
while True:
    var r = rt.next(s)
    if not r:
        break  # the task ended; an error raises here
    use(rt.take(r.value()))
```

Each `put` is an object of its own, stored, fetched, released and given to
other tasks as any task's result is. A stream's ID is an object ID from its
driver, and item `k` is that ID plus 1 plus `k`: the driver sets aside the
`ITEMS_PER_STREAM` IDs after the stream's, so nobody is asked for IDs while
the task runs. The stream's own object is its end: when the task returns,
its worker answers it with the number of items, and when the task raises,
with the error. So `next` waits for item `k` or the end, whichever settles
first, and the items made before an error all come before it.

The worker decides how `put` hands an item over (`StreamSink`). With one
slot the task runs on the worker's loop thread, and `put` stores and reports
the item itself. With several it runs on a slot's thread, and `put` leaves
the item in that slot's mailbox and wakes the loop, which stores and reports
it, since only the loop thread may. The task's function sees one `put`.

The window is carried on the wire but not enforced yet: `put` never blocks.
"""

from std.reflection import reflect_fn

from dista.codec import decode, encode
from dista.task import TaskLike, fnv1a64

comptime ITEMS_PER_STREAM = 1 << 20
"""The most items one stream may put: the IDs its driver sets aside after the
stream's own, as many as a `TaskMany` may have results."""

comptime ItemFn = def(Int, Int, UInt64, UInt64, var List[UInt8]) thin raises
"""Hands one encoded item to the worker: the sink's target and slot, the
stream's ID, the item's ID and its bytes."""


def _no_stream(
    target: Int, slot: Int, stream: UInt64, id: UInt64, var bytes: List[UInt8]
) raises:
    raise Error("this task is not running as a stream")


struct StreamSink(Copyable, Movable):
    """Where a running stream's items go, as its worker set it up: `hand`
    called with `target` and `slot`, which only `hand` knows how to read."""

    var target: Int
    """The address of what `hand` writes to: the worker itself with one
    slot, or the executors' shared state with several."""
    var slot: Int
    """The slot the task runs on; 0 with one slot."""
    var stream: UInt64
    """The stream's own object ID; 0 when no stream is running."""
    var hand: ItemFn

    def __init__(
        out self,
        target: Int = 0,
        slot: Int = 0,
        stream: UInt64 = 0,
        hand: ItemFn = _no_stream,
    ):
        self.target = target
        self.slot = slot
        self.stream = stream
        self.hand = hand


struct Emit[X: Movable & Deinitable](Movable):
    """What a `TaskStream`'s function puts its items through."""

    var _sink: StreamSink
    var _count: Int

    def __init__(out self, var sink: StreamSink):
        self._sink = sink^
        self._count = 0

    def put(mut self, x: Self.X) raises:
        """Hands `x` over as the stream's next item, an object of its own.

        It raises once the stream has put `ITEMS_PER_STREAM` items, which
        fails the task.
        """
        if self._count >= ITEMS_PER_STREAM:
            raise Error(
                "a stream can put at most ",
                ITEMS_PER_STREAM,
                " items (2^20), and this one has put them all",
            )
        var bytes = List[UInt8]()
        encode(x, bytes)
        var id = self._sink.stream + 1 + UInt64(self._count)
        self._sink.hand(
            self._sink.target, self._sink.slot, self._sink.stream, id, bytes^
        )
        self._count += 1

    def count(self) -> Int:
        """How many items it has put so far."""
        return self._count


trait StreamLike(TaskLike):
    """A task whose results are a stream of objects (`TaskStream`)."""

    comptime Item: Movable
    """The type of each item."""

    @staticmethod
    def run_stream(
        args: Span[UInt8, _], mut out: List[UInt8], mut sink: StreamSink
    ) raises:
        """Decodes the argument and runs the function, each item going to
        `sink`, then appends the encoded number of items: the end's value.
        """
        ...


struct TaskStream[
    A_: Movable & Deinitable,
    X: Movable & Deinitable,
    f: def(A_, mut Emit[X]) thin raises,
](StreamLike):
    """A remote function that hands over its results one at a time as it
    makes them: `rt.stream[T](a)` returns a `StreamRef[X]`, and `rt.next`
    reads it. See the module docstring."""

    comptime A = Self.A_
    comptime R = Int
    """The end's value: how many items the task put."""
    comptime Item = Self.X

    @staticmethod
    def name() -> StaticString:
        return reflect_fn[Self.f].linkage_name()

    @staticmethod
    def id() -> UInt64:
        return fnv1a64(Self.name())

    @staticmethod
    def is_stream() -> Bool:
        return True

    @staticmethod
    def run(args: Span[UInt8, _], mut out: List[UInt8]) raises:
        raise Error(Self.name(), " is a stream: run it through run_stream")

    @staticmethod
    def run_stream(
        args: Span[UInt8, _], mut out: List[UInt8], mut sink: StreamSink
    ) raises:
        var pos = 0
        var a = decode[Self.A](args, pos)
        if pos != len(args):
            raise Error(
                Self.name(), ": ", len(args) - pos, " argument bytes left over"
            )
        var emit = Emit[Self.X](sink.copy())
        Self.f(a, emit)
        encode(emit.count(), out)


struct StreamRef[T: AnyType](Copyable, Movable, Writable):
    """A handle to a running stream of `T`s, from `rt.stream`: `rt.next`
    reads its items in order."""

    var id: UInt64
    """The stream's own object, its end: settled once the task has ended."""
    var _taken: Int
    """How many items `next` has returned."""
    var _ended: Bool
    """`next` has read the end."""

    def __init__(out self, id: UInt64):
        self.id = id
        self._taken = 0
        self._ended = False

    def next_id(self) -> UInt64:
        """The object ID of the item `next` returns next: with `s.id`, what
        `rt.wait` takes to wait on several streams."""
        return self.id + 1 + UInt64(self._taken)

    def write_to(self, mut writer: Some[Writer]):
        writer.write("StreamRef(", self.id, ")")
