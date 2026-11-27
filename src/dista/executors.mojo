"""A worker's task slots: threads that run tasks while its loop keeps serving.

With `DISTA_WORKER_SLOTS` above 1, a worker runs up to that many tasks at
once, each on a thread of its own, while its event loop stays on the main
thread: it keeps reading the head, serving fetches, fetching inputs and
reporting results while tasks run. The executors only run the task: the loop
builds each one's arguments before handing it over, and takes its result
back, storing and reporting it as a worker with one slot does. So nothing an
executor touches is shared with the loop but the job it was handed.

The hand-off is one job per slot under a mutex. The loop fills an idle
slot's job, then marks it started and wakes the executors (a condition
variable); slot `i`'s executor runs it, marks it done and writes a byte to a
pipe the loop polls, which wakes the loop. The loop alone moves a slot from
idle to started and from done back to idle, so it knows which slots are free
without the lock. The mutex orders the job's fields between the two threads.

An executor that runs a `CtxTask` gives it a `TaskContext` of its own: a
client per slot, so two tasks never share one connection.

A `TaskStream` hands over each item as it puts it, while it runs. Its slot's
`put` leaves the encoded item in the slot's mailbox, under the same mutex,
and wakes the loop through the same pipe. The loop takes the items with the
finished jobs, in one hold of the mutex: an executor leaves its job's items
before it marks the job done, so a stream's items always reach the loop
before, or with, its end.

A stream's window is kept per slot under the same mutex: the items its task
has put, the items its driver has taken (the loop sets that when a credit
comes) and whether the stream was stopped. A `put` with the window full
waits on a second condition, `room`, which the loop signals on a credit or
a stop, and which ending the executors signals too, so a worker can still
leave while a stream waits for its reader.

A thread body can't raise or capture (see the threads tin), so an executor
catches its task's error and hands it back as the job's.

Leaving with tasks still running (a drain past its deadline, a head gone in
local mode) can't wait for them, since a task can't be interrupted: the
executors are detached instead, and their shared state is left for the
process's exit to reclaim.
"""

from std.ffi import c_int, external_call
from std.memory.alloc import unsafe_alloc
from std.os import getenv

from threads import (
    CondVar,
    Mutex,
    OpaquePtr,
    StartFn,
    ThreadHandle,
    num_cpus,
)

from dista.client import TaskContext
from dista.frame import KIND_TASK_STREAM
from dista.sizing import AUTO, Sizing
from dista.stream import StreamSink
from dista.socket import close_fd, set_nonblocking, wall_ns

comptime DispatchFn = def(
    UInt64, List[UInt8], mut List[UInt8], mut TaskContext
) thin raises
"""Runs task `task_id` on encoded arguments, appending the encoded result; a
`CtxTask` gets the worker's `TaskContext`."""

comptime IDLE = 0
comptime STARTED = 1
comptime DONE = 2

comptime STACK_BYTES = 64 << 20
"""Each executor's stack. A task runs where it would on the main thread, whose
stack is 8 MiB on Linux; macOS gives other threads only 512 KiB by default.
The stack is reserved, not committed: only the pages a task touches cost
memory."""


def worker_slots(sharing: Int = 1) raises -> Int:
    """`DISTA_WORKER_SLOTS`: tasks a worker runs at once (default 1; 0, one
    per core; `auto`, from the run's grant, shared by `sharing` workers: see
    `sizing.mojo`)."""
    var text = getenv("DISTA_WORKER_SLOTS", "1")
    if text == "":
        return 1
    if text == AUTO:
        return Sizing.from_env(sharing).slots
    var n: Int
    try:
        n = Int(text)
    except:
        raise Error("DISTA_WORKER_SLOTS must be a number, got ", text)
    if n < 0:
        raise Error("DISTA_WORKER_SLOTS must be 0 or more, got ", n)
    if n == 0:
        return num_cpus()
    return n


struct Job(Movable):
    """A task handed to a slot, and what it came back with."""

    var object_id: UInt64
    var task_id: UInt64
    var kind: UInt8
    var arrived: UInt64
    var args: List[UInt8]
    var many: Int
    """A `TaskMany`'s number of results; 0 for any other task."""
    var epoch: Int
    """The worker's head connection it was started under: one finished after
    the worker rejoined a new head is dropped."""
    var window: Int
    """A `TaskStream`'s window: how many items it may put that its driver
    hasn't taken; 0, any."""
    var started: UInt64
    var ended: UInt64
    var ok: Bool
    var out: List[UInt8]
    var error: String

    def __init__(
        out self,
        object_id: UInt64 = 0,
        task_id: UInt64 = 0,
        kind: UInt8 = 0,
        arrived: UInt64 = 0,
        var args: List[UInt8] = List[UInt8](),
        many: Int = 0,
        epoch: Int = 0,
        window: Int = 0,
    ):
        self.object_id = object_id
        self.task_id = task_id
        self.kind = kind
        self.arrived = arrived
        self.args = args^
        self.many = many
        self.epoch = epoch
        self.window = window
        self.started = 0
        self.ended = 0
        self.ok = False
        self.out = List[UInt8]()
        self.error = String()


struct StreamItem(Movable):
    """An item a stream put on a slot, for the loop to store and report."""

    var stream: UInt64
    var id: UInt64
    var epoch: Int
    """Its job's epoch: an item of a job from before a rejoin is dropped."""
    var bytes: List[UInt8]

    def __init__(
        out self, stream: UInt64, id: UInt64, epoch: Int, var bytes: List[UInt8]
    ):
        self.stream = stream
        self.id = id
        self.epoch = epoch
        self.bytes = bytes^


struct _Shared(Movable):
    """What the loop and the executors share; on the heap, at an address the
    executors are given."""

    var mutex: Mutex
    var cond: CondVar
    var room: CondVar
    """Signalled when a waiting `put` may go on: a credit, a stop, or the
    executors ending. Waited on under `mutex`."""
    var state: List[Int]
    """Each slot's state: `IDLE`, `STARTED` or `DONE`. Under `mutex`."""
    var jobs: List[Job]
    """Each slot's job: the loop's while the slot is idle or done, its
    executor's while it's started."""
    var ctxs: List[TaskContext]
    """Each slot's client, used only by its executor."""
    var mail: List[List[StreamItem]]
    """Each slot's mailbox: the items its stream put that the loop hasn't
    taken. Under `mutex`."""
    var window: List[Int]
    """Each slot's stream's window; 0, any. Under `mutex`."""
    var taken: List[Int]
    """How many of each slot's stream's items its driver has taken, as the
    last credit said. Under `mutex`."""
    var halted: List[Bool]
    """Each slot's stream was stopped: its `put` raises. Under `mutex`."""
    var next: Int
    """The next executor's slot. Under `mutex`."""
    var stop: Bool
    """Set when the executors should leave. Under `mutex`."""
    var wake_w: c_int
    """The pipe's write end: a byte per finished job."""

    def __init__(
        out self,
        n: Int,
        wake_w: c_int,
        head_addr: String,
        resume_at: String,
        registry_hash: UInt64,
        groups: List[UInt8],
    ) raises:
        self.mutex = Mutex()
        self.cond = CondVar()
        self.room = CondVar()
        self.state = List[Int](capacity=n)
        self.jobs = List[Job](capacity=n)
        self.ctxs = List[TaskContext](capacity=n)
        self.mail = List[List[StreamItem]](capacity=n)
        self.window = List[Int](capacity=n)
        self.taken = List[Int](capacity=n)
        self.halted = List[Bool](capacity=n)
        for _ in range(n):
            self.state.append(IDLE)
            self.jobs.append(Job())
            self.ctxs.append(
                TaskContext(head_addr, resume_at, registry_hash, groups.copy())
            )
            self.mail.append(List[StreamItem]())
            self.window.append(0)
            self.taken.append(0)
            self.halted.append(False)
        self.next = 0
        self.stop = False
        self.wake_w = wake_w


def _spawn[
    start: def(OpaquePtr) thin -> OpaquePtr
](arg: OpaquePtr,) raises -> ThreadHandle:
    """`ThreadHandle.spawn` with a `STACK_BYTES` stack."""
    var attr = Array[UInt64, 16](fill=0)  # pthread_attr_t, 128 bytes
    var at = attr.unsafe_ptr()
    if external_call["pthread_attr_init", c_int](at) != 0:
        raise Error("pthread_attr_init failed")
    _ = external_call["pthread_attr_setstacksize", c_int](at, STACK_BYTES)
    var tid = UInt64(0)
    # Declared exactly as the threads tin declares it: two declarations of
    # one C function with different signatures don't link.
    var rc = external_call[
        "pthread_create",
        c_int,
        Pointer[UInt64, MutUntrackedOrigin],  # pthread_t *
        Int,  # const pthread_attr_t *
        StartFn,
        OpaquePtr,
    ](
        Pointer[UInt64, MutUntrackedOrigin](
            unsafe_from_address=Int(Pointer(to=tid))
        ),
        Int(at),
        start,
        arg,
    )
    _ = external_call["pthread_attr_destroy", c_int](at)
    if rc != 0:
        raise Error("pthread_create failed, rc=", Int(rc))
    return ThreadHandle(_thread_id=tid)


struct Executors[dispatch: DispatchFn](Movable):
    """`n` threads, each running the tasks handed to its slot."""

    var _shared: Pointer[_Shared, MutUntrackedOrigin]
    var _threads: List[ThreadHandle]
    var _busy: List[Bool]
    """Slots started and not yet taken back: the loop's own view."""
    var _running: Int
    var _wake_r: c_int
    var _done: Bool
    var _items: List[StreamItem]
    """Items `take_done` took from the mailboxes, for `take_items`."""

    def __init__(
        out self,
        n: Int,
        head_addr: String = "",
        resume_at: String = "",
        registry_hash: UInt64 = 0,
        groups: List[UInt8] = List[UInt8](),
    ) raises:
        if n < 1:
            raise Error("a worker needs at least one slot")
        var fds = Array[c_int, 2](fill=-1)
        if external_call["pipe", c_int](fds.unsafe_ptr()) != 0:
            raise Error("pipe failed")
        # Neither end may block: the loop drains it until it's empty, and an
        # executor whose byte doesn't fit has nothing to add, since a full
        # pipe wakes the loop anyway.
        _ = set_nonblocking(fds[0])
        _ = set_nonblocking(fds[1])
        self._wake_r = fds[0]
        self._shared = unsafe_alloc[_Shared](1)
        self._shared.unsafe_write(
            _Shared(n, fds[1], head_addr, resume_at, registry_hash, groups)
        )
        self._threads = List[ThreadHandle](capacity=n)
        self._busy = List[Bool](capacity=n)
        self._running = 0
        self._done = False
        self._items = List[StreamItem]()
        for _ in range(n):
            self._busy.append(False)
        for _ in range(n):
            self._threads.append(
                _spawn[Self._executor](
                    OpaquePtr(unsafe_from_address=Int(self._shared))
                )
            )

    def __deinit__(deinit self):
        self._stop()

    def slots(self) -> Int:
        return len(self._busy)

    def running(self) -> Int:
        """Tasks started and not yet taken back."""
        return self._running

    def wake_fd(self) -> c_int:
        """The descriptor that's readable once a job is done."""
        return self._wake_r

    def free_slot(self) -> Int:
        """An idle slot, or -1 if every one is running a task."""
        for i in range(len(self._busy)):
            if not self._busy[i]:
                return i
        return -1

    def start(mut self, slot: Int, var job: Job, halted: Bool = False):
        """Hands `job` to idle slot `slot`; its executor starts it at once.
        A stream's window starts empty; `halted`, it was stopped before it
        started, and its first `put` raises."""
        var window = job.window
        # The slot is idle: no executor reads its job until it's started.
        self._shared[].jobs[slot] = job^
        self._busy[slot] = True
        self._running += 1
        ref sh = self._shared[]
        sh.mutex.lock()
        sh.window[slot] = window
        sh.taken[slot] = 0
        sh.halted[slot] = halted
        sh.state[slot] = STARTED
        # Every executor waits on the one condition: wake them all, and the
        # one whose slot it is takes it.
        sh.cond.broadcast()
        sh.mutex.unlock()

    def take_done(mut self) -> List[Job]:
        """The jobs finished since the last call, their slots idle again.

        It takes the items streams put meanwhile too, for `take_items`: those
        of a job it returns were all put before it ended."""
        var buf = Array[UInt8, 64](fill=0)
        while True:
            if (
                external_call["read", Int](self._wake_r, buf.unsafe_ptr(), 64)
                <= 0
            ):
                break
        var out = List[Job]()
        ref sh = self._shared[]
        sh.mutex.lock()
        for i in range(len(sh.mail)):
            if len(sh.mail[i]) > 0:
                var items = List[StreamItem]()
                swap(items, sh.mail[i])
                self._items.extend(items^)
        for i in range(len(sh.state)):
            if sh.state[i] == DONE:
                var job = Job()
                swap(job, sh.jobs[i])
                out.append(job^)
                sh.state[i] = IDLE
                self._busy[i] = False
                self._running -= 1
        sh.mutex.unlock()
        return out^

    def credit(mut self, slot: Int, taken: Int):
        """Slot `slot`'s stream's driver has taken `taken` of its items: a
        `put` waiting for the window goes on once there's room. A credit
        older than one already seen changes nothing."""
        ref sh = self._shared[]
        sh.mutex.lock()
        if taken > sh.taken[slot]:
            sh.taken[slot] = taken
            sh.room.broadcast()
        sh.mutex.unlock()

    def halt(mut self, slot: Int):
        """Stops slot `slot`'s stream: its next `put`, or the one waiting,
        raises "cancelled"."""
        ref sh = self._shared[]
        sh.mutex.lock()
        sh.halted[slot] = True
        sh.room.broadcast()
        sh.mutex.unlock()

    def take_items(mut self) -> List[StreamItem]:
        """The items the last `take_done` took, in the order they were put:
        store and report them before the jobs it returned."""
        var out = List[StreamItem]()
        swap(out, self._items)
        return out^

    @staticmethod
    def _mail(
        target: Int,
        slot: Int,
        stream: UInt64,
        id: UInt64,
        var bytes: List[UInt8],
    ) raises:
        """A slot's `put`: waits while the stream's window is full, then
        leaves the item in the slot's mailbox and wakes the loop, which
        stores and reports it. It raises "cancelled" once the stream is
        stopped, and raises too when the executors end while it waits."""
        var sh = Pointer[_Shared, MutUntrackedOrigin](
            unsafe_from_address=target
        )
        # Items put before this one: its ID is the stream's plus 1 plus that.
        var put = Int(id - stream - 1)
        sh[].mutex.lock()
        while (
            not sh[].halted[slot]
            and not sh[].stop
            and sh[].window[slot] > 0
            and put - sh[].taken[slot] >= sh[].window[slot]
        ):
            sh[].room.wait(sh[].mutex.ref())
        if sh[].halted[slot]:
            sh[].mutex.unlock()
            raise Error("cancelled")
        if sh[].stop:
            sh[].mutex.unlock()
            raise Error(
                "the worker left while this stream waited for its reader"
            )
        # The slot's job is its executor's while it runs: this is that thread.
        var epoch = sh[].jobs[slot].epoch
        sh[].mail[slot].append(StreamItem(stream, id, epoch, bytes^))
        sh[].mutex.unlock()
        var b = Array[UInt8, 1](fill=1)
        _ = external_call["write", Int](sh[].wake_w, b.unsafe_ptr(), 1)

    def _stop(mut self):
        """Ends the executors: joins them if none is running a task, else
        detaches them and leaves their state to the process's exit."""
        if self._done:
            return
        self._done = True
        ref sh = self._shared[]
        sh.mutex.lock()
        sh.stop = True
        var busy = False
        for i in range(len(sh.state)):
            if sh.state[i] == STARTED:
                busy = True
        sh.cond.broadcast()
        sh.room.broadcast()  # a `put` waiting for its reader gives up
        sh.mutex.unlock()
        if busy:
            for k in range(len(self._threads)):
                try:
                    self._threads[k].detach()
                except:
                    pass
            return
        for k in range(len(self._threads)):
            try:
                self._threads[k].join()
            except:
                pass
        _ = close_fd(self._wake_r)
        _ = close_fd(sh.wake_w)
        _ = self._shared.unsafe_take_pointee()
        self._shared.unsafe_free()

    @staticmethod
    def _executor(arg: OpaquePtr) -> OpaquePtr:
        """An executor: waits for its slot to start, runs the job, and marks
        it done, until told to stop."""
        var sh = Pointer[_Shared, MutUntrackedOrigin](
            unsafe_from_address=Int(arg)
        )
        sh[].mutex.lock()
        var i = sh[].next
        sh[].next += 1
        while True:
            while sh[].state[i] != STARTED and not sh[].stop:
                sh[].cond.wait(sh[].mutex.ref())
            if sh[].state[i] != STARTED:
                break  # stopping
            sh[].mutex.unlock()
            var streaming = sh[].jobs[i].kind == KIND_TASK_STREAM
            if streaming:
                sh[].ctxs[i].stream = StreamSink(
                    Int(arg), i, sh[].jobs[i].object_id, Self._mail
                )
            Self._run(sh[].jobs[i], sh[].ctxs[i])
            if streaming:
                sh[].ctxs[i].stream = StreamSink()
            sh[].mutex.lock()
            sh[].state[i] = DONE
            sh[].mutex.unlock()
            var b = Array[UInt8, 1](fill=1)
            _ = external_call["write", Int](sh[].wake_w, b.unsafe_ptr(), 1)
            sh[].mutex.lock()
        sh[].mutex.unlock()
        return arg

    @staticmethod
    def _run(mut job: Job, mut ctx: TaskContext):
        job.started = wall_ns()
        try:
            Self.dispatch(job.task_id, job.args, job.out, ctx)
            job.ok = True
        except e:
            job.error = String(e)
            job.ok = False
        job.args = List[UInt8]()  # its inputs, freed as it ends
        job.ended = wall_ns()
