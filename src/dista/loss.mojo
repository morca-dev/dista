"""The errors dista gives a result it lost, told from errors a task raised.

dista doesn't rebuild a result whose worker went away, so it fails it, and
fails a stream it can't finish. The head, the worker and the driver build
those errors from the texts here, and `is_loss` tells them from an error a
task's function raised, for a caller that can make the result again, as a
flow does by reading its input again. A function that raises an error
starting with one of these texts is taken for a loss too.
"""

comptime WORKER_LOST = "worker lost while running this "
"""The start of a task's or a stream's error when its worker went away and
it couldn't run again: "task" or "stream" follows."""
comptime RESULT_LOST = (
    " lost: its worker went away, and dista doesn't rebuild lost results;"
    " run the job again"
)
"""What follows `result <id>` when the worker holding that result went
away."""
comptime INPUT_LOST = "input lost: "
"""The start of a task's error on a worker when an input it was fetching
went away with the worker holding it."""
comptime INPUT_FAILED = "input failed: "
"""The start of a task's error when an input failed: a loss if the input's
error is one."""
comptime STREAM_INTERRUPTED = "stream interrupted: the head restarted"
"""A stream unfinished when the head restarted, from its driver."""
comptime ACTOR_LOST = "actor lost: its worker went away"
"""An actor call sent as its worker went away."""
comptime HEAD_LOST = (
    "the head went away while this stream waited for its reader"
)
"""A stream's `put` on a worker whose head went away."""


def lost_result(obj: UInt64) -> String:
    """The error for result `obj`, whose worker went away with it."""
    return String("result ", obj, RESULT_LOST)


def is_loss(message: String) -> Bool:
    """Whether `message`, a task's or a stream's error, says dista lost
    something rather than that a function raised: a result lost with its
    worker, a task or stream whose worker went away, an input lost either
    way, or a stream the head's restart interrupted. "cancelled" and "timed
    out" aren't losses."""
    var m = message
    while m.startswith(INPUT_FAILED):
        var rest = String(m[byte = INPUT_FAILED.byte_length() :])
        m = rest^
    if (
        m.startswith(WORKER_LOST)
        or m.startswith(INPUT_LOST)
        or m.startswith(STREAM_INTERRUPTED)
        or m.startswith(ACTOR_LOST)
        or m.startswith(HEAD_LOST)
    ):
        return True
    if not m.startswith("result "):
        return False
    var at = m.find(RESULT_LOST).or_else(-1)
    if at <= 7:
        return False
    for b in m.as_bytes()[7:at]:
        if b < UInt8(ord("0")) or b > UInt8(ord("9")):
            return False
    return True
