"""`dista` declares libc's socket calls as Mosaad-M/tcp and tls do.

Mojo allows one signature per C symbol in a program, so a binary importing
dista and tcp (or tls, or requests) compiles only if both declare `send`,
`recv`, `connect`, `close`, `setsockopt`, `getsockopt` and `poll` alike.
This test declares them as tcp does, in the same program as dista's own, and
sends a byte over a dista socket through them.
"""

from std.ffi import external_call
from std.memory.alloc import unsafe_alloc
from std.sys.info import CompilationTarget
from std.testing import TestSuite, assert_equal

from dista.socket import Listener, Stream


def _tcp_send(fd: Int32, buf_addr: Int, length: Int, flags: Int32) -> Int:
    return external_call["send", Int](fd, buf_addr, length, flags)


def _tcp_recv(fd: Int32, buf_addr: Int, length: Int, flags: Int32) -> Int:
    return external_call["recv", Int](fd, buf_addr, length, flags)


def _tcp_connect(fd: Int32, addr: Int, addrlen: Int32) -> Int32:
    return external_call["connect", Int32](fd, addr, addrlen)


def _tcp_close(fd: Int32) -> Int32:
    return external_call["close", Int32](fd)


def _tcp_setsockopt(
    fd: Int32, level: Int32, name: Int32, value: Int, length: Int32
) -> Int32:
    return external_call["setsockopt", Int32](fd, level, name, value, length)


def _tcp_getsockopt(
    fd: Int32, level: Int32, name: Int32, value: Int, length_addr: Int
) -> Int32:
    return external_call["getsockopt", Int32](
        fd, level, name, value, length_addr
    )


def _tcp_poll(fds_addr: Int, nfds: Int, timeout_ms: Int32) -> Int32:
    return external_call["poll", Int32](fds_addr, nfds, timeout_ms)


def _is_macos() -> Bool:
    return CompilationTarget.is_macos()


def test_dista_and_tcp_declarations_link_together() raises:
    var l = Listener(0)
    var c = Stream.connect(l.port)
    var s = l.accept()
    var one = List[UInt8](length=1, fill=7)
    assert_equal(_tcp_send(c.fd, Int(one.unsafe_ptr()), 1, Int32(0)), 1)
    assert_equal(one[0], 7)  # keeps `one` alive across the send
    var got = List[UInt8](length=1, fill=0)
    assert_equal(s.read_into(got.unsafe_ptr(), 1), 1)
    assert_equal(got[0], 7)
    s.write_all(got)
    # The stream is non-blocking: wait for the echo with poll(POLLIN).
    var pfd = List[Int32](length=2, fill=0)
    pfd[0] = c.fd
    pfd[1] = 1  # events = POLLIN, revents = 0
    assert_equal(_tcp_poll(Int(pfd.unsafe_ptr()), 1, Int32(5000)), 1)
    assert_equal(pfd[0], c.fd)  # keeps `pfd` alive across the poll
    var back = List[UInt8](length=1, fill=0)
    assert_equal(_tcp_recv(c.fd, Int(back.unsafe_ptr()), 1, Int32(0)), 1)
    assert_equal(back[0], 7)
    # The other declarations: each call is real, and fails or does nothing.
    var v = unsafe_alloc[Int32](2)
    v[unsafe_offset=0] = 99
    v[unsafe_offset=1] = 4  # the option's length, after its value
    var level: Int32 = 0xFFFF if _is_macos() else 1
    var so_error: Int32 = 0x1007 if _is_macos() else 4
    var r = _tcp_getsockopt(
        c.fd, level, so_error, Int(v), Int(v.unsafe_offset(1))
    )
    var err = v[unsafe_offset=0]
    v.unsafe_free()
    assert_equal(r, 0)
    assert_equal(err, 0)
    assert_equal(_tcp_setsockopt(-1, 0, 0, 0, Int32(0)), -1)
    assert_equal(_tcp_connect(-1, 0, Int32(0)), -1)
    assert_equal(_tcp_poll(Int(pfd.unsafe_ptr()), 0, Int32(0)), 0)
    assert_equal(_tcp_close(-1), -1)
    # Both ends stay open until here: a value dies after its last use, which
    # can be before a call that was passed its `fd` runs.
    c.close()
    s.close()


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
