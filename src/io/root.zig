//! Single-threaded stackless I/O. Backend-specific rules are in `backends/*.zig`.
//!
//! Pin loops, frames, operations, addresses and buffers until ready is consumed,
//! including after cancellation or run errors. Drain before reuse or deinit.
//! Poll/cancel on the owning loop thread; do not drive its executor separately.
//! Tasks cannot finish with unconsumed operations. Successful I/O wins late cancel/timeout.
//! Close after all I/O on that socket. Operations first polled by a canceled task or
//! during shutdown fail with OperationCanceled unsubmitted; use closeSocket in cleanup.
//!
//! Deadlines are absolute in io.now()'s clock; Timeout durations start on activation.
//! Recv returning zero means EOF; RecvFrom returning zero is an empty datagram.
//! Streams may transfer short counts; oversized datagrams fail with MessageTooBig.
//! A truncated datagram is consumed; peer outputs are unspecified on error.
//!
//! Parked tasks alone do not keep run alive; retain the loop for external producers.
//! Initialize the loop before socket helpers and close sockets before loop deinit.
//! Readiness backends cannot undo connect: close failed/canceled/timed-out connects.

const std = @import("std");
const builtin = @import("builtin");

const linux = std.os.linux;

pub const backend = switch (builtin.os.tag) {
    .linux => @import("backends/Uring.zig"),
    .macos => @import("backends/Kqueue.zig"),
    .windows => @import("backends/Iocp.zig"),
    else => @compileError("velo.io supports Linux, macOS, and Windows"),
};
pub const Loop = backend.Loop;
pub const LoopOptions = struct {
    /// io_uring submission capacity, not a task limit; Linux requires a power of two >= 2.
    entries: u16 = 32,
};

pub const Control = @import("Control.zig");
pub const WorkerPool = @import("WorkerPool.zig");
pub const fs = @import("fs.zig");
pub const SendAll = @import("stream.zig").SendAll;
pub const ReadExact = @import("stream.zig").ReadExact;

pub const Socket = if (builtin.os.tag == .windows) usize else std.c.fd_t;
pub const SockAddr = switch (builtin.os.tag) {
    .linux => linux.sockaddr,
    .windows => std.os.windows.ws2_32.sockaddr,
    else => std.c.sockaddr,
};
pub const SockLen = u32;
pub const Timespec = if (builtin.os.tag == .linux) linux.kernel_timespec else extern struct {
    sec: i64,
    nsec: i64,
};

pub const NativeError = backend.NativeError;
pub const now = backend.now;

pub const AddressFamily = enum { ipv4, ipv6 };
pub const tcpSocket = backend.tcpSocket;
pub const udpSocket = backend.udpSocket;

pub const ShutdownHow = enum { read, write, both };
pub const shutdownSocket = backend.shutdownSocket;
pub const bind = backend.bind;
pub const listen = backend.listen;
pub const localAddress = backend.localAddress;
pub const closeSocket = backend.closeSocket;

pub const Error = error{
    Timeout,
    OperationCanceled,
    ConnectionRefused,
    ConnectionResetByPeer,
    BrokenPipe,
    WouldBlock,
    BadFileDescriptor,
    InvalidArgument,
    SystemResources,
    MessageTooBig,
    OsError,
};

pub const Operation = @import("Operation.zig");
pub const Kind = Operation.Kind;
pub const Data = Operation.Data;
pub const Connect = Operation.Coroutine(.connect);
pub const Accept = Operation.Coroutine(.accept);
pub const Send = Operation.Coroutine(.send);
pub const Recv = Operation.Coroutine(.recv);
pub const SendTo = Operation.Coroutine(.sendto);
pub const RecvFrom = Operation.Coroutine(.recvfrom);
pub const Close = Operation.Coroutine(.close);
pub const Timeout = Operation.Coroutine(.timeout);

test {
    if (builtin.os.tag == .linux) _ = @import("tests/uring.zig");
    _ = @import("tests/portable.zig");
    _ = @import("tests/lifecycle.zig");
    _ = @import("tests/worker_pool.zig");
    _ = @import("tests/fs.zig");
    _ = @import("tests/runtime.zig");
}
