//! Single-threaded stackless I/O. Backend-specific rules are in `backends/*.zig`.
//!
//! Pin loops, frames, operations, addresses and buffers until ready is consumed,
//! including after cancellation or run errors. Drain before reuse or deinit.
//! Poll/cancel on the owning loop thread; do not drive its executor separately.
//! Tasks cannot finish with unconsumed operations. Successful I/O wins late cancel/timeout.
//! Close after all I/O on that socket. Operations first polled by a canceled task or
//! during shutdown fail with OperationCanceled unsubmitted; use closeSocket in cleanup.
//!
//! Run errors retain work for retry, not automatic cancellation or cleanup.
//! Shutdown: request shutdown(), then drive run() until drained. If a persistent
//! backend failure prevents draining, abort rather than unwind borrowed storage.
//!
//! Deadlines are absolute in event.now()'s clock; Timeout durations start on activation.
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
    else => @compileError("velo.event supports Linux, macOS, and Windows"),
};
pub const Loop = backend.Loop;
pub const LoopOptions = struct {
    /// io_uring SQ/macOS AIO admission capacity, not a task limit; Linux requires a power of two >= 2.
    entries: u16 = 32,
    /// Up to 64 borrowed regions, registered once by io_uring; other backends ignore this.
    /// Pin the descriptor array and backing memory until the loop is drained and deinitialized;
    /// leave the descriptors unchanged throughout that lifetime.
    /// Contained file transfers use fixed buffers; registration failures fail initialization.
    buffers: []const []u8 = &.{},
    /// Calling thread's Linux io_uring async-worker CPU IDs, borrowed only during init;
    /// duplicates are harmless. The kernel may share these workers across its rings.
    /// Empty leaves the kernel default untouched without an affinity syscall. Epoll and
    /// other backends ignore this; it does not configure application pools or all OS workers.
    /// Requires Linux 5.14+; registration failures fail init without automatic fallback.
    /// Effective affinity remains subject to the kernel's cpuset and CPU availability rules.
    /// Some kernels intersect this with inherited user affinity when they overlap;
    /// use CPUs disjoint from a pinned loop CPU to keep worker execution off that CPU.
    iowq_cpus: []const usize = &.{},
};

pub const Control = @import("Control.zig");
pub const SendAll = @import("stream.zig").SendAll;
pub const ReadExact = @import("stream.zig").ReadExact;
pub const WithTimeout = @import("timeout.zig").WithTimeout;

const net = @import("net.zig");
pub const Address = net.Address;
pub const NativeAddress = net.NativeAddress;
pub const Listener = net.Listener;
pub const Stream = net.Stream;
pub const Dial = net.Dial;
pub const Server = @import("server.zig").Server;

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

pub fn milliseconds(ms: u32) Timespec {
    return .{ .sec = ms / std.time.ms_per_s, .nsec = (ms % std.time.ms_per_s) * std.time.ns_per_ms };
}

pub fn seconds(s: u32) Timespec {
    return .{ .sec = s, .nsec = 0 };
}

/// Absolute deadline `duration` after `now()`, for operation and `Stream` deadlines.
pub fn deadlineIn(duration: Timespec) Error!Timespec {
    const start = try now();
    const nsec = start.nsec + duration.nsec;

    return .{
        .sec = start.sec + duration.sec + @divFloor(nsec, std.time.ns_per_s),
        .nsec = @mod(nsec, std.time.ns_per_s),
    };
}

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
    UnsupportedOperation,
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
    if (builtin.os.tag == .linux) {
        _ = @import("tests/uring.zig");
        _ = @import("tests/iowq.zig");
    }
    _ = @import("tests/portable.zig");
    _ = @import("tests/lifecycle.zig");
    _ = @import("tests/runtime.zig");
    _ = @import("tests/net.zig");
    _ = @import("tests/server.zig");
}
