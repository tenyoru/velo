const std = @import("std");
const linux = std.os.linux;
const io = @import("../root.zig");
const assert = std.debug.assert;

pub const Loop = @import("Linux.zig");

pub const OperationState = union {
    uring: struct {
        cancel_pending: bool = false,
        cancel_submitted: bool = false,
        cancel_result: ?i32 = null,
        message: linux.msghdr = undefined,
        vector: std.posix.iovec = undefined,
    },
    epoll: @import("Epoll.zig").OperationState,

    pub const init: OperationState = .{ .uring = .{} };
};

pub const NativeError = linux.E;

pub fn now() io.Error!io.Timespec {
    var time: linux.timespec = undefined;
    if (linux.errno(linux.clock_gettime(.MONOTONIC, &time)) != .SUCCESS) return error.OsError;

    return .{ .sec = time.sec, .nsec = time.nsec };
}

pub fn errno(raw: ?i32) ?NativeError {
    const value = raw orelse return null;
    return if (value < 0) @fromBackingInt(@intCast(-value)) else null;
}

pub fn mapError(e: linux.E) io.Error {
    return switch (e) {
        .TIMEDOUT => error.Timeout,
        .CANCELED => error.OperationCanceled,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .PIPE => error.BrokenPipe,
        .AGAIN => error.WouldBlock,
        .BADF => error.BadFileDescriptor,
        .INVAL => error.InvalidArgument,
        .NOMEM, .NOBUFS => error.SystemResources,
        .MSGSIZE => error.MessageTooBig,
        else => error.OsError,
    };
}

pub fn tcpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const rc = linux.socket(
        if (family == .ipv4) linux.AF.INET else linux.AF.INET6,
        linux.SOCK.STREAM | linux.SOCK.CLOEXEC,
        0,
    );
    const e = linux.errno(rc);
    if (e != .SUCCESS) return mapError(e);

    return @intCast(rc);
}

pub fn udpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const rc = linux.socket(
        if (family == .ipv4) linux.AF.INET else linux.AF.INET6,
        linux.SOCK.DGRAM | linux.SOCK.CLOEXEC,
        0,
    );
    const e = linux.errno(rc);
    if (e != .SUCCESS) return mapError(e);

    return @intCast(rc);
}

pub fn shutdownSocket(fd: io.Socket, how: io.ShutdownHow) io.Error!void {
    const e = linux.errno(linux.shutdown(fd, switch (how) {
        .read => 0,
        .write => 1,
        .both => 2,
    }));
    if (e != .SUCCESS) return mapError(e);
}

pub fn datagram(op: *const io.Operation, message: *linux.msghdr, vector: *std.posix.iovec) *linux.msghdr {
    message.* = .{
        .name = null,
        .namelen = 0,
        .iov = @ptrCast(vector),
        .iovlen = 1,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };

    switch (op.data) {
        .sendto => |v| {
            vector.* = .{ .base = @constCast(v.buffer.ptr), .len = v.buffer.len };
            message.name = @constCast(v.address);
            message.namelen = v.address_len;
        },
        .recvfrom => |v| {
            vector.* = .{ .base = v.buffer.ptr, .len = v.buffer.len };
            message.name = v.address;
            message.namelen = v.address_len.*;
        },
        else => unreachable,
    }

    return message;
}

pub fn bind(fd: io.Socket, address: *const io.SockAddr, len: io.SockLen) io.Error!void {
    const e = linux.errno(linux.bind(fd, address, len));
    if (e != .SUCCESS) return mapError(e);
}

pub fn listen(fd: io.Socket, backlog: u31) io.Error!void {
    const e = linux.errno(linux.listen(fd, backlog));
    if (e != .SUCCESS) return mapError(e);
}

pub fn localAddress(fd: io.Socket, address: *io.SockAddr, len: *io.SockLen) io.Error!void {
    const e = linux.errno(linux.getsockname(fd, address, len));
    if (e != .SUCCESS) return mapError(e);
}

pub fn closeSocket(fd: io.Socket) io.Error!void {
    const e = linux.errno(linux.close(fd));
    if (e != .SUCCESS) return mapError(e);
}

pub fn prepare(op: *io.Operation, sqe: *linux.io_uring_sqe, timer: ?*linux.io_uring_sqe) void {
    const native = &op.native.uring;
    switch (op.data) {
        .connect => |v| sqe.prep_connect(v.fd, v.address, v.address_len),
        .accept => |v| sqe.prep_accept(v.fd, null, null, linux.SOCK.CLOEXEC),
        .send => |v| sqe.prep_send(
            v.fd,
            v.buffer[0..@min(v.buffer.len, std.math.maxInt(i32))],
            linux.MSG.NOSIGNAL,
        ),
        .recv => |v| sqe.prep_recv(v.fd, v.buffer[0..@min(v.buffer.len, std.math.maxInt(i32))], 0),
        .sendto => |v| sqe.prep_sendmsg(v.fd, @ptrCast(datagram(op, &native.message, &native.vector)), linux.MSG.NOSIGNAL),
        .recvfrom => |v| sqe.prep_recvmsg(v.fd, datagram(op, &native.message, &native.vector), 0),
        .close => |v| sqe.prep_close(v.fd),
        .timeout => sqe.prep_timeout(&op.time, 0, 0),
    }

    const id: u64 = @intFromPtr(op);
    assert(id & 3 == 0);
    sqe.user_data = id;

    if (timer) |t| {
        sqe.flags |= linux.IOSQE_IO_LINK;
        t.prep_link_timeout(&op.time, linux.IORING_TIMEOUT_ABS);
        t.user_data = id | 1;
    }

    op.state = .inflight;
}

pub fn complete(op: *io.Operation, is_timer: bool, res: i32) void {
    assert(op.state == .inflight);

    if (is_timer) {
        assert(op.linked and op.timer == null);
        op.timer = res;
    } else {
        assert(op.primary == null);
        op.primary = res;
        if (op.data == .recvfrom and res >= 0)
            op.data.recvfrom.address_len.* = op.native.uring.message.namelen;
    }

    finishIfReady(op);
}

pub fn cancelComplete(op: *io.Operation, res: i32) void {
    assert(op.state == .inflight and op.native.uring.cancel_submitted);
    assert(op.native.uring.cancel_result == null);

    op.native.uring.cancel_result = res;
    finishIfReady(op);
}

fn finishIfReady(op: *io.Operation) void {
    if (op.primary == null or (op.linked and op.timer == null)) return;

    // A cancellation CQE also carries this operation's address.
    if (op.native.uring.cancel_pending or
        (op.native.uring.cancel_submitted and op.native.uring.cancel_result == null)) return;

    op.finish(result(op));
}

fn result(op: *const io.Operation) io.Error!usize {
    const p = op.primary.?;
    if (p >= 0) {
        if (op.data == .recvfrom and op.native.uring.message.flags & linux.MSG.TRUNC != 0)
            return error.MessageTooBig;

        return @intCast(p);
    }

    const e = errno(p).?;
    if (op.data == .timeout and e == .TIME) return 0;
    if (op.linked) {
        const t = errno(op.timer);
        if (e == .CANCELED and t == .TIME) return error.Timeout;
        if (t) |te| {
            if (te != .TIME and te != .CANCELED and te != .ALREADY) return mapError(te);
        } else return error.OsError;
    }

    return mapError(e);
}
