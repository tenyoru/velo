//! Adds O_NONBLOCK and SO_NOSIGPIPE; descriptors remain caller-owned.
//! Readiness removal cannot undo Connect; close its socket on failure, cancel or timeout.

const std = @import("std");
const c = std.c;
const io = @import("../root.zig");
const Control = @import("../Control.zig");
const co = @import("../../coroutines/root.zig");
const ThreadGuard = @import("../../ThreadGuard.zig");
const assert = std.debug.assert;

pub const NativeError = c.E;

pub const OperationState = struct {
    // Intrusive nodes and Task pointers only guarantee native pointer alignment.
    expires: ?i128 align(@alignOf(usize)) = null,
    prepared: bool = false,
    connecting: bool = false,
    registered: bool = false,
};

pub fn errno(result: ?i32) ?NativeError {
    const value = result orelse return null;
    return if (value < 0) @fromBackingInt(@intCast(-value)) else null;
}

pub fn now() io.Error!io.Timespec {
    var value: c.timespec = undefined;
    const e = c.errno(c.clock_gettime(.MONOTONIC, &value));
    if (e != .SUCCESS) return mapError(e);

    return .{ .sec = value.sec, .nsec = value.nsec };
}

pub fn tcpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const fd = while (true) {
        const rc = c.socket(
            if (family == .ipv4) c.AF.INET else c.AF.INET6,
            c.SOCK.STREAM,
            c.IPPROTO.TCP,
        );
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        break rc;
    };
    errdefer _ = c.close(fd);

    if (configureCreated(fd)) |e| return mapError(e);

    return fd;
}

pub fn udpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const fd = while (true) {
        const rc = c.socket(
            if (family == .ipv4) c.AF.INET else c.AF.INET6,
            c.SOCK.DGRAM,
            c.IPPROTO.UDP,
        );
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        break rc;
    };
    errdefer _ = c.close(fd);

    if (configureCreated(fd)) |e| return mapError(e);

    return fd;
}

pub fn shutdownSocket(fd: io.Socket, how: io.ShutdownHow) io.Error!void {
    while (true) {
        const e = c.errno(c.shutdown(fd, switch (how) {
            .read => c.SHUT.RD,
            .write => c.SHUT.WR,
            .both => c.SHUT.RDWR,
        }));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        return;
    }
}

pub fn bind(fd: io.Socket, address: *const io.SockAddr, len: io.SockLen) io.Error!void {
    while (true) {
        const e = c.errno(c.bind(fd, address, len));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        return;
    }
}

pub fn listen(fd: io.Socket, backlog: u31) io.Error!void {
    while (true) {
        const e = c.errno(c.listen(fd, backlog));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        return;
    }
}

pub fn localAddress(fd: io.Socket, address: *io.SockAddr, len: *io.SockLen) io.Error!void {
    while (true) {
        const e = c.errno(c.getsockname(fd, address, len));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);
        return;
    }
}

pub fn closeSocket(fd: io.Socket) io.Error!void {
    const e = c.errno(c.close(fd));
    if (e != .SUCCESS) return mapError(e);
}

pub const Loop = struct {
    kqueue: c.fd_t,
    control: Control = .{ .signal = signalLoop },
    executor: co.Executor = .{},
    pending: std.DoublyLinkedList = .{},
    active: usize = 0,
    running: bool = false,
    live: std.DoublyLinkedList = .{},

    pub fn init(entries: u16) !Loop {
        if (entries < 2) return error.RingTooSmall;

        const fd = while (true) {
            const rc = c.kqueue();
            const e = c.errno(rc);
            if (e == .INTR) continue;
            if (e != .SUCCESS) return mapError(e);
            break rc;
        };
        errdefer _ = c.close(fd);

        const changes = [_]c.Kevent{
            .{
                .ident = 0,
                .filter = c.EVFILT.USER,
                .flags = c.EV.ADD | c.EV.ENABLE | c.EV.CLEAR,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            },
        };

        while (true) {
            const e = c.errno(c.kevent(fd, &changes, 1, &.{}, 0, null));
            if (e == .INTR) continue;
            if (e != .SUCCESS) return mapError(e);
            break;
        }

        return .{ .kqueue = fd };
    }

    pub fn deinit(loop: *Loop) void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(!loop.running);
        loop.control.deinit();
        assert(loop.active == 0 and loop.pending.first == null);
        assert(loop.live.first == null and loop.executor.ready.first == null);
        assert(loop.executor.all_tasks.first == null);

        _ = c.close(loop.kqueue);
    }

    pub fn post(loop: *Loop, command: *Control.Post) void {
        loop.control.post(command);
    }

    pub fn retain(loop: *Loop) void {
        loop.control.retain();
    }

    pub fn release(loop: *Loop) void {
        loop.control.release();
    }

    pub fn stop(loop: *Loop) void {
        loop.control.stop();
    }

    pub fn shutdown(loop: *Loop) void {
        loop.control.shutdown();
    }

    pub fn run(loop: *Loop) !void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(!loop.running);
        loop.running = true;
        defer loop.running = false;

        while (true) {
            if (loop.control.service(&loop.executor)) return;

            var budget: usize = 64;
            while (budget > 0 and loop.executor.runOne()) : (budget -= 1) {}

            loop.activate(nanoseconds(try now()));
            loop.start();

            const signaled = try loop.events(false);
            try loop.cancelAndExpire(nanoseconds(try now()));

            // A wake consumed above may be the only notification for an unserviced post.
            if (signaled) continue;
            if (loop.executor.ready.first != null or loop.pending.first != null) continue;

            if (loop.live.first == null and !loop.control.hasWork()) {
                if (!loop.control.idle(&loop.executor)) continue;
                assert(loop.active == 0);
                return;
            }

            _ = try loop.events(true);
        }
    }

    pub fn cancel(loop: *Loop, op: *io.Operation) void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(op.loop == loop and op.state == .inflight and op.cancel_requested);
    }

    fn activate(loop: *Loop, timestamp: i128) void {
        while (loop.pending.popFirst()) |node| {
            const op: *io.Operation = @fieldParentPtr("node", node);

            op.state = .inflight;
            loop.live.append(node);

            if (op.linked or op.data == .timeout) {
                if (op.time.sec < 0 or op.time.nsec < 0 or op.time.nsec >= 1_000_000_000) {
                    loop.fail(op, .INVAL);
                    continue;
                }
                op.native.expires = nanoseconds(op.time) +
                    if (op.linked) @as(i128, 0) else timestamp;
            }
        }
    }

    fn start(loop: *Loop) void {
        var node = loop.live.first;

        while (node) |current| {
            node = current.next;
            const op: *io.Operation = @fieldParentPtr("node", current);
            if (op.data == .timeout or op.cancel_requested or op.native.prepared or
                loop.blocked(op)) continue;

            op.native.prepared = true;
            if (op.data != .close) {
                if (configure(socket(op))) |e| {
                    loop.fail(op, e);
                    continue;
                }
            }

            loop.attempt(op);
        }
    }

    // O(n²) activation, O(n) deadlines; use fd/direction indexing and a timer heap at scale.
    fn blocked(loop: *Loop, op: *io.Operation) bool {
        if (op.data == .close) return false;

        var node = loop.live.first;
        while (node) |current| : (node = current.next) {
            const other: *io.Operation = @fieldParentPtr("node", current);
            if (other == op) return false;
            if (other.data == .timeout or other.data == .close) continue;
            if (socket(other) == socket(op) and filter(other) == filter(op)) return true;
        }

        unreachable;
    }

    fn attempt(loop: *Loop, op: *io.Operation) void {
        assert(!op.native.registered);

        while (true) {
            const result: isize = switch (op.data) {
                .connect => |v| blk: {
                    if (!op.native.connecting) {
                        // EINTR can leave connect in progress, so never issue it twice.
                        op.native.connecting = true;
                        const rc = c.connect(v.fd, v.address, v.address_len);
                        const e = c.errno(rc);
                        if (e == .INTR or e == .INPROGRESS or e == .ALREADY or e == .AGAIN) {
                            loop.arm(op);
                            return;
                        }

                        break :blk rc;
                    }

                    var value: c_int = 0;
                    var length: c.socklen_t = @sizeOf(c_int);
                    const rc = c.getsockopt(v.fd, c.SOL.SOCKET, c.SO.ERROR, &value, &length);
                    if (rc == -1) break :blk rc;

                    const e: c.E = @fromBackingInt(@intCast(value));
                    if (e == .INPROGRESS or e == .ALREADY or e == .AGAIN) {
                        loop.arm(op);
                        return;
                    }
                    if (e != .SUCCESS) {
                        loop.fail(op, e);
                        return;
                    }

                    break :blk 0;
                },
                .send => |v| c.send(
                    v.fd,
                    v.buffer.ptr,
                    @min(v.buffer.len, std.math.maxInt(i32)),
                    0,
                ),
                .recv => |v| c.recv(
                    v.fd,
                    v.buffer.ptr,
                    @min(v.buffer.len, std.math.maxInt(i32)),
                    0,
                ),
                .sendto => |v| blk: {
                    if (v.buffer.len > std.math.maxInt(i32)) {
                        loop.fail(op, .MSGSIZE);
                        return;
                    }

                    const length = std.math.cast(c.socklen_t, v.address_len) orelse {
                        loop.fail(op, .INVAL);
                        return;
                    };

                    break :blk c.sendto(v.fd, v.buffer.ptr, v.buffer.len, 0, v.address, length);
                },
                .recvfrom => |v| blk: {
                    const length = std.math.cast(c.socklen_t, v.address_len.*) orelse {
                        loop.fail(op, .INVAL);
                        return;
                    };

                    var vectors = [_]c.iovec{
                        .{
                            .base = v.buffer.ptr,
                            .len = @min(v.buffer.len, std.math.maxInt(i32)),
                        },
                    };
                    var message: c.msghdr = .{
                        .name = v.address,
                        .namelen = length,
                        .iov = &vectors,
                        .iovlen = vectors.len,
                        .control = null,
                        .controllen = 0,
                        .flags = 0,
                    };

                    const rc = c.recvmsg(v.fd, &message, 0);
                    if (rc >= 0) {
                        v.address_len.* = @intCast(message.namelen);
                        if (message.flags & c.MSG.TRUNC != 0) {
                            loop.fail(op, .MSGSIZE);
                            return;
                        }
                    }

                    break :blk rc;
                },
                .accept => |v| blk: {
                    const fd = c.accept(v.fd, null, null);
                    if (fd < 0) break :blk fd;
                    if (configureCreated(fd)) |e| {
                        _ = c.close(fd);
                        loop.fail(op, e);
                        return;
                    }

                    break :blk fd;
                },
                .close => |v| c.close(v.fd),
                .timeout => unreachable,
            };

            const e = c.errno(result);
            if (e == .INTR and op.data != .close) continue;
            if (e == .AGAIN and op.data != .close) {
                loop.arm(op);
                return;
            }
            if (e != .SUCCESS) {
                loop.fail(op, e);
                return;
            }

            op.primary = @intCast(result);
            loop.finish(op, @intCast(result));
            return;
        }
    }

    fn arm(loop: *Loop, op: *io.Operation) void {
        if (loop.change(op, c.EV.ADD | c.EV.ENABLE | c.EV.ONESHOT)) |e| {
            loop.fail(op, e);
        } else {
            op.native.registered = true;
        }
    }

    fn change(loop: *Loop, op: *io.Operation, flags: u16) ?c.E {
        const changes = [_]c.Kevent{
            .{
                .ident = @intCast(socket(op)),
                .filter = filter(op),
                .flags = flags,
                .fflags = 0,
                .data = 0,
                .udata = @intFromPtr(op),
            },
        };
        var unused: [0]c.Kevent = .{};

        while (true) {
            const e = c.errno(c.kevent(loop.kqueue, &changes, 1, &unused, 0, null));
            if (e == .INTR) continue;
            return if (e == .SUCCESS) null else e;
        }
    }

    fn events(loop: *Loop, wait: bool) !bool {
        var timeout: c.timespec = .{ .sec = 0, .nsec = 0 };
        var timeout_ptr: ?*const c.timespec = &timeout;
        if (wait) {
            var nearest: ?i128 = null;
            var node = loop.live.first;

            while (node) |current| : (node = current.next) {
                const op: *io.Operation = @fieldParentPtr("node", current);
                if (op.native.expires) |expires| nearest = @min(nearest orelse expires, expires);
            }

            if (nearest) |expires| {
                const remaining = @max(0, expires - nanoseconds(try now()));
                timeout.sec = @intCast(@min(
                    @divTrunc(remaining, 1_000_000_000),
                    std.math.maxInt(@TypeOf(timeout.sec)),
                ));
                timeout.nsec = @intCast(@mod(remaining, 1_000_000_000));
            } else timeout_ptr = null;
        }

        var batch: [64]c.Kevent = undefined;
        const changes: [0]c.Kevent = .{};
        const count = c.kevent(loop.kqueue, &changes, 0, &batch, batch.len, timeout_ptr);
        const e = c.errno(count);
        if (e == .INTR) return false;
        if (e != .SUCCESS) return mapError(e);

        var signaled = false;
        // No callbacks or cancellation until every fetched operation pointer is consumed.
        for (batch[0..@intCast(count)]) |event| {
            if (event.udata == 0) {
                assert(event.filter == c.EVFILT.USER and event.ident == 0);
                signaled = true;
                continue;
            }

            const op: *io.Operation = @ptrFromInt(event.udata);
            assert(op.state == .inflight and op.native.registered);
            op.native.registered = false;
            if (event.flags & c.EV.ERROR != 0 and event.data != 0) {
                loop.fail(op, @fromBackingInt(@intCast(event.data)));
            } else loop.attempt(op);
        }

        return signaled;
    }

    fn cancelAndExpire(loop: *Loop, timestamp: i128) !void {
        var node = loop.live.first;

        while (node) |current| {
            node = current.next;
            const op: *io.Operation = @fieldParentPtr("node", current);
            if (!op.cancel_requested) {
                const expires = op.native.expires orelse continue;
                if (expires > timestamp) continue;
            }

            if (op.native.registered) {
                if (loop.change(op, c.EV.DELETE)) |e| {
                    if (e != .NOENT) return mapError(e);
                }
                op.native.registered = false;
            }

            if (op.cancel_requested) {
                loop.fail(op, .CANCELED);
            } else if (op.data == .timeout) {
                op.primary = negative(.TIMEDOUT);
                loop.finish(op, 0);
            } else {
                op.primary = negative(.CANCELED);
                op.timer = negative(.TIMEDOUT);
                loop.finish(op, error.Timeout);
            }
        }
    }

    fn fail(loop: *Loop, op: *io.Operation, e: c.E) void {
        op.primary = negative(e);
        loop.finish(op, mapError(e));
    }

    fn finish(loop: *Loop, op: *io.Operation, result: io.Error!usize) void {
        assert(!op.native.registered);
        if (op.linked and op.timer == null) op.timer = negative(.CANCELED);

        loop.live.remove(&op.node);
        op.finish(result);
    }
};

fn signalLoop(control: *Control) void {
    const loop: *Loop = @fieldParentPtr("control", control);
    const changes = [_]c.Kevent{
        .{
            .ident = 0,
            .filter = c.EVFILT.USER,
            .flags = 0,
            .fflags = c.NOTE.TRIGGER,
            .data = 0,
            .udata = 0,
        },
    };

    while (true) {
        const e = c.errno(c.kevent(loop.kqueue, &changes, 1, &.{}, 0, null));
        if (e == .INTR) continue;
        if (e != .SUCCESS) std.debug.panic("kqueue wake failed: {t}", .{e});
        return;
    }
}

fn socket(op: *const io.Operation) c.fd_t {
    return switch (op.data) {
        inline .connect, .accept, .send, .recv, .sendto, .recvfrom, .close => |v| v.fd,
        .timeout => unreachable,
    };
}

fn filter(op: *const io.Operation) i16 {
    return if (op.data == .recv or op.data == .recvfrom or op.data == .accept)
        c.EVFILT.READ
    else
        c.EVFILT.WRITE;
}

fn configureCreated(fd: c.fd_t) ?c.E {
    const flags = while (true) {
        const rc = c.fcntl(fd, c.F.GETFD);
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break rc;
    };

    while (true) {
        const e = c.errno(c.fcntl(fd, c.F.SETFD, flags | @as(c_int, c.FD_CLOEXEC)));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break;
    }

    return configure(fd);
}

fn configure(fd: c.fd_t) ?c.E {
    const flags = while (true) {
        const rc = c.fcntl(fd, c.F.GETFL);
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break rc;
    };

    const nonblock: c_int = @bitCast(@as(c.O, .{ .NONBLOCK = true }));
    while (true) {
        const e = c.errno(c.fcntl(fd, c.F.SETFL, flags | nonblock));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break;
    }

    const enabled: c_int = 1;
    while (true) {
        const e = c.errno(c.setsockopt(fd, c.SOL.SOCKET, c.SO.NOSIGPIPE, &enabled, @sizeOf(c_int)));
        if (e == .INTR) continue;
        return if (e == .SUCCESS) null else e;
    }
}

fn nanoseconds(value: io.Timespec) i128 {
    return @as(i128, value.sec) * 1_000_000_000 + value.nsec;
}

fn negative(e: c.E) i32 {
    return -@as(i32, @backingInt(e));
}

fn mapError(e: c.E) io.Error {
    return switch (e) {
        .TIMEDOUT => error.Timeout,
        .CANCELED => error.OperationCanceled,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .PIPE => error.BrokenPipe,
        .AGAIN => error.WouldBlock,
        .BADF => error.BadFileDescriptor,
        .INVAL => error.InvalidArgument,
        .MSGSIZE => error.MessageTooBig,
        .NOMEM, .NOBUFS, .MFILE, .NFILE => error.SystemResources,
        else => error.OsError,
    };
}

test "kqueue wakes defer callbacks to control service" {
    const Probe = struct {
        command: Control.Post = .{ .callback = callback },
        loop: *Loop,
        calls: usize = 0,

        fn callback(command: *Control.Post) void {
            const probe: *@This() = @fieldParentPtr("command", command);
            probe.calls += 1;
            if (probe.calls == 1) probe.loop.post(command) else probe.loop.release();
        }
    };

    var loop = try Loop.init(2);
    defer loop.deinit();

    var probe: Probe = .{ .loop = &loop };
    loop.retain();
    loop.post(&probe.command);

    try std.testing.expect(try loop.events(false));
    try std.testing.expectEqual(0, probe.calls);
    try loop.run();
    try std.testing.expectEqual(2, probe.calls);
    try std.testing.expect(!try loop.events(false));

    loop.stop();
    try loop.run();
    try loop.run();
}

test "kqueue datagrams report truncation and accept empty messages" {
    var loop = try Loop.init(2);
    defer loop.deinit();

    const receiver = try udpSocket(.ipv4);
    defer closeSocket(receiver) catch unreachable;
    const sender = try udpSocket(.ipv4);
    defer closeSocket(sender) catch unreachable;

    var address: io.SockAddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    var length: io.SockLen = @sizeOf(@TypeOf(address));
    try bind(receiver, @ptrCast(&address), length);
    try localAddress(receiver, @ptrCast(&address), &length);

    var source: io.SockAddr.in = undefined;
    var source_length: io.SockLen = @sizeOf(@TypeOf(source));
    var buffer: [1]u8 = undefined;
    var recv: co.Frame(io.RecvFrom) = .{
        .coroutine = io.RecvFrom.init(&loop, .{
            .fd = receiver,
            .buffer = &buffer,
            .address = @ptrCast(&source),
            .address_len = &source_length,
        }),
    };
    var send: co.Frame(io.SendTo) = .{
        .coroutine = io.SendTo.init(&loop, .{
            .fd = sender,
            .buffer = "large",
            .address = @ptrCast(&address),
            .address_len = length,
        }),
    };

    loop.executor.spawn(&recv.task);
    loop.executor.spawn(&send.task);
    try loop.run();

    try std.testing.expectEqual(5, try send.result.?);
    try std.testing.expectError(error.MessageTooBig, recv.result.?);
    try std.testing.expectEqual(@sizeOf(@TypeOf(source)), source_length);
    try std.testing.expect(source.port != 0);

    recv.coroutine.deinit();
    send.coroutine.deinit();

    recv = .{
        .coroutine = io.RecvFrom.init(&loop, .{
            .fd = receiver,
            .buffer = buffer[0..0],
            .address = @ptrCast(&source),
            .address_len = &source_length,
        }),
    };
    send = .{
        .coroutine = io.SendTo.init(&loop, .{
            .fd = sender,
            .buffer = "",
            .address = @ptrCast(&address),
            .address_len = length,
        }),
    };

    loop.executor.spawn(&recv.task);
    loop.executor.spawn(&send.task);
    try loop.run();
    try std.testing.expectEqual(0, try send.result.?);
    try std.testing.expectEqual(0, try recv.result.?);

    recv.coroutine.deinit();
    send.coroutine.deinit();
    try std.testing.expectError(error.BadFileDescriptor, shutdownSocket(-1, .both));
}
