//! Connect/Accept add O_NONBLOCK; transfers use MSG_DONTWAIT to avoid repeated fcntl.
//! Readiness removal cannot undo Connect; close its socket on failure, cancel or timeout.

const std = @import("std");
const linux = std.os.linux;
const io = @import("../root.zig");
const assert = std.debug.assert;

pub const OperationState = struct {
    deadline: Deadlines.Node = undefined,
    timed: bool = false,
    // Live pollable operations of one fd bucket, newest first.
    fd_prev: ?*io.Operation = null,
    fd_next: ?*io.Operation = null,
    // `io.Operation.node` is linked into `State.runnable`.
    queued: bool = false,
    prepared: bool = false,
    waiting: bool = false,
    connecting: bool = false,
    // One live operation owns the aggregate kernel mask for each fd.
    interest: u32 = 0,
    // A failed interest update must not repeat an already successful syscall.
    result: ?(io.Error!usize) = null,
};

pub const Deadline = struct {
    // Intrusive nodes and task pointers only guarantee native pointer alignment.
    ns: i128 align(@alignOf(usize)),
    // Breaks ties: the treap keeps one node per key.
    op: usize,
};

pub const Deadlines = std.Treap(Deadline, struct {
    fn order(a: Deadline, b: Deadline) std.math.Order {
        const by_time = std.math.order(a.ns, b.ns);
        return if (by_time == .eq) std.math.order(a.op, b.op) else by_time;
    }
}.order);

pub const State = struct {
    fd: linux.fd_t,
    live: usize = 0,
    // Operations to start, cancel, or finish on the next pass, oldest first.
    runnable: std.DoublyLinkedList = .{},
    deadlines: Deadlines = .{},
    // Fixed buckets avoid allocation; colliding fds increase lookup cost.
    by_fd: [1024]?*io.Operation = @splat(null),
};

pub fn init() !State {
    while (true) {
        const rc = linux.epoll_create1(linux.EPOLL.CLOEXEC);
        const e = linux.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(e);

        return .{ .fd = @intCast(rc) };
    }
}

pub fn deinit(state: *State) void {
    assert(state.live == 0 and state.runnable.first == null and state.deadlines.root == null);
    _ = linux.close(state.fd);
}

pub fn turn(loop: *io.Loop, state: *State) !bool {
    try activate(loop, state, nanoseconds(try io.now()));
    try start(state);

    // service checks control directly; only a blocking poll needs the wake fd.
    const woke = state.live != 0 and try events(loop, state, false);
    try expire(state, nanoseconds(try io.now()));
    return woke;
}

pub fn park(loop: *io.Loop, state: *State) !void {
    _ = try events(loop, state, true);
}

/// Like `turn`, consumes readiness.
pub fn ready(loop: *io.Loop, state: *State) !bool {
    if (try events(loop, state, false)) return true;
    return loop.executor.hasReady() or state.runnable.first != null;
}

pub fn cancel(state: *State, op: *io.Operation) void {
    enqueue(state, op);
}

fn activate(loop: *io.Loop, state: *State, timestamp: i128) !void {
    while (loop.pending.popFirst()) |node| {
        const op: *io.Operation = @fieldParentPtr("node", node);

        op.state = .inflight;
        state.live += 1;
        if (pollable(op)) link(state, op);

        if (op.linked or op.data == .timeout) {
            if (op.time.sec < 0 or op.time.nsec < 0 or op.time.nsec >= 1_000_000_000) {
                try fail(state, op, .INVAL);
                continue;
            }
            // A zero timeout would leave the treap again in this turn's expire.
            if (op.data == .timeout and op.time.sec == 0 and op.time.nsec == 0) {
                op.primary = negative(.TIME);
                try settle(state, op, 0);
                continue;
            }
            schedule(state, op, nanoseconds(op.time) + if (op.linked) @as(i128, 0) else timestamp);
        }
        if (op.data != .timeout) enqueue(state, op);
    }
}

fn start(state: *State) !void {
    while (state.runnable.popFirst()) |node| {
        const op: *io.Operation = @fieldParentPtr("node", node);
        op.native.epoll.queued = false;

        if (op.native.epoll.result != null) {
            try finish(state, op);
            continue;
        }

        if (op.cancel_requested) {
            try fail(state, op, .CANCELED);
            continue;
        }

        if (op.data == .timeout or op.native.epoll.prepared or blocked(op)) continue;

        op.native.epoll.prepared = true;
        if (op.data == .connect or op.data == .accept) {
            if (configure(socket(op))) |e| {
                try fail(state, op, e);
                continue;
            }
        }

        try attempt(state, op);
    }
}

/// FIFO per fd and direction: older operations sit after `op` in its bucket.
fn blocked(op: *const io.Operation) bool {
    if (!pollable(op)) return false;

    var next = op.native.epoll.fd_next;
    while (next) |other| : (next = other.native.epoll.fd_next) {
        if (socket(other) == socket(op) and direction(other) == direction(op)) return true;
    }

    return false;
}

fn bucket(state: *State, fd: linux.fd_t) *?*io.Operation {
    return &state.by_fd[@as(u32, @bitCast(fd)) % state.by_fd.len];
}

fn link(state: *State, op: *io.Operation) void {
    const head = bucket(state, socket(op));
    op.native.epoll.fd_prev = null;
    op.native.epoll.fd_next = head.*;
    if (head.*) |first| first.native.epoll.fd_prev = op;
    head.* = op;
}

fn unlink(state: *State, op: *io.Operation) void {
    const native = &op.native.epoll;
    if (native.fd_prev) |prev| prev.native.epoll.fd_next = native.fd_next else bucket(state, socket(op)).* = native.fd_next;
    if (native.fd_next) |next| next.native.epoll.fd_prev = native.fd_prev;
    native.fd_prev = null;
    native.fd_next = null;
}

fn enqueue(state: *State, op: *io.Operation) void {
    if (op.native.epoll.queued) return;

    op.native.epoll.queued = true;
    state.runnable.append(&op.node);
}

fn schedule(state: *State, op: *io.Operation, expires: i128) void {
    var entry = state.deadlines.getEntryFor(.{ .ns = expires, .op = @intFromPtr(op) });
    entry.set(&op.native.epoll.deadline);
    op.native.epoll.timed = true;
}

fn attempt(state: *State, op: *io.Operation) !void {
    assert(op.native.epoll.result == null);

    while (true) {
        const rc = switch (op.data) {
            .connect => |v| blk: {
                if (!op.native.epoll.connecting) {
                    // EINTR can leave connect in progress, so never issue it twice.
                    op.native.epoll.connecting = true;
                    const result = linux.connect(v.fd, v.address, v.address_len);
                    const e = linux.errno(result);
                    if (e == .INTR or e == .INPROGRESS or e == .ALREADY or e == .AGAIN) {
                        try arm(state, op);
                        return;
                    }

                    break :blk result;
                }

                var value: i32 = 0;
                var length: linux.socklen_t = @sizeOf(i32);
                const result = linux.getsockopt(
                    v.fd,
                    linux.SOL.SOCKET,
                    linux.SO.ERROR,
                    std.mem.asBytes(&value),
                    &length,
                );
                const e = linux.errno(result);
                if (e != .SUCCESS) break :blk result;

                const socket_error: linux.E = @fromBackingInt(@intCast(value));
                if (socket_error == .INPROGRESS or socket_error == .ALREADY or
                    socket_error == .AGAIN)
                {
                    try arm(state, op);
                    return;
                }
                if (socket_error != .SUCCESS) {
                    try fail(state, op, socket_error);
                    return;
                }

                break :blk @as(usize, 0);
            },
            .accept => |v| linux.accept4(
                v.fd,
                null,
                null,
                linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
            ),
            .send => |v| linux.sendto(
                v.fd,
                v.buffer.ptr,
                @min(v.buffer.len, std.math.maxInt(i32)),
                linux.MSG.NOSIGNAL | linux.MSG.DONTWAIT,
                null,
                0,
            ),
            .recv => |v| linux.recvfrom(
                v.fd,
                v.buffer.ptr,
                @min(v.buffer.len, std.math.maxInt(i32)),
                linux.MSG.DONTWAIT,
                null,
                null,
            ),
            .sendto => |v| linux.sendto(
                v.fd,
                v.buffer.ptr,
                v.buffer.len,
                linux.MSG.NOSIGNAL | linux.MSG.DONTWAIT,
                v.address,
                v.address_len,
            ),
            .recvfrom => |v| blk: {
                var header: linux.msghdr = undefined;
                var vector: std.posix.iovec = undefined;
                const message = @import("Uring.zig").datagram(op, &header, &vector);
                const rc = linux.recvmsg(v.fd, message, linux.MSG.DONTWAIT);
                if (linux.errno(rc) == .SUCCESS) {
                    v.address_len.* = message.namelen;
                    if (message.flags & linux.MSG.TRUNC != 0) {
                        try fail(state, op, .MSGSIZE);
                        return;
                    }
                }

                break :blk rc;
            },
            .close => |v| linux.close(v.fd),
            .timeout, .read_at, .write_at, .sync => unreachable,
        };

        const e = linux.errno(rc);
        // Linux releases the fd even when close reports EINTR; retry could close a reused fd.
        if (e == .INTR and op.data != .close) continue;
        if (e == .AGAIN and op.data != .close) {
            try arm(state, op);
            return;
        }
        if (e != .SUCCESS) {
            try fail(state, op, e);
            return;
        }

        op.primary = @intCast(rc);
        try settle(state, op, rc);
        return;
    }
}

fn arm(state: *State, op: *io.Operation) !void {
    op.native.epoll.waiting = true;
    if (refresh(state, socket(op), null)) |e| try fail(state, op, e);
}

fn refresh(state: *State, fd: linux.fd_t, exclude: ?*io.Operation) ?linux.E {
    // Operations that never started cannot change the fd's interest mask.
    if (exclude) |op| {
        if (!op.native.epoll.prepared) return null;
    }

    var old: u32 = 0;
    var wanted: u32 = 0;
    var owner: ?*io.Operation = null;
    var next = bucket(state, fd).*;

    while (next) |op| : (next = op.native.epoll.fd_next) {
        if (socket(op) != fd) continue;
        old |= op.native.epoll.interest;
        if (op == exclude or !op.native.epoll.waiting) continue;
        wanted |= direction(op);
        if (owner == null) owner = op;
    }

    if (old != wanted) {
        const change: u32 = if (wanted == 0)
            linux.EPOLL.CTL_DEL
        else if (old == 0)
            linux.EPOLL.CTL_ADD
        else
            linux.EPOLL.CTL_MOD;
        var event: linux.epoll_event = .{ .events = wanted, .data = .{ .fd = fd } };

        while (true) {
            const e = linux.errno(linux.epoll_ctl(
                state.fd,
                change,
                fd,
                if (wanted == 0) null else &event,
            ));
            if (e == .INTR) continue;
            if (e != .SUCCESS and !(change == linux.EPOLL.CTL_DEL and e == .NOENT)) return e;
            break;
        }
    }

    // Transfer the registration only after the kernel has accepted the new mask.
    next = bucket(state, fd).*;
    while (next) |op| : (next = op.native.epoll.fd_next) {
        if (socket(op) == fd) op.native.epoll.interest = 0;
    }
    if (owner) |op| op.native.epoll.interest = wanted;

    return null;
}

fn events(loop: *io.Loop, state: *State, wait: bool) !bool {
    var timeout: i32 = 0;
    if (wait) {
        timeout = if (state.deadlines.getMin()) |nearest|
            milliseconds(nearest.key.ns - nanoseconds(try io.now()))
        else
            -1;
    }

    var batch: [64]linux.epoll_event = undefined;
    const count = linux.epoll_wait(state.fd, &batch, batch.len, timeout);
    const e = linux.errno(count);
    if (e == .INTR) return false;
    if (e != .SUCCESS) return mapError(e);

    var woke = false;
    // Consume every fd identity before executor callbacks can close/reuse descriptors or frames.
    for (batch[0..count]) |event| {
        if (event.data.fd == loop.wake_fd) {
            loop.drainWake();
            woke = true;
            continue;
        }

        var next = bucket(state, event.data.fd).*;
        while (next) |op| {
            next = op.native.epoll.fd_next;
            if (!op.native.epoll.waiting or socket(op) != event.data.fd) continue;
            if (event.events & (direction(op) | linux.EPOLL.ERR | linux.EPOLL.HUP) != 0)
                try attempt(state, op);
        }
    }

    return woke;
}

fn expire(state: *State, timestamp: i128) !void {
    while (state.deadlines.getMin()) |nearest| {
        if (nearest.key.ns > timestamp) return;
        const op: *io.Operation = @ptrFromInt(nearest.key.op);

        if (op.data == .timeout) {
            op.primary = negative(.TIME);
            try settle(state, op, 0);
        } else {
            op.primary = negative(.CANCELED);
            op.timer = negative(.TIME);
            try settle(state, op, error.Timeout);
        }
    }
}

fn fail(state: *State, op: *io.Operation, e: linux.E) !void {
    op.primary = negative(e);
    try settle(state, op, mapError(e));
}

fn settle(state: *State, op: *io.Operation, result: io.Error!usize) !void {
    op.native.epoll.result = result;
    op.native.epoll.waiting = false;
    try finish(state, op);
}

fn finish(state: *State, op: *io.Operation) !void {
    const native = &op.native.epoll;
    if (pollable(op)) {
        if (refresh(state, socket(op), op)) |e| {
            // start retries the deregistration with the retained result.
            enqueue(state, op);
            return mapError(e);
        }
    }

    assert(native.interest == 0 and !native.waiting);
    if (op.linked and op.timer == null) op.timer = negative(.CANCELED);

    const result = native.result.?;
    native.result = null;
    if (native.queued) {
        state.runnable.remove(&op.node);
        native.queued = false;
    }
    if (native.timed) {
        var entry = state.deadlines.getEntryForExisting(&native.deadline);
        entry.set(null);
        native.timed = false;
    }
    if (pollable(op)) {
        // The oldest newer operation on this fd and direction may start now.
        var prev = native.fd_prev;
        while (prev) |other| : (prev = other.native.epoll.fd_prev) {
            if (socket(other) == socket(op) and direction(other) == direction(op)) {
                enqueue(state, other);
                break;
            }
        }
        unlink(state, op);
    }
    state.live -= 1;
    op.finish(result);
}

fn pollable(op: *const io.Operation) bool {
    return op.data != .timeout and op.data != .close;
}

fn socket(op: *const io.Operation) linux.fd_t {
    return switch (op.data) {
        inline .connect, .accept, .send, .recv, .sendto, .recvfrom, .close => |v| v.fd,
        .timeout, .read_at, .write_at, .sync => unreachable,
    };
}

fn direction(op: *const io.Operation) u32 {
    return if (op.data == .recv or op.data == .recvfrom or op.data == .accept)
        linux.EPOLL.IN
    else
        linux.EPOLL.OUT;
}

fn configure(fd: linux.fd_t) ?linux.E {
    const flags = while (true) {
        const rc = linux.fcntl(fd, linux.F.GETFL, 0);
        const e = linux.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break rc;
    };

    const nonblock: u32 = @bitCast(@as(linux.O, .{ .NONBLOCK = true }));
    if (flags & nonblock != 0) return null;

    while (true) {
        const e = linux.errno(linux.fcntl(fd, linux.F.SETFL, flags | nonblock));
        if (e == .INTR) continue;
        return if (e == .SUCCESS) null else e;
    }
}

pub fn nanoseconds(value: io.Timespec) i128 {
    return @as(i128, value.sec) * 1_000_000_000 + value.nsec;
}

fn milliseconds(remaining: i128) i32 {
    return @intCast(@min(@divTrunc(@max(0, remaining) + 999_999, 1_000_000), std.math.maxInt(i32)));
}

fn negative(e: linux.E) i32 {
    return -@as(i32, @backingInt(e));
}

pub fn mapError(e: linux.E) io.Error {
    return switch (e) {
        .TIME, .TIMEDOUT => error.Timeout,
        .CANCELED => error.OperationCanceled,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .PIPE => error.BrokenPipe,
        .AGAIN => error.WouldBlock,
        .BADF => error.BadFileDescriptor,
        .INVAL => error.InvalidArgument,
        .NOMEM, .NOBUFS, .MFILE, .NFILE, .NOSPC => error.SystemResources,
        .MSGSIZE => error.MessageTooBig,
        else => error.OsError,
    };
}

// `activate` without a loop, for operations built by hand.
fn track(state: *State, op: *io.Operation) void {
    state.live += 1;
    link(state, op);
    enqueue(state, op);
}

test "epoll deadlines preserve equal expiry and large timestamps" {
    const co = @import("../../coroutines/root.zig");

    var state = try init();
    defer deinit(&state);

    var executor: co.Executor = .{};
    var task: co.Task = .{
        .executor = &executor,
        .poll_fn = struct {
            fn poll(_: *co.Task) bool {
                unreachable;
            }
        }.poll,
    };
    var timers: [4]io.Operation = @splat(.{
        .loop = undefined,
        .data = .{ .timeout = .{ .duration = .{ .sec = 0, .nsec = 0 } } },
        .linked = false,
        .time = .{ .sec = 0, .nsec = 0 },
        .state = .inflight,
        .task = &task,
    });
    const distant = @as(i128, std.math.maxInt(i64)) + 7;
    for (&timers, [_]i128{ 7, 5, 5, distant }) |*op, expires| {
        state.live += 1;
        schedule(&state, op, expires);
    }

    try expire(&state, 4);
    try std.testing.expectEqual(4, state.live);
    try expire(&state, 5);
    try std.testing.expectEqual(2, state.live);
    try std.testing.expectEqual(null, timers[0].completion);
    try std.testing.expectEqual(0, try timers[1].completion.?);
    try std.testing.expectEqual(0, try timers[2].completion.?);
    try expire(&state, 7);
    try std.testing.expectEqual(0, try timers[0].completion.?);
    try expire(&state, distant - 1);
    try std.testing.expectEqual(1, state.live);
    try std.testing.expectEqual(null, timers[3].completion);
    try expire(&state, distant);
    try std.testing.expectEqual(0, try timers[3].completion.?);
}

test "epoll wait rounds deadlines up and caps long waits" {
    try std.testing.expectEqual(@as(i32, 0), milliseconds(-1));
    try std.testing.expectEqual(@as(i32, 0), milliseconds(0));
    try std.testing.expectEqual(@as(i32, 1), milliseconds(1));
    try std.testing.expectEqual(@as(i32, 1), milliseconds(1_000_000));
    try std.testing.expectEqual(@as(i32, 2), milliseconds(1_000_001));
    try std.testing.expectEqual(
        std.math.maxInt(i32),
        milliseconds(@as(i128, std.math.maxInt(i64)) * 1_000_000_000),
    );
}

test "epoll FIFO interest aggregation and cancellation remove idle write interest" {
    const co = @import("../../coroutines/root.zig");

    var state = try init();
    defer deinit(&state);

    var fds: [2]linux.fd_t = undefined;
    try std.testing.expectEqual(
        linux.E.SUCCESS,
        linux.errno(linux.socketpair(
            linux.AF.UNIX,
            linux.SOCK.STREAM | linux.SOCK.CLOEXEC,
            0,
            &fds,
        )),
    );
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var executor: co.Executor = .{};
    var task: co.Task = .{
        .executor = &executor,
        .poll_fn = struct {
            fn poll(_: *co.Task) bool {
                unreachable;
            }
        }.poll,
    };
    var bytes: [2]u8 = undefined;
    var first: io.Operation = .{
        .loop = undefined,
        .data = .{ .recv = .{ .fd = fds[0], .buffer = bytes[0..1] } },
        .linked = false,
        .time = .{ .sec = 0, .nsec = 0 },
        .state = .inflight,
        .task = &task,
    };
    var second: io.Operation = .{
        .loop = undefined,
        .data = .{ .recv = .{ .fd = fds[0], .buffer = bytes[1..] } },
        .linked = false,
        .time = .{ .sec = 0, .nsec = 0 },
        .state = .inflight,
        .task = &task,
    };
    var send: io.Operation = .{
        .loop = undefined,
        .data = .{ .send = .{ .fd = fds[0], .buffer = "x" } },
        .linked = false,
        .time = .{ .sec = 0, .nsec = 0 },
        .state = .inflight,
        .task = &task,
    };

    var canceled = second;
    track(&state, &first);
    track(&state, &canceled);
    track(&state, &second);
    track(&state, &send);
    try std.testing.expect(!blocked(&first));
    try std.testing.expect(blocked(&second));
    try std.testing.expect(!blocked(&send));

    first.native.epoll.prepared = true;
    send.native.epoll.prepared = true;
    try arm(&state, &first);
    try arm(&state, &send);
    try std.testing.expectEqual(
        @as(u32, linux.EPOLL.IN | linux.EPOLL.OUT),
        send.native.epoll.interest,
    );

    canceled.cancel_requested = true;
    cancel(&state, &canceled);
    try start(&state);
    try std.testing.expect(!canceled.native.epoll.prepared);
    try std.testing.expectError(error.OperationCanceled, canceled.completion.?);
    try std.testing.expect(blocked(&second));
    try std.testing.expectEqual(
        @as(u32, linux.EPOLL.IN | linux.EPOLL.OUT),
        send.native.epoll.interest,
    );

    try fail(&state, &send, .CANCELED);
    try std.testing.expectError(error.OperationCanceled, send.completion.?);
    try std.testing.expectEqual(@as(u32, linux.EPOLL.IN), first.native.epoll.interest);

    var batch: [1]linux.epoll_event = undefined;
    try std.testing.expectEqual(@as(usize, 0), linux.epoll_wait(state.fd, &batch, batch.len, 0));

    try fail(&state, &first, .CANCELED);
    try std.testing.expect(!blocked(&second));
    try start(&state);
    try std.testing.expect(second.native.epoll.waiting);

    second.cancel_requested = true;
    cancel(&state, &second);
    try start(&state);
    try std.testing.expectError(error.OperationCanceled, second.completion.?);
    try std.testing.expectEqual(0, state.live);
    try std.testing.expectEqual(@as(usize, 0), linux.epoll_wait(state.fd, &batch, batch.len, 0));
}

test "epoll failed deregistration retains result and does not repeat receive" {
    const co = @import("../../coroutines/root.zig");

    var state = try init();
    defer deinit(&state);

    var fds: [2]linux.fd_t = undefined;
    try std.testing.expectEqual(
        linux.E.SUCCESS,
        linux.errno(linux.socketpair(
            linux.AF.UNIX,
            linux.SOCK.STREAM | linux.SOCK.CLOEXEC,
            0,
            &fds,
        )),
    );
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var executor: co.Executor = .{};
    var task: co.Task = .{
        .executor = &executor,
        .poll_fn = struct {
            fn poll(_: *co.Task) bool {
                unreachable;
            }
        }.poll,
    };
    var byte: [1]u8 = undefined;
    var recv: io.Operation = .{
        .loop = undefined,
        .data = .{ .recv = .{ .fd = fds[0], .buffer = &byte } },
        .linked = false,
        .time = .{ .sec = 0, .nsec = 0 },
        .state = .inflight,
        .task = &task,
    };

    track(&state, &recv);
    try start(&state);
    try std.testing.expect(recv.native.epoll.waiting);
    try std.testing.expectEqual(
        @as(usize, 1),
        linux.sendto(fds[1], "a", 1, linux.MSG.NOSIGNAL, null, 0),
    );

    const epoll_fd = state.fd;
    state.fd = -1;
    const result = attempt(&state, &recv);
    state.fd = epoll_fd;

    try std.testing.expectError(error.BadFileDescriptor, result);
    try std.testing.expectEqual(.inflight, recv.state);
    try std.testing.expectEqual(@as(usize, 1), try recv.native.epoll.result.?);
    try std.testing.expectEqual(@as(u32, linux.EPOLL.IN), recv.native.epoll.interest);
    try std.testing.expectEqualStrings("a", &byte);

    try std.testing.expectEqual(
        @as(usize, 1),
        linux.sendto(fds[1], "b", 1, linux.MSG.NOSIGNAL, null, 0),
    );
    try start(&state);
    try std.testing.expectEqual(@as(usize, 1), try recv.completion.?);
    try std.testing.expectEqual(@as(usize, 1), linux.recvfrom(fds[0], &byte, 1, 0, null, null));
    try std.testing.expectEqualStrings("b", &byte);
}
