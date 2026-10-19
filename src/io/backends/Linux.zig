const std = @import("std");
const linux = std.os.linux;
const assert = std.debug.assert;
const Operation = @import("../root.zig").Operation;
const Executor = @import("../../coroutines/Executor.zig");
const Uring = @import("Uring.zig");
const Epoll = @import("Epoll.zig");
const Control = @import("../Control.zig");
const Loop = @This();
pub const Options = @import("../root.zig").LoopOptions;

ring: linux.IoUring = undefined,
epoll: ?Epoll.State = null,
executor: Executor = .{},
pending: std.DoublyLinkedList = .{},
cancellations: std.DoublyLinkedList = .{},
active: usize = 0,
outstanding_cqes: usize = 0,
running: bool = false,
control: Control = .{ .signal = signalLoop },
wake_fd: linux.fd_t = -1,
wake_armed: bool = false,
wake_received: bool = false,

pub fn init(options: Options) !Loop {
    return initWithBackend(options, .auto);
}

/// `.auto` falls back to epoll only when io_uring is denied, or the kernel lacks it or
/// an opcode the loop submits.
pub const Backend = enum { auto, uring, epoll };

pub fn initWithBackend(options: Options, selected: Backend) !Loop {
    const entries = options.entries;
    if (entries < 2) return error.RingTooSmall;
    if (!std.math.isPowerOfTwo(entries)) return error.EntriesNotPowerOfTwo;

    var loop: Loop = switch (selected) {
        .uring => try initUring(entries),
        .epoll => .{ .epoll = try Epoll.init() },
        .auto => initUring(entries) catch |err| switch (err) {
            error.PermissionDenied, error.SystemOutdated => .{ .epoll = try Epoll.init() },
            else => return err,
        },
    };
    errdefer if (loop.epoll) |*state| Epoll.deinit(state) else loop.ring.deinit();

    const fd = linux.eventfd(0, linux.EFD.NONBLOCK | linux.EFD.CLOEXEC);
    const e = linux.errno(fd);
    if (e != .SUCCESS) return Uring.mapError(e);

    loop.wake_fd = @intCast(fd);
    errdefer _ = linux.close(loop.wake_fd);

    if (loop.epoll) |*state| {
        var event: linux.epoll_event = .{
            .events = linux.EPOLL.IN,
            .data = .{ .fd = loop.wake_fd },
        };
        const result = linux.errno(linux.epoll_ctl(
            state.fd,
            linux.EPOLL.CTL_ADD,
            loop.wake_fd,
            &event,
        ));
        if (result != .SUCCESS) return Uring.mapError(result);
    }

    return loop;
}

fn signalLoop(control: *Control) void {
    const loop: *Loop = @fieldParentPtr("control", control);
    const one: u64 = 1;

    while (true) {
        const e = linux.errno(linux.write(loop.wake_fd, std.mem.asBytes(&one).ptr, @sizeOf(u64)));
        if (e == .INTR) continue;

        assert(e == .SUCCESS or e == .AGAIN);
        return;
    }
}

pub fn drainWake(loop: *Loop) void {
    var value: u64 = undefined;

    while (true) {
        const e = linux.errno(linux.read(loop.wake_fd, std.mem.asBytes(&value).ptr, @sizeOf(u64)));
        if (e == .INTR) continue;

        assert(e == .SUCCESS or e == .AGAIN);
        return;
    }
}

const shared = @import("../loop.zig").Shared(Loop, .{
    .turn = turn,
    .inflight = inflight,
    .park = park,
    .close = close,
});
pub const run = shared.run;
pub const deinit = shared.deinit;
pub const post = shared.post;
pub const retain = shared.retain;
pub const release = shared.release;
pub const stop = shared.stop;
pub const shutdown = shared.shutdown;

/// Every opcode the loop submits.
const required_ops = [_]linux.IORING_OP{
    .POLL_ADD,
    .SENDMSG,
    .RECVMSG,
    .TIMEOUT,
    .ACCEPT,
    .ASYNC_CANCEL,
    .LINK_TIMEOUT,
    .CONNECT,
    .CLOSE,
    .SEND,
    .RECV,
};

fn initUring(entries: u16) !Loop {
    var ring = try linux.IoUring.init(entries, 0);
    errdefer ring.deinit();

    // Live requests can exceed CQ capacity; limiting them can strand their dependencies.
    if (ring.features & linux.IORING_FEAT_NODROP == 0) return error.SystemOutdated;

    // NODROP arrived in 5.5, SEND/RECV/CLOSE and the probe in 5.6; 5.5 rejects the probe.
    const probe = ring.get_probe() catch |err| switch (err) {
        error.ArgumentsInvalid => return error.SystemOutdated,
        else => return err,
    };
    if (!supportsAll(probe)) return error.SystemOutdated;

    return .{ .ring = ring };
}

fn supportsAll(probe: linux.io_uring_probe) bool {
    for (required_ops) |op| {
        if (!probe.is_supported(op)) return false;
    }

    return true;
}

fn close(loop: *Loop) void {
    assert(loop.cancellations.first == null and loop.outstanding_cqes == 0);

    if (loop.epoll) |*state| Epoll.deinit(state) else loop.ring.deinit();
    _ = linux.close(loop.wake_fd);
}

pub fn cancel(loop: *Loop, op: *Operation) void {
    if (loop.epoll != null) return;

    assert(!op.native.cancel_pending and !op.native.cancel_submitted);
    op.native.cancel_pending = true;
    loop.cancellations.append(&op.native.cancel_node);
}

fn turn(loop: *Loop) !bool {
    if (loop.epoll) |*state| return Epoll.turn(loop, state);

    loop.wake_received = false;
    try loop.completions(0);
    if (loop.wake_received) return true;

    loop.prepare();
    if (try loop.submitPending()) {
        try loop.completions(0);
        return true;
    }

    return loop.cancellations.first != null;
}

fn inflight(loop: *const Loop) bool {
    if (loop.epoll) |*state| return state.live.first != null;
    return loop.outstanding_cqes > 0;
}

fn park(loop: *Loop) !void {
    if (loop.epoll) |*state| return Epoll.park(loop, state);

    // A full SQ can defer arming; never block where a post cannot wake us.
    if (loop.wake_armed) try loop.completions(1);
}

fn submitPending(loop: *Loop) !bool {
    // Waiting in the submitting enter saves a syscall per blocking iteration.
    var wait: u32 = if (loop.canWait()) 1 else 0;
    var waited = false;

    // sq_ready includes previously flushed SQEs left by partial submissions.
    while (loop.ring.sq_ready() > 0) {
        const submitted = loop.ring.submit_and_wait(wait) catch |err| switch (err) {
            error.SignalInterrupt => continue,
            error.CompletionQueueOvercommitted => {
                try loop.completions(0);
                wait = 0;
                continue;
            },
            else => return err,
        };

        waited = wait != 0;
        if (submitted == 0 and loop.ring.sq_ready() > 0) return error.SubmissionStalled;
    }

    return waited;
}

fn canWait(loop: *Loop) bool {
    return loop.executor.ready.first == null and loop.pending.first == null and
        loop.cancellations.first == null and loop.wake_armed and
        (loop.outstanding_cqes > 0 or loop.control.hasWork());
}

fn prepare(loop: *Loop) void {
    if (!loop.wake_armed) {
        if (loop.ring.get_sqe()) |sqe| {
            sqe.prep_poll_add(loop.wake_fd, linux.POLL.IN);
            // This poll owns only an fd reference, never caller/frame memory.
            sqe.user_data = 3;
            loop.wake_armed = true;
        } else |_| {}
    }

    while (loop.cancellations.first) |node| {
        const sqe = loop.ring.get_sqe() catch break;
        const native: *Uring.OperationState = @fieldParentPtr("cancel_node", node);
        const op: *Operation = @fieldParentPtr("native", native);

        sqe.prep_cancel(@intFromPtr(op), 0);
        sqe.user_data = @intFromPtr(op) | 2;

        loop.cancellations.remove(node);
        native.cancel_pending = false;
        native.cancel_submitted = true;
        loop.outstanding_cqes += 1;
    }

    while (loop.pending.first) |node| {
        const op: *Operation = @fieldParentPtr("node", node);
        const count: usize = if (op.linked) 2 else 1;
        if (loop.ring.sq.sqes.len - loop.ring.sq_ready() < count) return;

        const primary = loop.ring.get_sqe() catch unreachable;
        const timer = if (op.linked) loop.ring.get_sqe() catch unreachable else null;

        loop.pending.remove(node);
        op.prepare(primary, timer);
        loop.outstanding_cqes += count;
    }
}

fn completions(loop: *Loop, wait: u32) !void {
    var cqes: [64]linux.io_uring_cqe = undefined;
    const count = while (true) {
        break loop.ring.copy_cqes(&cqes, wait) catch |err| switch (err) {
            error.SignalInterrupt => continue,
            else => return err,
        };
    };

    // copy_cqes already advanced past the whole batch, so a wake error must not drop the rest.
    var wake_failed = false;

    for (cqes[0..count]) |cqe| {
        if (cqe.user_data == 3) {
            loop.wake_armed = false;
            if (cqe.res < 0) {
                wake_failed = true;
                continue;
            }

            loop.wake_received = true;
            loop.drainWake();
            continue;
        }

        assert(loop.outstanding_cqes > 0);
        loop.outstanding_cqes -= 1;

        const op: *Operation = @ptrFromInt(cqe.user_data & ~@as(u64, 3));
        const tag = cqe.user_data & 3;
        if (tag == 2) Uring.cancelComplete(op, cqe.res) else op.complete(tag == 1, cqe.res);
    }

    if (wake_failed) return error.WakePollFailed;
}

test "io_uring probe requires every submitted opcode" {
    var probe = std.mem.zeroInit(linux.io_uring_probe, .{
        .last_op = .RECV,
        .ops_len = @backingInt(linux.IORING_OP.RECV) + 1,
    });
    for (required_ops) |op| probe.ops[@backingInt(op)].flags = linux.IO_URING_OP_SUPPORTED;

    try std.testing.expect(supportsAll(probe));

    // Like 5.5: CONNECT is the newest opcode, so SEND, RECV, and CLOSE are missing.
    probe.last_op = .CONNECT;
    try std.testing.expect(!supportsAll(probe));

    probe.last_op = .RECV;
    probe.ops[@backingInt(linux.IORING_OP.SEND)].flags = 0;
    try std.testing.expect(!supportsAll(probe));
}
