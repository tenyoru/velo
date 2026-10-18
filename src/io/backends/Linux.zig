const std = @import("std");
const linux = std.os.linux;
const assert = std.debug.assert;
const Operation = @import("../root.zig").Operation;
const Executor = @import("../../coroutines/Executor.zig");
const Uring = @import("Uring.zig");
const Epoll = @import("Epoll.zig");
const Control = @import("../Control.zig");
const ThreadGuard = @import("../../ThreadGuard.zig");
const Loop = @This();

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

pub fn init(entries: u16) !Loop {
    return initWithBackend(entries, .auto);
}

/// `.auto` falls back to epoll only when io_uring is unsupported or denied.
pub const Backend = enum { auto, uring, epoll };

pub fn initWithBackend(entries: u16, selected: Backend) !Loop {
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

fn initUring(entries: u16) !Loop {
    var ring = try linux.IoUring.init(entries, 0);
    errdefer ring.deinit();

    // Live requests can exceed CQ capacity; limiting them can strand their dependencies.
    if (ring.features & linux.IORING_FEAT_NODROP == 0) return error.SystemOutdated;

    return .{ .ring = ring };
}

pub fn deinit(loop: *Loop) void {
    const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

    assert(!loop.running);
    loop.control.deinit();
    assert(loop.executor.all_tasks.first == null);
    assert(loop.active == 0 and loop.pending.first == null);
    assert(loop.cancellations.first == null);
    assert(loop.outstanding_cqes == 0 and loop.executor.ready.first == null);

    if (loop.epoll) |*state| Epoll.deinit(state) else loop.ring.deinit();
    _ = linux.close(loop.wake_fd);
}

pub fn cancel(loop: *Loop, op: *Operation) void {
    const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

    if (loop.epoll != null) return;

    assert(!op.native.cancel_pending and !op.native.cancel_submitted);
    op.native.cancel_pending = true;
    loop.cancellations.append(&op.native.cancel_node);
}

pub fn run(loop: *Loop) !void {
    // Keep guard address calculations out of normal builds.
    const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

    assert(!loop.running);
    loop.running = true;
    defer loop.running = false;

    if (loop.epoll) |*state| return Epoll.run(loop, state);

    while (true) {
        if (loop.control.service(&loop.executor)) return;

        loop.wake_received = false;
        var budget: usize = 64;
        while (budget > 0 and loop.executor.runOne()) : (budget -= 1) {}

        try loop.completions(0);
        if (loop.wake_received) continue;

        loop.prepare();
        if (try loop.submitPending()) {
            try loop.completions(0);
            continue;
        }

        // Queued sends may be what allows already-submitted receives to complete.
        if (loop.executor.ready.first != null or loop.pending.first != null or
            loop.cancellations.first != null) continue;

        if (loop.outstanding_cqes == 0 and !loop.control.hasWork()) {
            if (!loop.control.idle(&loop.executor)) continue;
            assert(loop.pending.first == null and loop.active == 0);
            return;
        }

        // A full SQ can defer arming; never block where a post cannot wake us.
        if (!loop.wake_armed) continue;
        try loop.completions(1);
    }
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
