const std = @import("std");
const linux = std.os.linux;
const assert = std.debug.assert;
const Operation = @import("../root.zig").Operation;
const Error = @import("../root.zig").Error;
const Executor = @import("../../coroutines/Executor.zig");
const Uring = @import("Uring.zig");
const Epoll = @import("Epoll.zig");
const Control = @import("../Control.zig");
const Loop = @This();
pub const Options = @import("../root.zig").LoopOptions;

ring: linux.IoUring = undefined,
epoll: ?Epoll.State = null,
file_ops: struct { read: bool = false, write: bool = false, sync: bool = false } = .{},
buffers: []const []u8 = &.{},
executor: Executor = .{},
pending: std.DoublyLinkedList = .{},
cancellations: std.DoublyLinkedList = .{},
// Software timers: a kernel timeout per timer costs an SQE, a CQE and async cancellation.
timers: Epoll.Deadlines = .{},
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

/// `.auto` falls back to epoll only when io_uring is denied, or the kernel lacks it,
/// a required networking/control opcode, or timed waits (Linux 5.11).
pub const Backend = enum { auto, uring, epoll };

pub fn initWithBackend(options: Options, selected: Backend) !Loop {
    const entries = options.entries;
    if (entries < 2) return error.RingTooSmall;
    if (!std.math.isPowerOfTwo(entries)) return error.EntriesNotPowerOfTwo;

    var loop: Loop = switch (selected) {
        .uring => try initUring(options),
        .epoll => .{ .epoll = try Epoll.init() },
        .auto => initUring(options) catch |err| switch (err) {
            error.PermissionDenied, error.SystemOutdated => .{ .epoll = try Epoll.init() },
            else => return err,
        },
    };
    errdefer if (loop.epoll) |*state| Epoll.deinit(state) else loop.ring.deinit();

    // Registration errors must not enter the setup-only automatic fallback above.
    if (loop.epoll == null and options.iowq_cpus.len != 0) {
        const allocator = std.heap.page_allocator;
        var mask = try iowqMask(allocator, options.iowq_cpus);
        defer mask.deinit(allocator);
        try checkIowqAffinity(linux.errno(linux.io_uring_register(
            loop.ring.fd,
            .REGISTER_IOWQ_AFF,
            mask.masks,
            @intCast(mask.bit_length / 8),
        )));
    }

    if (loop.epoll == null and options.buffers.len != 0) {
        var vectors: [64]std.posix.iovec = undefined;
        if (options.buffers.len > vectors.len) return error.TooManyBuffers;
        for (options.buffers, 0..) |buffer, i| {
            vectors[i] = .{ .base = buffer.ptr, .len = buffer.len };
        }
        const e = linux.errno(linux.io_uring_register(
            loop.ring.fd,
            .REGISTER_BUFFERS,
            vectors[0..options.buffers.len].ptr,
            @intCast(options.buffers.len),
        ));
        switch (e) {
            .SUCCESS => {},
            .FAULT => return error.InvalidArgument,
            .PERM, .ACCES => return error.PermissionDenied,
            else => return Uring.mapError(e),
        }
        loop.buffers = options.buffers;
    }

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

fn iowqMask(allocator: std.mem.Allocator, cpus: []const usize) !std.DynamicBitSetUnmanaged {
    assert(cpus.len != 0);
    const word_bits = @bitSizeOf(usize);
    const bits = (try std.math.add(usize, std.mem.max(usize, cpus), word_bits)) &
        ~@as(usize, word_bits - 1);
    _ = std.math.cast(u32, bits / 8) orelse return error.Overflow;
    var mask = try std.DynamicBitSetUnmanaged.initEmpty(allocator, bits);
    for (cpus) |cpu| mask.set(cpu);
    return mask;
}

fn checkIowqAffinity(e: linux.E) !void {
    switch (e) {
        .SUCCESS => {},
        .NOSYS, .OPNOTSUPP, .INVAL => return error.IowqAffinityUnsupported,
        .PERM, .ACCES => return error.PermissionDenied,
        else => return Uring.mapError(e),
    }
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
pub const spawn = shared.spawn;
pub const block = shared.block;
pub const stats = shared.stats;
pub const post = shared.post;
pub const retain = shared.retain;
pub const release = shared.release;
pub const stop = shared.stop;
pub const shutdown = shared.shutdown;

/// Required networking/control opcodes; optional file operations fall back to workers.
const required_ops = [_]linux.IORING_OP{
    .POLL_ADD,
    .SENDMSG,
    .RECVMSG,
    .ACCEPT,
    .ASYNC_CANCEL,
    .LINK_TIMEOUT,
    .CONNECT,
    .CLOSE,
    .SEND,
    .RECV,
};

fn initUring(options: Options) !Loop {
    var ring = try linux.IoUring.init(options.entries, 0);
    errdefer ring.deinit();

    // Live requests can exceed CQ capacity; limiting them can strand their dependencies.
    if (ring.features & linux.IORING_FEAT_NODROP == 0) return error.SystemOutdated;
    // Timers bound the blocking enter itself; see `timedEnter`.
    if (ring.features & linux.IORING_FEAT_EXT_ARG == 0) return error.SystemOutdated;

    // NODROP arrived in 5.5, SEND/RECV/CLOSE and the probe in 5.6; 5.5 rejects the probe.
    const probe = ring.get_probe() catch |err| switch (err) {
        error.ArgumentsInvalid => return error.SystemOutdated,
        else => return err,
    };
    if (!supportsAll(probe)) return error.SystemOutdated;
    if (options.buffers.len != 0 and
        (!probe.is_supported(.READ_FIXED) or !probe.is_supported(.WRITE_FIXED)))
        return error.FixedBuffersUnsupported;

    return .{
        .ring = ring,
        .file_ops = .{
            .read = probe.is_supported(.READ),
            .write = probe.is_supported(.WRITE),
            .sync = probe.is_supported(.FSYNC),
        },
    };
}

fn supportsAll(probe: linux.io_uring_probe) bool {
    for (required_ops) |op| {
        if (!probe.is_supported(op)) return false;
    }

    return true;
}

fn close(loop: *Loop) void {
    assert(loop.cancellations.first == null and loop.outstanding_cqes == 0);
    assert(loop.timers.root == null);

    if (loop.epoll) |*state| Epoll.deinit(state) else loop.ring.deinit();
    _ = linux.close(loop.wake_fd);
}

pub fn cancel(loop: *Loop, op: *Operation) void {
    if (loop.epoll) |*state| return Epoll.cancel(state, op);

    if (op.data == .timeout) {
        loop.finishTimer(op, .CANCELED, error.OperationCanceled);
        return;
    }

    assert(op.native.uring.cancel == .none);
    op.native.uring.cancel = .pending;
    // Inflight operations have already left pending, so cancellation can reuse their node.
    loop.cancellations.append(&op.node);
}

fn turn(loop: *Loop) !bool {
    if (loop.epoll) |*state| return Epoll.turn(loop, state);

    loop.wake_received = false;
    try loop.completions(0);
    if (loop.wake_received) return true;

    try loop.prepare();
    try loop.expire();
    if (try loop.submitPending()) {
        try loop.completions(0);
        return true;
    }

    return loop.cancellations.first != null;
}

fn inflight(loop: *const Loop) bool {
    if (loop.epoll) |*state| return state.live != 0;
    return loop.outstanding_cqes > 0 or loop.timers.root != null;
}

fn park(loop: *Loop) !void {
    if (loop.epoll) |*state| return Epoll.park(loop, state);

    // A full SQ can defer arming; never block where a post cannot wake us.
    if (!loop.wake_armed) return;
    if (loop.timers.root == null) return loop.completions(1);

    _ = loop.timedEnter(0) catch |err| switch (err) {
        error.SignalInterrupt => {},
        else => return err,
    };
    try loop.completions(0);
}

fn submitPending(loop: *Loop) !bool {
    // Waiting in the submitting enter saves a syscall per blocking iteration.
    const parked = loop.ring.sq_ready() > 0 and loop.canWait();
    if (parked) loop.executor.stats.parks += 1;

    var wait: u32 = @intFromBool(parked);
    var waited = false;

    // sq_ready includes previously flushed SQEs left by partial submissions.
    while (loop.ring.sq_ready() > 0) {
        const submitted = loop.submit(wait) catch |err| switch (err) {
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

fn submit(loop: *Loop, wait: u32) !u32 {
    if (wait == 0 or loop.timers.root == null) return loop.ring.submit_and_wait(wait);
    return loop.timedEnter(loop.ring.flush_sq());
}

/// Waits for one completion, but no longer than the nearest timer. The timeout travels in
/// the call, leaving nothing in the kernel to cancel; std's wrappers cannot pass it.
fn timedEnter(loop: *Loop, to_submit: u32) !u32 {
    const nearest = loop.timers.getMin().?.key.ns;
    const remaining = @max(0, nearest - Epoll.nanoseconds(try Uring.now()));
    var time: linux.kernel_timespec = .{
        .sec = @intCast(@divTrunc(remaining, std.time.ns_per_s)),
        .nsec = @intCast(@rem(remaining, std.time.ns_per_s)),
    };
    var arg: linux.io_uring_getevents_arg = .{
        .sigmask = 0,
        .sigmask_sz = linux.NSIG / 8,
        .pad = 0,
        .ts = @intFromPtr(&time),
    };

    const rc = linux.syscall6(
        .io_uring_enter,
        @as(u32, @bitCast(loop.ring.fd)),
        to_submit,
        1,
        linux.IORING_ENTER_GETEVENTS | linux.IORING_ENTER_EXT_ARG,
        @intFromPtr(&arg),
        @sizeOf(linux.io_uring_getevents_arg),
    );
    return switch (linux.errno(rc)) {
        .SUCCESS => @intCast(rc),
        .TIME => 0,
        .INTR => error.SignalInterrupt,
        .BUSY => error.CompletionQueueOvercommitted,
        .AGAIN => error.SystemResources,
        else => |e| Uring.mapError(e),
    };
}

fn startTimer(loop: *Loop, op: *Operation, current: i128) void {
    op.state = .inflight;

    if (op.time.sec < 0 or op.time.nsec < 0 or op.time.nsec >= std.time.ns_per_s) {
        op.primary = -@as(i32, @backingInt(linux.E.INVAL));
        op.finish(error.InvalidArgument);
        return;
    }

    op.native = .{ .timer = undefined };
    var entry = loop.timers.getEntryFor(.{ .ns = current + Epoll.nanoseconds(op.time), .op = @intFromPtr(op) });
    entry.set(&op.native.timer);
}

fn finishTimer(loop: *Loop, op: *Operation, e: linux.E, result: Error!usize) void {
    var entry = loop.timers.getEntryForExisting(&op.native.timer);
    entry.set(null);

    op.primary = -@as(i32, @backingInt(e));
    op.finish(result);
}

fn expire(loop: *Loop) !void {
    if (loop.timers.root == null) return;

    const current = Epoll.nanoseconds(try Uring.now());
    while (loop.timers.getMin()) |nearest| {
        if (nearest.key.ns > current) return;
        loop.finishTimer(@ptrFromInt(nearest.key.op), .TIME, 0);
    }
}

fn canWait(loop: *Loop) bool {
    return loop.executor.ready.first == null and loop.pending.first == null and
        loop.cancellations.first == null and loop.wake_armed and
        (loop.inflight() or loop.control.hasWork());
}

fn prepare(loop: *Loop) !void {
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
        const op: *Operation = @fieldParentPtr("node", node);

        sqe.prep_cancel(@intFromPtr(op), 0);
        sqe.user_data = @intFromPtr(op) | 2;

        loop.cancellations.remove(node);
        op.native.uring.cancel = .submitted;
        loop.outstanding_cqes += 1;
    }

    var current: ?i128 = null;
    while (loop.pending.first) |node| {
        const op: *Operation = @fieldParentPtr("node", node);

        if (op.data == .timeout) {
            loop.pending.remove(node);
            current = current orelse Epoll.nanoseconds(try Uring.now());
            loop.startTimer(op, current.?);
            continue;
        }

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
        if (tag == 2) Uring.cancelComplete(op) else op.complete(tag == 1, cqe.res);
    }

    if (wake_failed) return error.WakePollFailed;
}

test "io_uring probe requires networking opcodes but not optional files" {
    var probe = std.mem.zeroInit(linux.io_uring_probe, .{
        .last_op = .RECV,
        .ops_len = @backingInt(linux.IORING_OP.RECV) + 1,
    });
    for (required_ops) |op| probe.ops[@backingInt(op)].flags = linux.IO_URING_OP_SUPPORTED;

    try std.testing.expect(!probe.is_supported(.READ));
    try std.testing.expect(!probe.is_supported(.WRITE));
    try std.testing.expect(!probe.is_supported(.FSYNC));
    try std.testing.expect(supportsAll(probe));

    // Like 5.5: CONNECT is the newest opcode, so SEND, RECV, and CLOSE are missing.
    probe.last_op = .CONNECT;
    try std.testing.expect(!supportsAll(probe));

    probe.last_op = .RECV;
    probe.ops[@backingInt(linux.IORING_OP.SEND)].flags = 0;
    try std.testing.expect(!supportsAll(probe));
}

test "io-wq affinity mask supports sparse CPU IDs and validates overflow" {
    var mask = try iowqMask(std.testing.allocator, &.{ 0, 63, 64, 4097, 4097 });
    defer mask.deinit(std.testing.allocator);
    try std.testing.expectEqual(4, mask.count());
    for ([_]usize{ 0, 63, 64, 4097 }) |cpu| try std.testing.expect(mask.isSet(cpu));
    try std.testing.expect(!mask.isSet(4096));
    try std.testing.expectEqual(0, mask.bit_length % @bitSizeOf(usize));
    try std.testing.expectError(error.Overflow, iowqMask(std.testing.allocator, &.{std.math.maxInt(usize)}));
    if (@bitSizeOf(usize) > 32) {
        try std.testing.expectError(error.Overflow, iowqMask(std.testing.allocator, &.{@as(usize, std.math.maxInt(u32)) * 8}));
    }
}

test "io-wq affinity registration errno contract" {
    try checkIowqAffinity(.SUCCESS);
    for ([_]linux.E{ .NOSYS, .OPNOTSUPP, .INVAL }) |e|
        try std.testing.expectError(error.IowqAffinityUnsupported, checkIowqAffinity(e));
    for ([_]linux.E{ .PERM, .ACCES }) |e|
        try std.testing.expectError(error.PermissionDenied, checkIowqAffinity(e));
    try std.testing.expectError(error.SystemResources, checkIowqAffinity(.NOMEM));
    try std.testing.expectError(error.BadFileDescriptor, checkIowqAffinity(.BADF));
}
