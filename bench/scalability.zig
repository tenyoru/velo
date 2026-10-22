const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;

const Backend = enum { auto, uring, epoll };
const Workload = enum { @"idle-recv", @"idle-deadline", @"idle-timeout", @"same-fd", @"expire-one", @"expire-all" };
const expiration_ns = 2_000_000_000;
const long_seconds = 3600;
const deadline_seed: usize = 0x6a09e667;

const Config = struct {
    backend: Backend,
    workload: Workload,
    n: usize,
    steps: usize = 10_000,

    fn parse(args: []const []const u8) !Config {
        if (args.len < 3 or args.len > 4) return error.Usage;
        const backend = std.meta.stringToEnum(Backend, args[0]) orelse return error.InvalidBackend;
        if (builtin.os.tag != .linux and backend != .auto) return error.UnsupportedBackend;
        const workload = std.meta.stringToEnum(Workload, args[1]) orelse return error.InvalidWorkload;
        const n = std.fmt.parseInt(usize, args[2], 10) catch return error.InvalidN;
        if (n == 0 or n > 10_000) return error.InvalidN;
        const steps = if (args.len == 4)
            std.fmt.parseInt(usize, args[3], 10) catch return error.InvalidSteps
        else
            10_000;
        if (steps == 0 or steps > 1_000_000_000) return error.InvalidSteps;
        if (args.len == 4 and (workload == .@"expire-one" or workload == .@"expire-all"))
            return error.InvalidSteps;
        return .{ .backend = backend, .workload = workload, .n = n, .steps = steps };
    }
};

const Counts = struct {
    admitted: usize = 0,
    completed: usize = 0,
    expired: usize = 0,
    canceled: usize = 0,
    cancel_callbacks: usize = 0,
    unexpected: usize = 0,
    expiration_target: usize,
};

const Waiter = struct {
    wait: union(enum) { recv: io.Recv, timeout: io.Timeout },
    counts: *Counts,
    due: bool,

    fn operation(self: *Waiter) *io.Operation {
        return switch (self.wait) {
            inline else => |*op| &op.operation,
        };
    }

    pub fn poll(self: *Waiter, task: *co.Task) co.Poll(void) {
        const first = self.operation().state == .idle;
        switch (self.wait) {
            inline else => |*op| switch (op.poll(task)) {
                .pending => {
                    if (first) self.counts.admitted += 1;
                    return .pending;
                },
                .ready => |result| {
                    self.counts.completed += 1;
                    if (result) |_| {
                        if (self.due) {
                            self.counts.expired += 1;
                        } else self.counts.unexpected += 1;
                    } else |err| {
                        if (err == error.OperationCanceled)
                            self.counts.canceled += 1
                        else
                            self.counts.unexpected += 1;
                    }
                    if (!self.operation().loop.control.isShuttingDown() and
                        (self.counts.unexpected != 0 or
                            (self.due and self.counts.expired == self.counts.expiration_target)))
                        self.operation().loop.stop();
                    return .{ .ready = {} };
                },
            },
        }
    }

    pub fn cancel(self: *Waiter) void {
        self.counts.cancel_callbacks += 1;
        switch (self.wait) {
            inline else => |*op| op.cancel(),
        }
    }
};

const Frame = co.Frame(Waiter);

const Probe = struct {
    task: co.Task = .{ .poll_fn = poll },
    loop: *io.Loop,
    counts: *Counts,
    last: ?*io.Operation = null,
    target: usize = 0,
    ready_steps: usize = 0,

    fn poll(task: *co.Task) bool {
        const self: *Probe = @alignCast(@fieldParentPtr("task", task));
        const done = if (self.last) |last|
            last.state == .inflight and self.loop.pending.first == null
        else blk: {
            self.ready_steps += 1;
            break :blk self.ready_steps == self.target;
        };
        if (done or self.counts.completed != 0 or task.cancel_requested) {
            if (!self.loop.control.isShuttingDown()) self.loop.stop();
            return true;
        }
        task.wake();
        return false;
    }
};

const Report = struct {
    backend_requested: []const u8,
    backend: []const u8,
    deadline_seed: ?usize,
    workload: []const u8,
    n: usize,
    tcp_pairs: usize,
    admitted: usize,
    active_at_checkpoint: usize,
    ready_at_checkpoint: usize,
    fixed_ready_work_steps: usize,
    expired: usize,
    cancel_callbacks: usize,
    canceled: usize,
    completed: usize,
    admission_ns: i128,
    fixed_ready_work_ns: ?i128,
    expiration_observed_ns: ?i128,
    expiration_duration_ns: ?i128,
    expiration_wait_ns: ?i128,
    cancel_request_ns: i128,
    shutdown_drain_ns: i128,
    loop_bytes: usize = @sizeOf(io.Loop),
    operation_bytes: usize = @sizeOf(io.Operation),
    frame_bytes: usize = @sizeOf(Frame),
    probe_bytes: usize = @sizeOf(Probe),
    drained: bool = true,
};

fn now() i128 {
    const time = io.now() catch |err| std.debug.panic("io.now: {s}", .{@errorName(err)});
    return nanoseconds(time);
}

fn nanoseconds(time: io.Timespec) i128 {
    return @as(i128, time.sec) * 1_000_000_000 + time.nsec;
}

fn timespec(ns: i128) io.Timespec {
    return .{ .sec = @intCast(@divTrunc(ns, 1_000_000_000)), .nsec = @intCast(@mod(ns, 1_000_000_000)) };
}

fn drive(loop: *io.Loop) void {
    // A run error retains live borrows; never unwind the pinned storage.
    loop.run() catch |err| std.debug.panic("loop.run: {s}", .{@errorName(err)});
}

fn close(fd: io.Socket) void {
    io.closeSocket(fd) catch |err| std.debug.panic("closeSocket: {s}", .{@errorName(err)});
}

fn tcpPair(loop: *io.Loop) ![2]io.Socket {
    const listener = try io.tcpSocket(.ipv4);
    defer close(listener);
    var address: io.SockAddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    var len: io.SockLen = @sizeOf(@TypeOf(address));
    try io.bind(listener, @ptrCast(&address), len);
    try io.listen(listener, 1);
    try io.localAddress(listener, @ptrCast(&address), &len);
    const client = try io.tcpSocket(.ipv4);
    errdefer close(client);
    const deadline = timespec(now() + 3_000_000_000);
    var accept: co.Frame(io.Accept) = .{ .coroutine = io.Accept.init(loop, .{
        .fd = listener,
        .deadline = deadline,
    }) };
    var connect: co.Frame(io.Connect) = .{ .coroutine = io.Connect.init(loop, .{
        .fd = client,
        .address = @ptrCast(&address),
        .address_len = len,
        .deadline = deadline,
    }) };
    loop.executor.spawn(&accept.task);
    loop.executor.spawn(&connect.task);
    drive(loop);
    defer accept.coroutine.deinit();
    defer connect.coroutine.deinit();
    const server = try accept.getResult();
    errdefer close(server);
    try connect.getResult();
    return .{ client, server };
}

fn pending(frames: []Frame, counts: *const Counts) !void {
    if (counts.admitted != frames.len or counts.completed != 0) return error.EarlyCompletion;
    for (frames) |*frame| {
        if (frame.coroutine.operation().state != .inflight or frame.result != null or
            frame.task.outstanding_io != 1) return error.NotActive;
    }
}

fn drained(loop: *io.Loop, frames: []Frame) !void {
    if (loop.active != 0 or loop.pending.first != null or loop.executor.ready.first != null or
        loop.executor.all_tasks.first != null or loop.control.registrations.first != null)
        return error.NotDrained;
    if (builtin.os.tag == .linux) {
        if (loop.outstanding_cqes != 0 or loop.cancellations.first != null) return error.NotDrained;
        if (loop.epoll) |*state| {
            if (state.live != 0) return error.NotDrained;
        }
    } else if (loop.live.first != null) return error.NotDrained;
    for (frames) |*frame| {
        if (frame.coroutine.operation().state != .done or frame.result == null or
            frame.task.outstanding_io != 0 or frame.task.registered or frame.task.state != .done)
            return error.NotDrained;
    }
}

fn run(gpa: std.mem.Allocator, system_io: std.Io, config: Config) !Report {
    var loop = if (builtin.os.tag == .linux)
        try io.Loop.initWithBackend(.{ .entries = 256 }, switch (config.backend) {
            .auto => .auto,
            .uring => .uring,
            .epoll => .epoll,
        })
    else blk: {
        if (config.backend != .auto) return error.UnsupportedBackend;
        break :blk try io.Loop.init(.{ .entries = 256 });
    };
    defer loop.deinit();
    if (builtin.os.tag == .linux) {
        if (loop.epoll) |*state| {
            // Address-seeded priorities would make ASLR change the work between runs.
            state.deadlines.prng.xorshift = deadline_seed;
        }
    }
    const backend = if (builtin.os.tag == .linux)
        (if (loop.epoll != null) "epoll" else "uring")
    else if (builtin.os.tag == .macos)
        "kqueue"
    else
        "iocp";
    const expiring = config.workload == .@"expire-one" or config.workload == .@"expire-all";
    const timers = expiring or config.workload == .@"idle-timeout";
    const pair_count = if (timers) 0 else if (config.workload == .@"same-fd") 1 else config.n;
    const frames = try gpa.alloc(Frame, config.n);
    defer gpa.free(frames);
    const buffers = try gpa.alloc(u8, if (timers) 0 else config.n);
    defer gpa.free(buffers);
    const pairs = try gpa.alloc([2]io.Socket, pair_count);
    defer gpa.free(pairs);
    var pairs_ready: usize = 0;
    defer for (pairs[0..pairs_ready]) |pair| for (pair) |fd| close(fd);
    while (pairs_ready < pairs.len) : (pairs_ready += 1) pairs[pairs_ready] = try tcpPair(&loop);

    var counts: Counts = .{ .expiration_target = if (!expiring) 0 else if (config.workload == .@"expire-one") 1 else config.n };
    const long_deadline = timespec(now() + long_seconds * 1_000_000_000);
    for (frames, 0..) |*frame, i| {
        const due = expiring and (config.workload == .@"expire-all" or i == 0);
        frame.* = .{ .coroutine = .{
            .counts = &counts,
            .due = due,
            .wait = if (timers)
                .{ .timeout = io.Timeout.init(&loop, .{ .duration = if (due) timespec(expiration_ns) else .{ .sec = long_seconds, .nsec = 0 } }) }
            else
                .{ .recv = io.Recv.init(&loop, .{
                    .fd = pairs[if (config.workload == .@"same-fd") 0 else i][0],
                    .buffer = buffers[i .. i + 1],
                    .deadline = if (config.workload == .@"idle-deadline") long_deadline else null,
                }) },
        } };
    }
    defer {
        loop.shutdown();
        drive(&loop);
        drained(&loop, frames) catch @panic("cleanup left live borrows");
    }

    var admission: Probe = .{ .loop = &loop, .counts = &counts, .last = frames[frames.len - 1].coroutine.operation() };
    const admission_start = now();
    for (frames) |*frame| loop.executor.spawn(&frame.task);
    loop.executor.spawn(&admission.task);
    drive(&loop);
    const admission_end = now();
    if (expiring and admission_end >= admission_start + expiration_ns) return error.ExpiredDuringAdmission;
    try pending(frames, &counts);
    if (loop.active != config.n or admission.task.state != .done) return error.NotActive;
    const active_at_checkpoint = loop.active;

    var ready: Probe = .{ .loop = &loop, .counts = &counts, .target = config.steps };
    var ready_ns: ?i128 = null;
    var expiration_observed_ns: ?i128 = null;
    var expiration_wait_ns: ?i128 = null;
    var ready_steps: usize = 0;
    if (expiring) {
        const wait_start = now();
        const until = admission_end + expiration_ns + 10_000_000;
        while (true) {
            const remaining = until - now();
            if (remaining <= 0) break;
            try std.Io.sleep(system_io, .fromNanoseconds(@intCast(remaining)), .awake);
        }
        expiration_wait_ns = now() - wait_start;
        const start = now();
        drive(&loop);
        expiration_observed_ns = now() - start;
        if (counts.expired != counts.expiration_target or counts.completed != counts.expired or counts.unexpected != 0)
            return error.IncorrectExpirationCount;
    } else {
        const start = now();
        loop.executor.spawn(&ready.task);
        drive(&loop);
        ready_ns = now() - start;
        ready_steps = ready.ready_steps;
        try pending(frames, &counts);
        if (ready_steps != config.steps or ready.task.state != .done) return error.IncorrectReadyCount;
    }

    const completed_before_cancel = counts.completed;
    const cancel_start = now();
    loop.executor.cancelAll();
    const cancel_ns = now() - cancel_start;
    if (counts.completed != completed_before_cancel) return error.CompletionDuringCancelRequest;
    const drain_start = now();
    loop.shutdown();
    drive(&loop);
    const drain_ns = now() - drain_start;
    try drained(&loop, frames);
    if (counts.completed != config.n or counts.canceled != config.n - counts.expired or
        counts.cancel_callbacks != counts.canceled or counts.unexpected != 0)
        return error.IncorrectCompletionCount;

    return .{
        .backend_requested = @tagName(config.backend),
        .backend = backend,
        .deadline_seed = if (builtin.os.tag == .linux and loop.epoll != null) deadline_seed else null,
        .workload = @tagName(config.workload),
        .n = config.n,
        .tcp_pairs = pair_count,
        .admitted = counts.admitted,
        .active_at_checkpoint = active_at_checkpoint,
        .ready_at_checkpoint = 0,
        .fixed_ready_work_steps = ready_steps,
        .expired = counts.expired,
        .cancel_callbacks = counts.cancel_callbacks,
        .canceled = counts.canceled,
        .completed = counts.completed,
        .admission_ns = admission_end - admission_start,
        .fixed_ready_work_ns = ready_ns,
        .expiration_observed_ns = expiration_observed_ns,
        .expiration_duration_ns = if (expiring) expiration_ns else null,
        .expiration_wait_ns = expiration_wait_ns,
        .cancel_request_ns = cancel_ns,
        .shutdown_drain_ns = drain_ns,
    };
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const config = Config.parse(args[1..]) catch |err| {
        std.debug.print("usage: scalability <auto|uring|epoll> <idle-recv|idle-deadline|idle-timeout|same-fd|expire-one|expire-all> <1..10000> [steps]\n", .{});
        return err;
    };
    const report = try run(init.gpa, init.io, config);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try std.json.Stringify.value(report, .{}, &stdout.interface);
    try stdout.interface.writeByte('\n');
    try stdout.interface.flush();
}

test "small workloads preserve admission, fixed ready work, cancellation, and expiration boundaries" {
    const modes = if (builtin.os.tag == .linux) [_]Backend{ .auto, .epoll } else [_]Backend{.auto};
    for (modes) |backend| {
        for (std.enums.values(Workload)) |workload| {
            const result = try run(std.testing.allocator, std.testing.io, .{ .backend = backend, .workload = workload, .n = 4, .steps = 137 });
            const expiring = workload == .@"expire-one" or workload == .@"expire-all";
            const expired: usize = if (workload == .@"expire-one") 1 else if (workload == .@"expire-all") 4 else 0;
            try std.testing.expectEqual(4, result.admitted);
            try std.testing.expectEqual(4, result.active_at_checkpoint);
            try std.testing.expectEqual(0, result.ready_at_checkpoint);
            try std.testing.expectEqual(if (expiring) @as(usize, 0) else 137, result.fixed_ready_work_steps);
            try std.testing.expectEqual(expired, result.expired);
            try std.testing.expectEqual(4 - expired, result.cancel_callbacks);
            try std.testing.expectEqual(4 - expired, result.canceled);
            try std.testing.expectEqual(4, result.completed);
            try std.testing.expectEqual(expiring, result.expiration_observed_ns != null);
            try std.testing.expectEqual(!expiring, result.fixed_ready_work_ns != null);
            try std.testing.expect(result.drained);
            if (backend == .epoll) try std.testing.expectEqualStrings("epoll", result.backend);
        }
        const pressure = try run(std.testing.allocator, std.testing.io, .{ .backend = backend, .workload = .@"idle-timeout", .n = 257, .steps = 3 });
        try std.testing.expectEqual(257, pressure.admitted);
        try std.testing.expectEqual(257, pressure.canceled);
        try std.testing.expectEqual(3, pressure.fixed_ready_work_steps);
    }
}

test "CLI rejects invalid arguments" {
    try std.testing.expectError(error.Usage, Config.parse(&.{}));
    try std.testing.expectError(error.InvalidBackend, Config.parse(&.{ "invalid", "idle-recv", "4" }));
    try std.testing.expectError(error.InvalidWorkload, Config.parse(&.{ "auto", "invalid", "4" }));
    for ([_][]const u8{ "0", "10001", "-1", "abc" }) |n|
        try std.testing.expectError(error.InvalidN, Config.parse(&.{ "auto", "idle-recv", n }));
    try std.testing.expectError(error.InvalidSteps, Config.parse(&.{ "auto", "idle-recv", "4", "0" }));
    try std.testing.expectError(error.InvalidSteps, Config.parse(&.{ "auto", "expire-all", "4", "10" }));
    const parsed = try Config.parse(&.{ "auto", "idle-recv", "10000" });
    try std.testing.expectEqual(10_000, parsed.n);
    try std.testing.expectEqual(10_000, parsed.steps);
    if (builtin.os.tag != .linux)
        try std.testing.expectError(error.UnsupportedBackend, Config.parse(&.{ "epoll", "idle-recv", "4" }));
}
