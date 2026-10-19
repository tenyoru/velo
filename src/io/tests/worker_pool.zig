const std = @import("std");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const WorkerPool = @import("../WorkerPool.zig");
const support = @import("support.zig");
const testing = std.testing;

const Gated = struct {
    started: *std.Io.Event,
    release: *std.Io.Event,
    calls: *std.atomic.Value(usize),
    value: usize,

    pub fn run(self: *@This(), std_io: std.Io) error{}!usize {
        _ = self.calls.fetchAdd(1, .monotonic);
        self.started.set(std_io);
        self.release.waitUncancelable(std_io);

        return self.value;
    }
};

test "worker pool rejects zero workers and joins unused workers" {
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: WorkerPool = undefined;

    try testing.expectError(error.InvalidWorkerCount, pool.init(testing.allocator, &loop, 0));

    try pool.init(testing.allocator, &loop, 2);

    pool.deinit();
}

test "worker queued cancellation skips action and running cancellation retains result" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 1);
        defer pool.deinit();

        const std_io = pool.threaded.io();
        var started: std.Io.Event = .unset;
        var release: std.Io.Event = .unset;
        var calls: std.atomic.Value(usize) = .init(0);
        const W = WorkerPool.Work(Gated);
        var running: co.Frame(W) = .{
            .coroutine = W.init(&pool, .{
                .started = &started,
                .release = &release,
                .calls = &calls,
                .value = 17,
            }),
        };
        var queued: co.Frame(W) = .{
            .coroutine = W.init(&pool, .{
                .started = &started,
                .release = &release,
                .calls = &calls,
                .value = 99,
            }),
        };

        loop.executor.spawn(&running.task);
        loop.executor.spawn(&queued.task);
        loop.executor.run();
        started.waitUncancelable(std_io);
        queued.coroutine.cancel();
        queued.coroutine.cancel();
        running.coroutine.cancel();

        try testing.expectEqual(1, running.task.outstanding_io);
        try testing.expectEqual(1, queued.task.outstanding_io);
        try testing.expectEqual(2, pool.outstanding);

        release.set(std_io);
        support.run(&loop);

        try testing.expectEqual(17, try running.getResult());
        try testing.expectError(error.OperationCanceled, queued.getResult());
        try testing.expectEqual(1, calls.load(.acquire));
        try testing.expectEqual(0, running.task.outstanding_io);
        try testing.expectEqual(0, queued.task.outstanding_io);
        try testing.expectEqual(0, pool.outstanding);
        try testing.expect(!loop.control.hasWork());
        try testing.expectEqual(null, loop.control.registrations.first);

        running.coroutine.deinit();
        queued.coroutine.deinit();
    }
}

test "worker task cancellation before submission never runs action" {
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var started: std.Io.Event = .unset;
    var release: std.Io.Event = .unset;
    var calls: std.atomic.Value(usize) = .init(0);
    const W = WorkerPool.Work(Gated);
    var frame: co.Frame(W) = .{
        .coroutine = W.init(&pool, .{
            .started = &started,
            .release = &release,
            .calls = &calls,
            .value = 0,
        }),
    };

    loop.executor.spawn(&frame.task);
    frame.task.cancel();
    support.run(&loop);

    try testing.expectError(error.OperationCanceled, frame.getResult());
    try testing.expectEqual(0, calls.load(.acquire));
    try testing.expectEqual(0, frame.task.outstanding_io);

    frame.coroutine.deinit();
}

const ReleaseOnTimer = struct {
    timer: io.Timeout,
    release: *std.Io.Event,
    std_io: std.Io,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
        const result = switch (self.timer.poll(task)) {
            .pending => return .pending,
            .ready => |result| result,
        };
        self.release.set(self.std_io);

        return .{ .ready = result };
    }
};

test "worker blocked action does not block native loop timer" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 1);
        defer pool.deinit();

        var started: std.Io.Event = .unset;
        var release: std.Io.Event = .unset;
        var calls: std.atomic.Value(usize) = .init(0);
        const W = WorkerPool.Work(Gated);
        var work: co.Frame(W) = .{
            .coroutine = W.init(&pool, .{
                .started = &started,
                .release = &release,
                .calls = &calls,
                .value = 42,
            }),
        };

        loop.executor.spawn(&work.task);
        loop.executor.run();
        started.waitUncancelable(pool.threaded.io());

        var timer: co.Frame(ReleaseOnTimer) = .{
            .coroutine = .{
                .timer = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 2_000_000 },
                }),
                .release = &release,
                .std_io = pool.threaded.io(),
            },
        };

        loop.executor.spawn(&timer.task);
        support.run(&loop);

        try timer.getResult();
        try testing.expectEqual(42, try work.getResult());
        try testing.expectEqual(0, work.task.outstanding_io);

        work.coroutine.deinit();
        timer.coroutine.timer.deinit();
    }
}

const Aligned = struct {
    started: *std.Io.Event,
    release: *std.Io.Event,
    calls: *std.atomic.Value(usize),
    value: usize align(128),

    const Result = struct { value: usize align(256) };

    pub fn run(self: *@This(), std_io: std.Io) error{}!Result {
        _ = self.calls.fetchAdd(1, .monotonic);
        self.started.set(std_io);
        self.release.waitUncancelable(std_io);

        return .{ .value = self.value };
    }
};

test "worker fixed concurrency drains many jobs with over-aligned actions and results" {
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 2);
    defer pool.deinit();

    var started: [12]std.Io.Event = @splat(.unset);
    var release: std.Io.Event = .unset;
    var calls: std.atomic.Value(usize) = .init(0);
    const W = WorkerPool.Work(Aligned);
    var frames: [12]co.Frame(W) = undefined;

    for (&frames, &started, 0..) |*frame, *event, i| {
        frame.* = .{
            .coroutine = W.init(&pool, .{
                .started = event,
                .release = &release,
                .calls = &calls,
                .value = i,
            }),
        };

        loop.executor.spawn(&frame.task);
    }

    loop.executor.run();
    started[0].waitUncancelable(pool.threaded.io());
    started[1].waitUncancelable(pool.threaded.io());

    try testing.expectEqual(2, calls.load(.acquire));

    frames[2].task.cancel();
    loop.executor.run();
    release.set(pool.threaded.io());
    support.run(&loop);

    for (&frames, 0..) |*frame, i| {
        if (i == 2) {
            try testing.expectError(error.OperationCanceled, frame.getResult());
        } else {
            try testing.expectEqual(i, (try frame.getResult()).value);
        }
        try testing.expectEqual(0, frame.task.outstanding_io);

        frame.coroutine.deinit();
    }

    try testing.expectEqual(11, calls.load(.acquire));
    try testing.expectEqual(0, pool.outstanding);
}

test "worker shutdown cancels queued jobs drains running results rejects new work and joins" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 1);
        defer pool.deinit();

        var started: std.Io.Event = .unset;
        var release: std.Io.Event = .unset;
        var calls: std.atomic.Value(usize) = .init(0);
        const W = WorkerPool.Work(Gated);
        var frames: [3]co.Frame(W) = undefined;
        for (&frames, 0..) |*frame, i| frame.* = .{
            .coroutine = W.init(&pool, .{
                .started = &started,
                .release = &release,
                .calls = &calls,
                .value = i,
            }),
        };

        loop.executor.spawn(&frames[0].task);
        loop.executor.spawn(&frames[1].task);
        loop.executor.run();
        started.waitUncancelable(pool.threaded.io());

        loop.control.shutdown();
        _ = loop.control.service(&loop.executor);
        loop.executor.spawn(&frames[2].task);
        loop.executor.run();
        release.set(pool.threaded.io());
        support.run(&loop);

        try testing.expectEqual(0, try frames[0].getResult());
        try testing.expectError(error.OperationCanceled, frames[1].getResult());
        try testing.expectError(error.OperationCanceled, frames[2].getResult());
        try testing.expectEqual(1, calls.load(.acquire));
        try testing.expect(!loop.control.hasWork());
        try testing.expectEqual(null, loop.control.registrations.first);

        for (&frames) |*frame| {
            try testing.expectEqual(0, frame.task.outstanding_io);

            frame.coroutine.deinit();
        }
    }
}
