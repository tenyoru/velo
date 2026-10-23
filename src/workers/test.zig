const std = @import("std");
const builtin = @import("builtin");
const io = @import("../event/root.zig");
const co = @import("../coroutines/root.zig");
const WorkerPool = @import("Pool.zig");
const Group = @import("Group.zig");
const support = @import("../event/tests/support.zig");
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

test "group runs one loop per thread and posts between loops" {
    const Context = struct {
        group: *Group,
        peers: []Peer,

        const Self = @This();
        const Peer = struct {
            entry_thread: ?std.Thread.Id = null,
            callback_thread: ?std.Thread.Id = null,
            calls: usize = 0,
            done: std.Io.Event = .unset,
            post: io.Control.Post = .{ .callback = callback },
            frame: co.Frame(Sender) = undefined,

            fn callback(post: *io.Control.Post) void {
                const self: *@This() = @fieldParentPtr("post", post);
                self.callback_thread = std.Thread.getCurrentId();
                self.done.set(testing.io);
            }
        };
        const Sender = struct {
            context: *Self,
            index: usize,

            pub fn poll(self: *@This(), _: *co.Task) co.Poll(void) {
                const context = self.context;
                const destination = (self.index + 1) % context.peers.len;
                const next = context.group.loop(destination);
                next.retain();
                next.control.postRelease(&context.peers[destination].post);

                return .{ .ready = {} };
            }
        };

        fn entry(self: *Self, loop: *io.Loop, index: usize) void {
            const peer = &self.peers[index];
            peer.entry_thread = std.Thread.getCurrentId();
            peer.calls += 1;
            peer.frame = .{ .coroutine = .{ .context = self, .index = index } };
            loop.executor.spawn(&peer.frame.task);
        }
    };

    for (support.modes) |mode| {
        for ([_]?usize{ 1, 2, 4, null }) |count| {
            const n = count orelse try std.Thread.getCpuCount();
            const peers = try testing.allocator.alloc(Context.Peer, n);
            defer testing.allocator.free(peers);
            for (peers) |*peer| peer.* = .{};

            var group: Group = undefined;
            var context: Context = .{ .group = &group, .peers = peers };
            try group.init(testing.allocator, .{
                .count = count,
                .loop = .{ .entries = 8 },
                .backend = if (builtin.os.tag == .linux) switch (mode) {
                    .auto => .auto,
                    .epoll => .epoll,
                } else .auto,
            }, &context, Context.entry);

            for (peers) |*peer| peer.done.waitUncancelable(testing.io);
            group.deinit();

            for (peers, 0..) |peer, i| {
                try testing.expectEqual(1, peer.calls);
                try testing.expect(peer.entry_thread.? != std.Thread.getCurrentId());
                try testing.expectEqual(peer.entry_thread, peer.callback_thread);
                for (peers[0..i]) |previous| try testing.expect(previous.entry_thread != peer.entry_thread);
                try testing.expect(peer.frame.result != null);
            }
        }
    }
}

test "group startup failure cancels started loops" {
    const Context = struct {
        events: [3]co.Event = @splat(.{}),
        frames: [3]co.Frame(co.Event.Wait) = undefined,

        fn entry(self: *@This(), loop: *io.Loop, index: usize) error{Injected}!void {
            self.frames[index] = .{ .coroutine = self.events[index].wait() };
            loop.executor.spawn(&self.frames[index].task);
            if (index == 1) return error.Injected;
        }

        fn never(calls: *std.atomic.Value(usize), _: *io.Loop, _: usize) void {
            _ = calls.fetchAdd(1, .monotonic);
        }
    };

    for (support.modes) |mode| {
        var group: Group = undefined;
        var context: Context = .{};
        try testing.expectError(error.Injected, group.init(testing.allocator, .{
            .count = 3,
            .backend = if (builtin.os.tag == .linux) switch (mode) {
                .auto => .auto,
                .epoll => .epoll,
            } else .auto,
        }, &context, Context.entry));

        for (&context.frames, &context.events) |*frame, *event| {
            try testing.expectError(error.Canceled, frame.getResult());
            event.deinit();
        }
    }

    if (builtin.os.tag == .linux) {
        var group: Group = undefined;
        var calls: std.atomic.Value(usize) = .init(0);
        try testing.expectError(error.EntriesNotPowerOfTwo, group.init(testing.allocator, .{
            .count = 3,
            .loop = .{ .entries = 3 },
        }, &calls, Context.never));
        try testing.expectEqual(0, calls.load(.acquire));
    }
}

test "group pins each thread to one allowed CPU" {
    const Noop = struct {
        fn entry(_: void, _: *io.Loop, _: usize) void {}
    };

    var group: Group = undefined;
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            const allowed = try std.posix.sched_getaffinity(0);
            const count = @min(2, linux.CPU_COUNT(allowed));
            const Context = struct {
                observed: [2]linux.cpu_set_t = undefined,

                fn entry(self: *@This(), _: *io.Loop, index: usize) !void {
                    self.observed[index] = try std.posix.sched_getaffinity(0);
                }
            };
            var context: Context = .{};
            try group.init(testing.allocator, .{ .count = count, .pin = true }, &context, Context.entry);
            group.deinit();

            var cpu: usize = 0;
            for (context.observed[0..count]) |observed| {
                while (allowed[cpu / @bitSizeOf(usize)] & (@as(usize, 1) << @intCast(cpu % @bitSizeOf(usize))) == 0) cpu += 1;
                var expected: linux.cpu_set_t = @splat(0);
                expected[cpu / @bitSizeOf(usize)] = @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));

                try testing.expectEqual(1, linux.CPU_COUNT(observed));
                try testing.expectEqualDeep(expected, observed);
                cpu += 1;
            }
            if (count == 2) try testing.expect(!std.mem.eql(usize, &context.observed[0], &context.observed[1]));

            try testing.expectError(error.NotEnoughCpus, group.init(testing.allocator, .{
                .count = @as(usize, linux.CPU_COUNT(allowed)) + 1,
                .pin = true,
            }, {}, Noop.entry));
        },
        .windows => {
            try group.init(testing.allocator, .{ .count = 1, .pin = true }, {}, Noop.entry);
            group.deinit();
        },
        else => try testing.expectError(error.Unsupported, group.init(testing.allocator, .{ .pin = true }, {}, Noop.entry)),
    }
}

test "group names threads and rejects invalid options" {
    const Context = struct {
        names: [2][std.Thread.max_name_len + 1]u8 = undefined,

        fn entry(self: *@This(), _: *io.Loop, index: usize) !void {
            _ = try std.posix.prctl(.GET_NAME, .{@intFromPtr(&self.names[index])});
        }

        fn noop(_: void, _: *io.Loop, _: usize) void {}
    };

    var group: Group = undefined;
    if (builtin.os.tag == .linux) {
        var context: Context = .{};
        try group.init(testing.allocator, .{ .count = 2 }, &context, Context.entry);
        group.deinit();

        try testing.expectEqualStrings("velo-0", std.mem.sliceTo(&context.names[0], 0));
        try testing.expectEqualStrings("velo-1", std.mem.sliceTo(&context.names[1], 0));
    }

    try testing.expectError(error.InvalidThreadCount, group.init(testing.allocator, .{ .count = 0 }, {}, Context.noop));
    const long_name: [std.Thread.max_name_len]u8 = @splat('x');
    try testing.expectError(error.NameTooLong, group.init(testing.allocator, .{
        .count = 1,
        .name = &long_name,
    }, {}, Context.noop));
}
