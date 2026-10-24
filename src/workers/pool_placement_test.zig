const std = @import("std");
const builtin = @import("builtin");
const Pool = @import("Pool.zig");
const event = @import("../event/root.zig");
const co = @import("../coroutines/root.zig");
const numa = @import("numa/root.zig");
const support = @import("../event/tests/support.zig");
const testing = std.testing;

const AffinityAction = struct {
    expected: []const usize,
    started: *std.Io.Event,
    release: *std.Io.Event,

    pub fn run(self: *@This(), io: std.Io) !std.Thread.Id {
        self.started.set(io);
        self.release.waitUncancelable(io);
        const actual = try numa.allowedCpus(std.heap.page_allocator);
        defer std.heap.page_allocator.free(actual);
        try testing.expectEqualSlices(usize, self.expected, actual);
        return std.Thread.getCurrentId();
    }
};

const AffinityWork = struct {
    work: Pool.Work(AffinityAction),
    completed_on: ?std.Thread.Id = null,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!std.Thread.Id) {
        const result = switch (self.work.poll(task)) {
            .pending => return .pending,
            .ready => |result| result,
        };
        self.completed_on = std.Thread.getCurrentId();
        return .{ .ready = result };
    }
};

fn checkWorkers(pool: *Pool, expected: []const usize) !void {
    const count = pool.threads.len;
    var started: [3]std.Io.Event = @splat(.unset);
    var release: std.Io.Event = .unset;
    var storage: [3]co.Frame(AffinityWork) = undefined;
    const frames = storage[0..count];
    for (frames, started[0..count]) |*frame, *ready| {
        frame.* = .{ .coroutine = .{ .work = .init(pool, .{
            .expected = expected,
            .started = ready,
            .release = &release,
        }) } };
        pool.loop.executor.spawn(&frame.task);
    }
    pool.loop.executor.run();
    for (started[0..count]) |*ready| ready.waitUncancelable(pool.threaded.io());
    release.set(pool.threaded.io());
    support.run(pool.loop);
    defer for (frames) |*frame| frame.coroutine.work.deinit();

    for (frames, 0..) |*frame, i| {
        const worker = try frame.getResult();
        try testing.expect(worker != std.Thread.getCurrentId());
        try testing.expectEqual(std.Thread.getCurrentId(), frame.coroutine.completed_on.?);
        for (frames[0..i]) |*previous| try testing.expect(worker != try previous.getResult());
        try testing.expectEqual(0, frame.task.outstanding_io);
    }
    try testing.expectEqual(0, pool.outstanding);
    try testing.expect(!pool.loop.control.hasWork());
    try testing.expectEqual(null, pool.loop.control.registrations.first);
}

test "pool placement validates counts and portable defaults" {
    var loop = try event.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: Pool = undefined;
    try testing.expectError(error.InvalidWorkerCount, pool.initWithOptions(testing.allocator, &loop, .{ .count = 0 }));
    try testing.expectError(error.InvalidWorkerCount, pool.initWithOptions(testing.allocator, &loop, .{
        .count = 0,
        .cpus = &.{0},
    }));
    for ([_]usize{ 1, 3 }) |count| {
        try pool.initWithOptions(testing.allocator, &loop, .{ .count = count });
        try testing.expectEqual(count, pool.threads.len);
        pool.deinit();
    }
    if (builtin.os.tag != .linux) try testing.expectError(error.Unsupported, pool.initWithOptions(testing.failing_allocator, &loop, .{
        .count = 2,
        .cpus = &.{0},
    }));
}

test "pool placement widens inherited affinity and completes on the owning loop" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const before = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(before);
    try testing.expect(before.len != 0);
    const Runner = struct {
        fn run(cpus: []const usize, result: *anyerror!void) void {
            result.* = check(cpus);
        }

        fn check(cpus: []const usize) !void {
            try numa.pin(cpus[0..1]);
            var loop = try event.Loop.init(.{ .entries = 8 });
            defer loop.deinit();
            for (0..3) |mode| {
                for ([_]usize{ 1, 3 }) |count| {
                    var requested: [2]usize = undefined;
                    @memcpy(requested[0..cpus.len], cpus);
                    var pool: Pool = undefined;
                    if (mode == 0) {
                        try pool.init(testing.allocator, &loop, count);
                    } else {
                        try pool.initWithOptions(testing.allocator, &loop, .{
                            .count = count,
                            .cpus = if (mode == 2) requested[0..cpus.len] else &.{},
                        });
                    }
                    defer pool.deinit();
                    @memset(&requested, std.math.maxInt(usize));
                    try testing.expectEqual(count, pool.threads.len);
                    try checkWorkers(&pool, if (mode == 2) cpus else cpus[0..1]);
                    const parent = try numa.allowedCpus(testing.allocator);
                    defer testing.allocator.free(parent);
                    try testing.expectEqualSlices(usize, cpus[0..1], parent);
                }
            }
        }
    };
    var result: anyerror!void = {};
    const parent = try std.Thread.spawn(.{}, Runner.run, .{ before[0..@min(2, before.len)], &result });
    parent.join();
    result catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    const after = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(usize, before, after);
}

test "pool placement rolls back all workers after pin and partial setup failures" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const before = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(before);
    try testing.expect(before.len != 0);
    const linux = std.os.linux;
    var words: usize = 16;
    const invalid_cpu = while (true) {
        const mask = try testing.allocator.alloc(usize, words);
        defer testing.allocator.free(mask);
        const rc = linux.syscall3(.sched_getaffinity, 0, mask.len * @sizeOf(usize), @intFromPtr(mask.ptr));
        switch (linux.errno(rc)) {
            // A successful buffer spans every kernel-supported CPU bit.
            .SUCCESS => break mask.len * @bitSizeOf(usize),
            .INVAL => words = try std.math.mul(usize, words, 2),
            else => return error.Unexpected,
        }
    };

    var loop = try event.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: Pool = undefined;
    for ([_]usize{ 1, 4 }) |count| {
        const cases = [_]struct { cpus: []const usize, err: anyerror }{
            .{ .cpus = &.{std.math.maxInt(usize)}, .err = error.Overflow },
            .{ .cpus = &.{invalid_cpu}, .err = error.InvalidArgument },
            .{ .cpus = &.{ before[0], invalid_cpu }, .err = error.AffinityMismatch },
        };
        for (cases) |case| {
            pool.initWithOptions(testing.allocator, &loop, .{ .count = count, .cpus = case.cpus }) catch |err| {
                switch (err) {
                    error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
                    else => try testing.expectEqual(case.err, err),
                }
                try testing.expect(pool.stopping);
                try testing.expectEqual(null, pool.queue.first);
                try testing.expectEqual(0, pool.outstanding);
                try testing.expect(!loop.control.hasWork());
                try pool.init(testing.allocator, &loop, count);
                pool.deinit();
                continue;
            };
            pool.deinit();
            return error.TestExpectedError;
        }
    }
    const after = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(usize, before, after);
}
