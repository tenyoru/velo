const std = @import("std");
const linux = std.os.linux;
const event = @import("../root.zig");
const testing = std.testing;

fn capability(err: anyerror) anyerror {
    return switch (err) {
        error.PermissionDenied, error.SystemOutdated, error.IowqAffinityUnsupported => error.SkipZigTest,
        else => err,
    };
}

fn affinity(tid: linux.pid_t) ![]usize {
    var words: usize = 1;
    while (true) {
        const mask = try testing.allocator.alloc(usize, words);
        @memset(mask, 0);
        const rc = linux.syscall3(.sched_getaffinity, @intCast(tid), std.mem.sliceAsBytes(mask).len, @intFromPtr(mask.ptr));
        switch (linux.errno(rc)) {
            .SUCCESS => return mask,
            .INVAL => {
                testing.allocator.free(mask);
                words = try std.math.mul(usize, words, 2);
            },
            else => |e| {
                testing.allocator.free(mask);
                return switch (e) {
                    .PERM, .ACCES => error.PermissionDenied,
                    .SRCH => error.ProcessNotFound,
                    else => error.TestAffinityFailed,
                };
            },
        }
    }
}

fn expectCpus(mask: []const usize, cpus: []const usize) !void {
    for (mask, 0..) |word, i| {
        var expected: usize = 0;
        for (cpus) |cpu| {
            if (cpu / @bitSizeOf(usize) == i)
                expected |= @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));
        }
        try testing.expectEqual(expected, word);
    }
    for (cpus) |cpu| try testing.expect(cpu / @bitSizeOf(usize) < mask.len);
}

fn drain(ring: *linux.IoUring) !linux.io_uring_cqe {
    while (ring.sq_ready() != 0) {
        const submitted = ring.submit() catch |err| switch (err) {
            error.SignalInterrupt => continue,
            else => return err,
        };
        if (submitted == 0 and ring.sq_ready() != 0) return error.TestSubmissionStalled;
    }
    while (true) {
        return ring.copy_cqe() catch |err| switch (err) {
            error.SignalInterrupt => continue,
            else => return err,
        };
    }
}

fn waitWorkers(ring: *linux.IoUring, cpus: []const usize) !usize {
    const tasks = std.Io.Dir.openDirAbsolute(testing.io, "/proc/self/task", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound, error.AccessDenied => return error.SkipZigTest,
        else => return err,
    };
    defer tasks.close(testing.io);
    var name_buffer: [64]u8 = undefined;
    const name = try std.fmt.bufPrint(&name_buffer, "iou-wrk-{d}", .{std.Thread.getCurrentId()});
    var attempt: usize = 0;
    while (attempt < 500) : (attempt += 1) {
        var found: usize = 0;
        var iterator = tasks.iterate();
        while (try iterator.next(testing.io)) |entry| {
            const tid = std.fmt.parseInt(linux.pid_t, entry.name, 10) catch continue;
            var path_buffer: [64]u8 = undefined;
            const path = try std.fmt.bufPrint(&path_buffer, "{s}/comm", .{entry.name});
            var comm_buffer: [64]u8 = undefined;
            const comm = tasks.readFile(testing.io, path, &comm_buffer) catch |err| switch (err) {
                error.FileNotFound => continue,
                error.AccessDenied => return error.SkipZigTest,
                else => return err,
            };
            if (!std.mem.eql(u8, std.mem.trimEnd(u8, comm, "\n"), name[0..@min(15, name.len)])) continue;
            const actual = affinity(tid) catch |err| switch (err) {
                error.ProcessNotFound => continue,
                else => return err,
            };
            defer testing.allocator.free(actual);
            if (cpus.len != 0) try expectCpus(actual, cpus);
            found += 1;
        }
        if (found != 0 or ring.cq_ready() != 0) return found;
        const delay: linux.timespec = .{ .sec = 0, .nsec = 10_000_000 };
        switch (linux.errno(linux.nanosleep(&delay, null))) {
            .SUCCESS, .INTR => {},
            else => return error.TestSleepFailed,
        }
    }
    return error.TestIowqWorkerNotFound;
}

const WorkerTest = struct {
    cpu: usize,
    cpus: []const usize,
    result: anyerror!void = {},

    fn run(self: *@This()) void {
        self.result = self.check() catch |err| capability(err);
    }

    fn check(self: *@This()) !void {
        const pinned = try testing.allocator.alloc(usize, self.cpu / @bitSizeOf(usize) + 1);
        defer testing.allocator.free(pinned);
        @memset(pinned, 0);
        pinned[self.cpu / @bitSizeOf(usize)] = @as(usize, 1) << @intCast(self.cpu % @bitSizeOf(usize));
        switch (linux.errno(linux.syscall3(.sched_setaffinity, 0, std.mem.sliceAsBytes(pinned).len, @intFromPtr(pinned.ptr)))) {
            .SUCCESS => {},
            .PERM, .ACCES => return error.PermissionDenied,
            else => return error.TestPinFailed,
        }
        var loop = try event.Loop.initWithBackend(.{ .entries = 8, .iowq_cpus = self.cpus }, .uring);
        defer loop.deinit();
        const actual = try affinity(0);
        defer testing.allocator.free(actual);
        try expectCpus(actual, &.{self.cpu});

        var pipe: [2]linux.fd_t = undefined;
        if (linux.errno(linux.pipe2(&pipe, .{ .CLOEXEC = true })) != .SUCCESS) return error.TestPipeFailed;
        defer {
            _ = linux.close(pipe[0]);
            if (pipe[1] != -1) _ = linux.close(pipe[1]);
        }
        var buffer: [1]u8 = undefined;
        const sqe = try loop.ring.get_sqe();
        sqe.prep_read(pipe[0], &buffer, 0);
        sqe.flags |= linux.IOSQE_ASYNC;
        sqe.user_data = 1;
        var borrowed = true;
        // Closing the writer releases the blocking read even when an assertion fails.
        defer if (borrowed) {
            _ = linux.close(pipe[1]);
            pipe[1] = -1;
            _ = drain(&loop.ring) catch |err| std.debug.panic("io-wq cleanup could not drain: {s}", .{@errorName(err)});
        };
        while (loop.ring.sq_ready() != 0) {
            const submitted = loop.ring.submit() catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            if (submitted == 0 and loop.ring.sq_ready() != 0) return error.TestSubmissionStalled;
        }
        const found = try waitWorkers(&loop.ring, self.cpus);
        _ = linux.close(pipe[1]);
        pipe[1] = -1;
        const cqe = try drain(&loop.ring);
        borrowed = false;
        if (cqe.res < 0) switch (@as(linux.E, @fromBackingInt(@as(u16, @intCast(-cqe.res))))) {
            .NOSYS, .OPNOTSUPP, .PERM, .ACCES => return error.SkipZigTest,
            else => return error.TestAsyncReadFailed,
        };
        try testing.expectEqual(1, cqe.user_data);
        try testing.expectEqual(0, cqe.res);
        try testing.expect(found != 0);
        try testing.expectEqual(0, loop.ring.sq_ready());
        try testing.expectEqual(0, loop.ring.cq_ready());
    }
};

fn workerTest(cpu: usize, cpus: []const usize) !void {
    var context: WorkerTest = .{ .cpu = cpu, .cpus = cpus };
    const thread = try std.Thread.spawn(.{}, WorkerTest.run, .{&context});
    thread.join();
    try context.result;
}

fn allowedCpus() !struct { ids: [3]usize, len: usize } {
    const mask = affinity(0) catch |err| return capability(err);
    defer testing.allocator.free(mask);
    var ids: [3]usize = undefined;
    var len: usize = 0;
    for (mask, 0..) |word, i| {
        var remaining = word;
        while (remaining != 0 and len < ids.len) {
            ids[len] = i * @bitSizeOf(usize) + @ctz(remaining);
            len += 1;
            remaining &= remaining - 1;
        }
    }
    if (len == 0) return error.TestExpectedAllowedCpu;
    return .{ .ids = ids, .len = len };
}

test "io-wq explicit affinity places workers outside their pinned loop CPU" {
    const allowed = try allowedCpus();
    try workerTest(allowed.ids[0], &.{ allowed.ids[0], allowed.ids[0] });
    if (allowed.len > 1)
        try workerTest(allowed.ids[0], &.{ allowed.ids[1], allowed.ids[1] });
    if (allowed.len > 2)
        try workerTest(allowed.ids[0], &.{ allowed.ids[1], allowed.ids[2], allowed.ids[1] });
}

test "io-wq empty default still runs asynchronous pipe reads" {
    const allowed = try allowedCpus();
    try workerTest(allowed.ids[0], &.{});
}

test "io-wq epoll ignores the option" {
    var epoll = try event.Loop.initWithBackend(.{ .iowq_cpus = &.{std.math.maxInt(usize)} }, .epoll);
    defer epoll.deinit();
    try testing.expect(epoll.epoll != null);
    try epoll.run();
}

test "io-wq invalid masks fail automatic selection" {
    var available = event.Loop.initWithBackend(.{}, .uring) catch |err| return capability(err);
    available.deinit();
    for ([_]event.Loop.Backend{ .uring, .auto }) |backend| {
        if (event.Loop.initWithBackend(.{ .iowq_cpus = &.{std.math.maxInt(usize)} }, backend)) |value| {
            var unexpected = value;
            unexpected.deinit();
            return error.TestExpectedOverflow;
        } else |err| try testing.expectEqual(error.Overflow, err);
    }
}
