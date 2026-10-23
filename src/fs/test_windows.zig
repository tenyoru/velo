const std = @import("std");
const io = @import("../event/root.zig");
const co = @import("../coroutines/root.zig");
const workers = @import("../workers/root.zig");
const fs = @import("root.zig");
const w = @import("../event/backends/win32.zig");
const support = @import("../event/tests/support.zig");
const testing = std.testing;

fn finish(loop: *io.Loop, frame: anytype) void {
    loop.executor.spawn(&frame.task);
    support.run(loop);
    std.debug.assert(frame.task.outstanding_io == 0);
    frame.coroutine.deinit();
}

fn create(pool: *workers.Pool, dir: std.Io.Dir, path: []const u8) !fs.File {
    var frame: co.Frame(fs.Create) = .{
        .coroutine = .init(pool, .{ .dir = dir, .path = path, .flags = .{ .read = true } }),
    };
    finish(pool.loop, &frame);
    return frame.getResult();
}

fn drained(loop: *io.Loop, pool: *workers.Pool) !void {
    try support.drained(loop);
    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try testing.expectEqual(null, loop.control.registrations.first);
    while (true) {
        var bytes: u32 = undefined;
        var key: usize = undefined;
        var overlapped: ?*w.OVERLAPPED = null;
        const success = w.GetQueuedCompletionStatus(loop.port, &bytes, &key, &overlapped, 0) != 0;
        const code = if (success) 0 else w.GetLastError();
        try testing.expectEqual(null, overlapped);
        if (!success) {
            try testing.expectEqual(w.WAIT_TIMEOUT, code);
            break;
        }
        try testing.expectEqual(1, key);
    }
}

test "windows filesystem ordinary handles select IOCP and preserve positional results" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    const file = try create(&pool, tmp.dir, "native");
    defer file.close(testing.io);
    try testing.expect(file.flags.nonblocking);
    try testing.expect(loop.file_ops.read and loop.file_ops.write and !loop.file_ops.sync);

    var writes: [8]co.Frame(fs.WriteAt) = undefined;
    for (&writes, 0..) |*write, i| {
        write.* = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = "x", .offset = 7 + i }) };
        std.debug.assert(write.coroutine == .native);
        loop.executor.spawn(&write.task);
    }
    loop.executor.run();
    std.debug.assert(pool.outstanding == 0 and loop.active == writes.len);
    support.run(&loop);
    for (&writes) |*write| write.coroutine.deinit();
    for (&writes) |*write| try testing.expectEqual(1, try write.getResult());

    var buffer: [4]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 9 }),
    };
    try testing.expect(read.coroutine == .native);
    finish(&loop, &read);
    try testing.expectEqual(4, try read.getResult());
    try testing.expectEqualStrings("xxxx", &buffer);
    read = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 13 }) };
    finish(&loop, &read);
    try testing.expectEqual(2, try read.getResult());
    try testing.expectEqualStrings("xx", buffer[0..2]);

    for ([_]u64{ 15, 16, (@as(u64, 1) << 32) + 9 }) |offset| {
        read = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = offset }) };
        finish(&loop, &read);
        try testing.expectEqual(0, try read.getResult());
    }
    var empty_write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "", .offset = std.math.maxInt(i64) }),
    };
    finish(&loop, &empty_write);
    try testing.expectEqual(0, try empty_write.getResult());
    read = .{ .coroutine = .init(&pool, .{
        .file = file,
        .buffer = buffer[0..0],
        .offset = std.math.maxInt(i64),
    }) };
    finish(&loop, &read);
    try testing.expectEqual(0, try read.getResult());

    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "native" }),
    };
    finish(&loop, &open);
    const readonly = try open.getResult();
    defer readonly.close(testing.io);
    try testing.expect(readonly.flags.nonblocking);
    read = .{ .coroutine = .init(&pool, .{ .file = readonly, .buffer = &buffer, .offset = 7 }) };
    try testing.expect(read.coroutine == .native);
    finish(&loop, &read);
    try testing.expectEqual(4, try read.getResult());
    var denied: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = readonly, .buffer = "!" }),
    };
    try testing.expect(denied.coroutine == .native);
    finish(&loop, &denied);
    try testing.expectError(error.NotOpenForWriting, denied.getResult());
    try drained(&loop, &pool);
}

test "windows filesystem workers interoperate before and after IOCP association" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 2);
    defer pool.deinit();
    const file = try create(&pool, tmp.dir, "interop");
    defer file.close(testing.io);
    try testing.expect(file.flags.nonblocking);

    for ([_]bool{ false, true }) |associated| {
        var write: co.Frame(fs.worker.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "worker", .offset = 4 }),
        };
        finish(&loop, &write);
        try testing.expectEqual(6, try write.getResult());
        var buffer: [6]u8 = undefined;
        var worker_read: co.Frame(fs.worker.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 4 }),
        };
        finish(&loop, &worker_read);
        try testing.expectEqual(6, try worker_read.getResult());
        try testing.expectEqualStrings("worker", &buffer);
        worker_read = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 64 }) };
        finish(&loop, &worker_read);
        try testing.expectEqual(0, try worker_read.getResult());
        var empty: co.Frame(fs.worker.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "" }),
        };
        finish(&loop, &empty);
        try testing.expectEqual(0, try empty.getResult());

        var native_read: co.Frame(fs.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 4 }),
        };
        write = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = "other", .offset = 32 }) };
        loop.executor.spawn(&native_read.task);
        loop.executor.spawn(&write.task);
        support.run(&loop);
        native_read.coroutine.deinit();
        write.coroutine.deinit();
        try testing.expectEqual(6, try native_read.getResult());
        try testing.expectEqualStrings("worker", &buffer);
        try testing.expectEqual(5, try write.getResult());

        var sync: co.Frame(fs.Sync) = .{ .coroutine = .init(&pool, .{ .file = file }) };
        try testing.expect(sync.coroutine == .worker);
        finish(&loop, &sync);
        try sync.getResult();
        try drained(&loop, &pool);
        if (!associated) {
            var native_write: co.Frame(fs.WriteAt) = .{
                .coroutine = .init(&pool, .{ .file = file, .buffer = "native", .offset = 4 }),
            };
            finish(&loop, &native_write);
            try testing.expectEqual(6, try native_write.getResult());
        }
    }

    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "interop" }),
    };
    finish(&loop, &open);
    const readonly = try open.getResult();
    defer readonly.close(testing.io);
    var denied: co.Frame(fs.worker.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = readonly, .buffer = "!" }),
    };
    finish(&loop, &denied);
    try testing.expectError(error.NotOpenForWriting, denied.getResult());
    for ([_]u64{ @as(u64, std.math.maxInt(i64)) + 1, std.math.maxInt(u64) }) |offset| {
        var buffer: [1]u8 = undefined;
        var read: co.Frame(fs.worker.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = offset }),
        };
        var write: co.Frame(fs.worker.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "!", .offset = offset }),
        };
        finish(&loop, &read);
        finish(&loop, &write);
        try testing.expectError(error.InvalidArgument, read.getResult());
        try testing.expectError(error.InvalidArgument, write.getResult());
    }
    try drained(&loop, &pool);
}

test "windows filesystem caller synchronous and special open handles stay on workers" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    const file = try tmp.dir.createFile(testing.io, "sync", .{ .read = true });
    defer file.close(testing.io);
    try testing.expect(!file.flags.nonblocking);
    var write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "fallback", .offset = 3 }),
    };
    try testing.expect(write.coroutine == .worker);
    finish(&loop, &write);
    try testing.expectEqual(8, try write.getResult());
    var buffer: [8]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 3 }),
    };
    try testing.expect(read.coroutine == .worker);
    finish(&loop, &read);
    try testing.expectEqual(8, try read.getResult());
    try testing.expectEqualStrings("fallback", &buffer);
    var direct: co.Frame(io.Operation.Coroutine(.read_at)) = .{
        .coroutine = .init(&loop, .{ .file = file, .buffer = &buffer }),
    };
    finish(&loop, &direct);
    try testing.expectError(error.UnsupportedOperation, direct.getResult());

    var locked: co.Frame(fs.Create) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "locked",
            .flags = .{ .read = true, .lock = .exclusive },
        }),
    };
    finish(&loop, &locked);
    const locked_file = try locked.getResult();
    defer locked_file.close(testing.io);
    try testing.expect(!locked_file.flags.nonblocking);
    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "sync", .flags = .{ .path_only = true } }),
    };
    finish(&loop, &open);
    const path_file = try open.getResult();
    defer path_file.close(testing.io);
    try testing.expect(!path_file.flags.nonblocking);
    open = .{ .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "." }) };
    finish(&loop, &open);
    const directory = try open.getResult();
    defer directory.close(testing.io);
    try testing.expect(!directory.flags.nonblocking);
    try testing.expectEqual(.directory, (try directory.stat(testing.io)).kind);
    try drained(&loop, &pool);
}

test "windows filesystem queued worker cancellation skips writes and claimed writes keep their result" {
    const GatedWrite = workers.Pool.Work(struct {
        action: @FieldType(fs.worker.WriteAt, "action"),
        started: *std.Io.Event,
        release: *std.Io.Event,

        pub fn run(self: *@This(), threaded: std.Io) anyerror!usize {
            self.started.set(threaded);
            self.release.waitUncancelable(threaded);
            return self.action.run(threaded);
        }
    });
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    const file = try create(&pool, tmp.dir, "worker cancel");
    defer file.close(testing.io);
    var seed: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "native.." }),
    };
    finish(&loop, &seed);
    try testing.expectEqual(8, try seed.getResult());
    var started: std.Io.Event = .unset;
    var release: std.Io.Event = .unset;
    var running: co.Frame(GatedWrite) = .{
        .coroutine = .init(&pool, .{
            .action = .{ .file = file, .buffer = "running", .offset = 8 },
            .started = &started,
            .release = &release,
        }),
    };
    var queued: co.Frame(fs.worker.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "wrong" }),
    };
    loop.executor.spawn(&running.task);
    loop.executor.run();
    started.waitUncancelable(pool.threaded.io());
    loop.executor.spawn(&queued.task);
    loop.executor.run();
    running.coroutine.cancel();
    queued.coroutine.cancel();
    release.set(pool.threaded.io());
    support.run(&loop);
    running.coroutine.deinit();
    queued.coroutine.deinit();
    try testing.expectEqual(7, try running.getResult());
    try testing.expectError(error.OperationCanceled, queued.getResult());
    var buffer: [15]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer }),
    };
    finish(&loop, &read);
    try testing.expectEqual(15, try read.getResult());
    try testing.expectEqualStrings("native..running", &buffer);
    try drained(&loop, &pool);
}

fn submitAndStop(loop: *io.Loop) void {
    const Stop = struct {
        loop: *io.Loop,

        pub fn poll(self: *@This(), _: *co.Task) co.Poll(void) {
            self.loop.stop();
            return .{ .ready = {} };
        }
    };
    // The stop wake is dequeued before submissions, so run returns with their packets unconsumed.
    var stop: co.Frame(Stop) = .{ .coroutine = .{ .loop = loop } };
    loop.executor.spawn(&stop.task);
    support.run(loop);
    std.debug.assert(stop.result != null);
}

test "windows filesystem queued and inflight cancellation drain native packets including late success" {
    inline for (.{ fs.ReadAt, fs.WriteAt }) |C| {
        for ([_]bool{ false, true }) |shutdown| {
            var tmp = testing.tmpDir(.{});
            defer tmp.cleanup();
            var loop = try io.Loop.init(.{ .entries = 8 });
            defer loop.deinit();
            var pool: workers.Pool = undefined;
            try pool.init(testing.allocator, &loop, 1);
            defer pool.deinit();
            const file = try create(&pool, tmp.dir, "cancel");
            defer file.close(testing.io);
            var seed: co.Frame(fs.worker.WriteAt) = .{
                .coroutine = .init(&pool, .{ .file = file, .buffer = "xxxxxxxx" }),
            };
            finish(&loop, &seed);
            try testing.expectEqual(8, try seed.getResult());
            var buffer: [8][1]u8 = undefined;
            for ([_]bool{ false, true }) |queued| {
                var frame: co.Frame(C) = .{
                    .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer[0] }),
                };
                try testing.expect(frame.coroutine == .native);
                if (queued) {
                    loop.executor.spawn(&frame.task);
                    loop.executor.run();
                    std.debug.assert(frame.coroutine.native.inner.operation.state == .queued);
                }
                frame.coroutine.cancel();
                frame.coroutine.cancel();
                if (!queued) loop.executor.spawn(&frame.task);
                loop.executor.run();
                frame.coroutine.deinit();
                try testing.expectError(error.OperationCanceled, frame.getResult());
                try drained(&loop, &pool);
            }

            var frames: [8]co.Frame(C) = undefined;
            for (&frames, &buffer, 0..) |*frame, *byte, i| {
                byte.* = .{'x'};
                frame.* = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = byte, .offset = i }) };
                loop.executor.spawn(&frame.task);
            }
            submitAndStop(&loop);
            for (&frames) |*frame| {
                std.debug.assert(frame.result == null);
                std.debug.assert(frame.coroutine.native.inner.operation.state == .inflight);
                std.debug.assert(frame.task.outstanding_io == 1);
                if (!shutdown) {
                    frame.coroutine.cancel();
                    frame.coroutine.cancel();
                }
                std.debug.assert(frame.coroutine.native.inner.operation.registration.registered);
            }
            if (shutdown) loop.shutdown();
            support.run(&loop);
            for (&frames) |*frame| frame.coroutine.deinit();
            for (&frames) |*frame| {
                const n = frame.getResult() catch |err| {
                    try testing.expectEqual(error.OperationCanceled, err);
                    continue;
                };
                try testing.expectEqual(1, n);
                try testing.expectEqual(0, frame.task.outstanding_io);
            }
            try drained(&loop, &pool);
        }
    }
}

test "windows direct filesystem native and isolated worker transfers enforce sector constraints" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var frame: co.Frame(fs.Create) = .{ .coroutine = .init(&pool, .{
        .dir = tmp.dir,
        .path = "direct",
        .flags = .{ .read = true },
        .direct = true,
    }) };
    finish(&loop, &frame);
    const file = try frame.getResult();
    defer file.close(testing.io);
    try testing.expect(file.flags.nonblocking);
    var storage_info: extern struct {
        logical: u32,
        physical_atomic: u32,
        physical_performance: u32,
        effective_atomic: u32,
        flags: u32,
        sector_offset: u32,
        partition_offset: u32,
    } = undefined;
    try testing.expect(w.GetFileInformationByHandleEx(file.handle, w.FileStorageInfo, &storage_info, @sizeOf(@TypeOf(storage_info))) != 0);
    const alignment: usize = @max(storage_info.logical, storage_info.physical_atomic, storage_info.effective_atomic);
    try testing.expect(storage_info.logical > 1 and std.math.isPowerOfTwo(alignment));
    const storage = try testing.allocator.alloc(u8, alignment * 3);
    defer testing.allocator.free(storage);
    const start = std.mem.alignForward(usize, @intFromPtr(storage.ptr), alignment) - @intFromPtr(storage.ptr);
    const buffer = storage[start..][0..alignment];
    @memset(buffer, 0x5a);
    inline for (.{ fs.WriteAt, fs.worker.WriteAt }) |C| {
        var write: co.Frame(C) = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = buffer }) };
        finish(&loop, &write);
        try testing.expectEqual(buffer.len, try write.getResult());
    }
    inline for (.{ fs.ReadAt, fs.worker.ReadAt }) |C| {
        @memset(buffer, 0);
        var read: co.Frame(C) = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = buffer }) };
        finish(&loop, &read);
        try testing.expectEqual(buffer.len, try read.getResult());
        for (buffer) |byte| try testing.expectEqual(0x5a, byte);
    }
    inline for (.{ fs.ReadAt, fs.WriteAt, fs.worker.ReadAt, fs.worker.WriteAt }) |C| {
        for ([_]bool{ false, true }) |offset| {
            var invalid: co.Frame(C) = .{ .coroutine = .init(&pool, .{
                .file = file,
                .buffer = if (offset) buffer else buffer[0 .. buffer.len - 1],
                .offset = if (offset) 1 else 0,
            }) };
            finish(&loop, &invalid);
            try testing.expectError(error.InvalidArgument, invalid.getResult());
        }
    }
    var open: co.Frame(fs.Open) = .{ .coroutine = .init(&pool, .{
        .dir = tmp.dir,
        .path = "direct",
        .direct = true,
    }) };
    finish(&loop, &open);
    const reopened = try open.getResult();
    defer reopened.close(testing.io);
    try testing.expect(reopened.flags.nonblocking);
    open = .{ .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = ".", .direct = true }) };
    finish(&loop, &open);
    try testing.expectError(error.UnsupportedOperation, open.getResult());
    frame = .{ .coroutine = .init(&pool, .{
        .dir = tmp.dir,
        .path = "locked direct",
        .flags = .{ .lock = .exclusive },
        .direct = true,
    }) };
    finish(&loop, &frame);
    try testing.expectError(error.UnsupportedOperation, frame.getResult());
    try testing.expectError(error.FileNotFound, tmp.dir.openFile(testing.io, "locked direct", .{}));
    try drained(&loop, &pool);
}

test "windows Allocate never lowers preexisting allocation or EOF on overlapped handles" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    const file = try create(&pool, tmp.dir, "allocation");
    defer file.close(testing.io);
    const eof = 64 * 1024;
    var write: co.Frame(fs.WriteAt) = .{ .coroutine = .init(&pool, .{
        .file = file,
        .buffer = "tail",
        .offset = eof - 4,
    }) };
    finish(&loop, &write);
    try testing.expectEqual(4, try write.getResult());
    const reservation: i64 = 1024 * 1024;
    try testing.expect(w.SetFileInformationByHandle(file.handle, w.FileAllocationInfo, &reservation, @sizeOf(i64)) != 0);
    var before: w.FILE_STANDARD_INFO = undefined;
    try testing.expect(w.GetFileInformationByHandleEx(file.handle, w.FileStandardInfo, &before, @sizeOf(w.FILE_STANDARD_INFO)) != 0);
    for ([_]u64{ 1, eof + 4096, 2 * reservation }) |end| {
        var allocate: co.Frame(fs.Allocate) = .{ .coroutine = .init(&pool, .{ .file = file, .len = end }) };
        finish(&loop, &allocate);
        try allocate.getResult();
        var after: w.FILE_STANDARD_INFO = undefined;
        try testing.expect(w.GetFileInformationByHandleEx(file.handle, w.FileStandardInfo, &after, @sizeOf(w.FILE_STANDARD_INFO)) != 0);
        try testing.expect(after.AllocationSize >= before.AllocationSize);
        try testing.expect(after.AllocationSize >= @as(i64, @intCast(end)));
        try testing.expectEqual(@max(before.EndOfFile, @as(i64, @intCast(end))), after.EndOfFile);
        var tail: [4]u8 = undefined;
        var read: co.Frame(fs.ReadAt) = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = &tail, .offset = eof - 4 }) };
        finish(&loop, &read);
        try testing.expectEqual(4, try read.getResult());
        try testing.expectEqualStrings("tail", &tail);
        before = after;
    }
    var open: co.Frame(fs.Open) = .{ .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "allocation" }) };
    finish(&loop, &open);
    const readonly = try open.getResult();
    defer readonly.close(testing.io);
    var denied: co.Frame(fs.Allocate) = .{ .coroutine = .init(&pool, .{ .file = readonly, .len = 4 * reservation }) };
    finish(&loop, &denied);
    try testing.expectError(error.AccessDenied, denied.getResult());
    try drained(&loop, &pool);
}

test "windows Allocate rejects sparse files without changing allocation or EOF" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(testing.io, "sparse", .{ .read = true });
    defer file.close(testing.io);
    const FSCTL_SET_SPARSE = 0x900c4;
    var returned: u32 = undefined;
    if (w.DeviceIoControl(file.handle, FSCTL_SET_SPARSE, null, 0, null, 0, &returned, null) == 0) {
        const code = w.GetLastError();
        if (code == 1 or code == 50) return error.SkipZigTest;
        return @import("windows.zig").mapError(.sync, code);
    }
    try file.setLength(testing.io, 1024 * 1024);
    var before: w.FILE_STANDARD_INFO = undefined;
    try testing.expect(w.GetFileInformationByHandleEx(file.handle, w.FileStandardInfo, &before, @sizeOf(w.FILE_STANDARD_INFO)) != 0);
    var pool: workers.Pool = undefined;
    var allocate = fs.Allocate.init(&pool, .{ .file = file, .len = 2 * 1024 * 1024 });
    try testing.expectError(error.UnsupportedOperation, allocate.action.run(testing.io));
    var after: w.FILE_STANDARD_INFO = undefined;
    try testing.expect(w.GetFileInformationByHandleEx(file.handle, w.FileStandardInfo, &after, @sizeOf(w.FILE_STANDARD_INFO)) != 0);
    try testing.expectEqual(before.AllocationSize, after.AllocationSize);
    try testing.expectEqual(before.EndOfFile, after.EndOfFile);
}
