const std = @import("std");
const builtin = @import("builtin");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const WorkerPool = @import("../../workers/root.zig").Pool;
const fs = @import("../root.zig");
const support = @import("../../event/tests/support.zig");
const finish = @import("support.zig").finish;
const roundtrip = @import("support.zig").roundtrip;
const testing = std.testing;

fn nativeLoop(entries: u16) !io.Loop {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = io.Loop.initWithBackend(.{ .entries = entries }, .uring) catch |err| switch (err) {
        error.PermissionDenied, error.SystemOutdated => return error.SkipZigTest,
        else => return err,
    };

    if (!loop.file_ops.read or !loop.file_ops.write or !loop.file_ops.sync) {
        loop.deinit();
        return error.SkipZigTest;
    }

    return loop;
}

fn simulatedLoop() io.Loop {
    return .{
        .file_ops = .{ .read = true, .write = true, .sync = true },
        .control = .{ .signal = struct {
            fn signal(_: *io.Control) void {}
        }.signal },
    };
}

test "filesystem data-only sync submits a datasync fsync SQE" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const linux = std.os.linux;

    for ([_]bool{ false, true }) |data_only| {
        var loop = simulatedLoop();
        var pool: WorkerPool = undefined;
        pool.loop = &loop;
        pool.outstanding = 0;
        var frame: co.Frame(fs.Sync) = .{
            .coroutine = .init(&pool, .{ .file = .stdout(), .data_only = data_only }),
        };

        try testing.expect(frame.coroutine.tag == .native);

        loop.executor.spawn(&frame.task);
        loop.executor.run();

        const op = &frame.coroutine.storage.native.inner.operation;
        loop.pending.remove(&op.node);
        var sqe: linux.io_uring_sqe = undefined;
        op.prepare(&sqe, null);

        try testing.expectEqual(linux.IORING_OP.FSYNC, sqe.opcode);
        try testing.expectEqual(@as(u32, if (data_only) linux.IORING_FSYNC_DATASYNC else 0), sqe.rw_flags);

        op.complete(false, 0);
        loop.executor.run();

        try frame.getResult();

        frame.coroutine.deinit();

        try support.drained(&loop);
    }
}

test "filesystem forced uring roundtrip and per-opcode worker fallback" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = try nativeLoop(8);
    defer loop.deinit();

    try testing.expect(loop.epoll == null);
    try roundtrip(&loop);

    var pool: WorkerPool = undefined;
    pool.loop = &loop;

    inline for (.{ "read", "write", "sync" }) |capability| {
        @field(loop.file_ops, capability) = false;
        var buffer: [1]u8 = undefined;
        var read: fs.ReadAt = .init(&pool, .{ .file = .stdout(), .buffer = &buffer });
        var write: fs.WriteAt = .init(&pool, .{ .file = .stdout(), .buffer = "x" });
        var sync: fs.Sync = .init(&pool, .{ .file = .stdout() });

        try testing.expectEqual(std.mem.eql(u8, capability, "read"), read.tag == .worker);
        try testing.expectEqual(std.mem.eql(u8, capability, "write"), write.tag == .worker);
        try testing.expectEqual(std.mem.eql(u8, capability, "sync"), sync.tag == .worker);

        read.deinit();
        write.deinit();
        sync.deinit();
        @field(loop.file_ops, capability) = true;
    }

    loop.file_ops = .{};
    try roundtrip(&loop);
}

test "filesystem forced uring returns native access and device errors without worker replay" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = try nativeLoop(8);
    defer loop.deinit();

    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const write_only = try tmp.dir.createFile(testing.io, "access", .{});
    defer write_only.close(testing.io);
    const read_only = try tmp.dir.openFile(testing.io, "access", .{});
    defer read_only.close(testing.io);
    const full = try std.Io.Dir.cwd().openFile(testing.io, "/dev/full", .{ .mode = .write_only });
    defer full.close(testing.io);

    var buffer: [1]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = write_only, .buffer = &buffer }),
    };
    var denied: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = read_only, .buffer = "x" }),
    };
    var write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = full, .buffer = "x" }),
    };
    var sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = full }),
    };

    inline for (.{ &read, &denied, &write, &sync }) |frame| {
        try testing.expect(frame.coroutine.tag == .native);
        loop.executor.spawn(&frame.task);
    }
    loop.executor.run();

    try testing.expectEqual(0, pool.outstanding);

    support.run(&loop);

    try testing.expectError(error.NotOpenForReading, read.getResult());
    try testing.expectError(error.NotOpenForWriting, denied.getResult());
    try testing.expectError(error.NoSpaceLeft, write.getResult());
    try testing.expectError(error.InvalidArgument, sync.getResult());

    inline for (.{ &read, &denied, &write, &sync }) |frame| {
        try testing.expect(frame.coroutine.tag == .native);
        try testing.expect(frame.coroutine.storage.native.inner.operation.primary.? < 0);

        frame.coroutine.deinit();
    }

    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try support.drained(&loop);
}

test "filesystem native SQEs and raw errno retain filesystem semantics without retry" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const linux = std.os.linux;
    const cases = [_]struct { errno: linux.E, expected: anyerror }{
        .{ .errno = .IO, .expected = error.InputOutput },
        .{ .errno = .NOSPC, .expected = error.NoSpaceLeft },
        .{ .errno = .DQUOT, .expected = error.DiskQuota },
        .{ .errno = .INVAL, .expected = error.InvalidArgument },
        .{ .errno = .SPIPE, .expected = error.Unseekable },
        .{ .errno = .OPNOTSUPP, .expected = error.UnsupportedOperation },
        .{ .errno = .INTR, .expected = error.SignalInterrupt },
        .{ .errno = .BADF, .expected = error.BadFileDescriptor },
    };

    inline for (.{ "ReadAt", "WriteAt", "Sync" }, .{ linux.IORING_OP.READ, .WRITE, .FSYNC }) |name, opcode| {
        const C = @field(fs, name);

        for (cases) |case| {
            var loop = simulatedLoop();
            var pool: WorkerPool = undefined;
            pool.loop = &loop;
            pool.outstanding = 0;
            var buffer: [3]u8 = undefined;
            var frame: co.Frame(C) = .{
                .coroutine = .init(&pool, if (opcode == .FSYNC)
                    .{ .file = .stdout() }
                else
                    .{ .file = .stdout(), .buffer = &buffer, .offset = 17 }),
            };

            try testing.expect(frame.coroutine.tag == .native);

            loop.executor.spawn(&frame.task);
            loop.executor.run();

            const op = &frame.coroutine.storage.native.inner.operation;
            loop.pending.remove(&op.node);
            var sqe: linux.io_uring_sqe = undefined;
            op.prepare(&sqe, null);

            try testing.expectEqual(opcode, sqe.opcode);
            try testing.expectEqual(fs.File.stdout().handle, sqe.fd);
            try testing.expectEqual(0, sqe.flags);
            try testing.expectEqual(0, sqe.rw_flags);
            if (opcode != .FSYNC) {
                try testing.expectEqual(17, sqe.off);
                try testing.expectEqual(buffer.len, sqe.len);
            }

            op.complete(false, -@as(i32, @backingInt(case.errno)));

            try testing.expectEqual(1, frame.task.outstanding_io);
            try testing.expectEqual(1, loop.active);
            try testing.expect(op.registration.registered);

            loop.executor.run();

            const expected = if (case.errno == .BADF) switch (opcode) {
                .READ => error.NotOpenForReading,
                .WRITE => error.NotOpenForWriting,
                else => error.BadFileDescriptor,
            } else case.expected;
            try testing.expectError(expected, frame.getResult());
            try testing.expectEqual(0, pool.outstanding);
            try testing.expectEqual(null, loop.pending.first);
            try testing.expectEqual(null, loop.control.registrations.first);

            frame.coroutine.deinit();

            try support.drained(&loop);
        }
    }
}

test "filesystem native cancellation before poll and queued never submits" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    inline for (.{ "ReadAt", "WriteAt", "Sync" }) |name| {
        for ([_]bool{ false, true }) |queued| {
            var loop = simulatedLoop();
            var pool: WorkerPool = undefined;
            pool.loop = &loop;
            var buffer: [1]u8 = undefined;
            var frame: co.Frame(@field(fs, name)) = .{
                .coroutine = .init(&pool, if (comptime std.mem.eql(u8, name, "Sync"))
                    .{ .file = .stdout() }
                else
                    .{ .file = .stdout(), .buffer = &buffer }),
            };

            if (queued) {
                loop.executor.spawn(&frame.task);
                loop.executor.run();

                try testing.expectEqual(1, loop.active);
            }

            frame.coroutine.cancel();
            frame.coroutine.cancel();
            if (!queued) loop.executor.spawn(&frame.task);
            loop.executor.run();

            try testing.expectError(error.OperationCanceled, frame.getResult());
            try testing.expectEqual(null, frame.coroutine.storage.native.inner.operation.primary);
            try testing.expectEqual(null, loop.control.registrations.first);

            frame.coroutine.deinit();

            try support.drained(&loop);
        }
    }
}

test "filesystem native cancellation and shutdown retain both CQEs and successful primary wins" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const linux = std.os.linux;

    inline for (.{ "ReadAt", "WriteAt", "Sync" }) |name| {
        for ([_]bool{ false, true }) |shutdown| for ([_]bool{ false, true }) |success| for ([_]bool{ false, true }) |cancel_first| {
            var loop = simulatedLoop();
            var pool: WorkerPool = undefined;
            pool.loop = &loop;
            var buffer: [3]u8 = undefined;
            const is_sync = comptime std.mem.eql(u8, name, "Sync");
            var frame: co.Frame(@field(fs, name)) = .{
                .coroutine = .init(&pool, if (is_sync)
                    .{ .file = .stdout() }
                else
                    .{ .file = .stdout(), .buffer = &buffer }),
            };

            loop.executor.spawn(&frame.task);
            loop.executor.run();

            const op = &frame.coroutine.storage.native.inner.operation;
            loop.pending.remove(&op.node);
            var sqe: linux.io_uring_sqe = undefined;
            op.prepare(&sqe, null);

            if (shutdown) {
                loop.shutdown();
                _ = loop.control.service(&loop.executor);
            } else frame.coroutine.cancel();

            try testing.expectEqual(.pending, op.native.uring.cancel);

            const primary: i32 = if (success) (if (is_sync) 0 else 2) else -@as(i32, @backingInt(linux.E.CANCELED));
            if (!cancel_first) op.complete(false, primary);
            loop.executor.run();

            try testing.expect(frame.result == null);
            try testing.expectEqual(.inflight, op.state);
            try testing.expectEqual(1, frame.task.outstanding_io);
            try testing.expect(op.registration.registered);

            loop.cancellations.remove(&op.node);
            op.native.uring.cancel = .submitted;
            @import("../../event/backends/Uring.zig").cancelComplete(op);
            if (cancel_first) {
                try testing.expectEqual(.inflight, op.state);
                try testing.expectEqual(null, loop.executor.ready.first);

                op.complete(false, primary);
            }

            try testing.expectEqual(.complete, op.state);
            try testing.expectEqual(1, frame.task.outstanding_io);

            frame.coroutine.cancel();
            loop.executor.run();

            if (success) {
                if (is_sync) try frame.getResult() else try testing.expectEqual(2, try frame.getResult());
            } else {
                try testing.expectError(error.OperationCanceled, frame.getResult());
            }
            try testing.expectEqual(null, loop.control.registrations.first);

            frame.coroutine.deinit();

            try support.drained(&loop);
        };
    }
}

test "filesystem tiny native ring mixes file socket timer pressure and drains shutdown" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = try nativeLoop(2);
    defer loop.deinit();

    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "pressure", .{ .read = true });
    defer file.close(testing.io);
    const sockets = try support.pair(&loop, .ipv4);
    defer for (sockets) |socket| support.close(socket);

    var byte: [1]u8 = undefined;
    var recv: co.Frame(io.Recv) = .{
        .coroutine = .init(&loop, .{ .fd = sockets[1], .buffer = &byte }),
    };
    var send: co.Frame(io.Send) = .{
        .coroutine = .init(&loop, .{ .fd = sockets[0], .buffer = "s" }),
    };
    var writes: [32]co.Frame(fs.WriteAt) = undefined;
    var timers: [32]co.Frame(io.Timeout) = undefined;

    for (&writes, &timers, 0..) |*write, *timer, offset| {
        write.* = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "x", .offset = offset }),
        };
        timer.* = .{
            .coroutine = .init(&loop, .{ .duration = .{ .sec = 0, .nsec = 1 } }),
        };

        try testing.expect(write.coroutine.tag == .native);

        loop.executor.spawn(&write.task);
        loop.executor.spawn(&timer.task);
    }
    loop.executor.spawn(&recv.task);
    loop.executor.spawn(&send.task);
    loop.executor.run();

    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try testing.expectEqual(writes.len + timers.len + 2, loop.active);

    support.run(&loop);

    for (&writes, &timers) |*write, *timer| {
        try testing.expectEqual(1, try write.getResult());
        try timer.getResult();

        write.coroutine.deinit();
        timer.coroutine.deinit();
    }
    try testing.expectEqual(1, try recv.getResult());
    try testing.expectEqual(1, try send.getResult());
    try testing.expectEqualStrings("s", &byte);

    recv.coroutine.deinit();
    send.coroutine.deinit();

    var sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };
    try testing.expect(sync.coroutine.tag == .native);

    finish(&loop, &sync);

    try sync.getResult();

    var bytes: [writes.len]u8 = undefined;
    try testing.expectEqual(bytes.len, try file.readPositional(testing.io, &.{&bytes}, 0));
    const expected: [writes.len]u8 = @splat('x');
    try testing.expectEqualStrings(&expected, &bytes);
    try support.drained(&loop);

    for (&writes) |*write| {
        write.* = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "never written" }),
        };
        loop.executor.spawn(&write.task);
    }
    loop.executor.run();
    loop.shutdown();
    support.run(&loop);

    for (&writes) |*write| {
        try testing.expectError(error.OperationCanceled, write.getResult());

        write.coroutine.deinit();
    }
    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try testing.expectEqual(null, loop.control.registrations.first);
    try support.drained(&loop);
}

test "filesystem direct uring aligned transfers and alignment handling" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = try nativeLoop(8);
    defer loop.deinit();

    try directRoundtrip(&loop);
}

test "filesystem direct epoll aligned transfers and alignment handling" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var loop = try io.Loop.initWithBackend(.{ .entries = 8 }, .epoll);
    defer loop.deinit();

    try directRoundtrip(&loop);
}

fn directRoundtrip(loop: *io.Loop) !void {
    const linux = std.os.linux;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, loop, 1);
    defer pool.deinit();

    var create: co.Frame(fs.Create) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "direct",
            .flags = .{ .read = true },
            .direct = true,
        }),
    };

    finish(loop, &create);

    const file = create.getResult() catch |err| switch (err) {
        error.InvalidArgument, error.UnsupportedOperation => return error.SkipZigTest,
        else => return err,
    };
    defer file.close(testing.io);
    const flags = linux.fcntl(file.handle, linux.F.GETFL, 0);
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(flags));
    try testing.expect((@as(linux.O, @bitCast(@as(u32, @intCast(flags))))).DIRECT);
    try testing.expectEqual(.RDWR, (@as(linux.O, @bitCast(@as(u32, @intCast(flags))))).ACCMODE);

    var stat: linux.Statx = undefined;
    const rc = linux.statx(file.handle, "", linux.AT.EMPTY_PATH, .{ .DIOALIGN = true }, &stat);
    if (linux.errno(rc) == .NOSYS or linux.errno(rc) == .INVAL) return error.SkipZigTest;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
    if (!stat.mask.DIOALIGN or stat.dio_mem_align == 0 or stat.dio_offset_align <= 1)
        return error.SkipZigTest;

    const alignment: usize = @max(stat.dio_mem_align, stat.dio_offset_align);
    try testing.expect(std.math.isPowerOfTwo(alignment));
    const storage = try testing.allocator.alloc(u8, alignment * 3);
    defer testing.allocator.free(storage);
    const start = std.mem.alignForward(usize, @intFromPtr(storage.ptr), alignment) - @intFromPtr(storage.ptr);
    const buffer = storage[start..][0..alignment];
    @memset(buffer, 0x5a);

    inline for (.{ fs.WriteAt, fs.worker.WriteAt }) |C| {
        var write: co.Frame(C) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = buffer }),
        };

        finish(loop, &write);

        try testing.expectEqual(buffer.len, try write.getResult());
    }

    inline for (.{ fs.ReadAt, fs.worker.ReadAt }) |C| {
        @memset(buffer, 0);
        var read: co.Frame(C) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = buffer }),
        };

        finish(loop, &read);

        try testing.expectEqual(buffer.len, try read.getResult());
        for (buffer) |byte| try testing.expectEqual(0x5a, byte);
    }

    // O_DIRECT may reject misalignment or fall back to buffered I/O.
    inline for (.{ fs.ReadAt, fs.worker.ReadAt, fs.WriteAt, fs.worker.WriteAt }) |C| {
        const is_read = C == fs.ReadAt or C == fs.worker.ReadAt;
        if (!is_read) try file.setLength(testing.io, alignment + 1);

        for (0..3) |case| {
            const misaligned = switch (case) {
                0 => buffer[0 .. buffer.len - 1],
                1 => storage[start + 1 ..][0..alignment],
                else => buffer,
            };
            const offset: usize = if (case == 2) 1 else 0;
            @memset(misaligned, 0);
            const raw = while (true) {
                const result = if (is_read)
                    linux.pread(file.handle, misaligned.ptr, misaligned.len, @intCast(offset))
                else
                    linux.pwrite(file.handle, misaligned.ptr, misaligned.len, @intCast(offset));
                if (linux.errno(result) == .INTR) continue;
                break result;
            };
            @memset(misaligned, if (is_read) 0 else 0xa5);
            var frame: co.Frame(C) = .{
                .coroutine = .init(&pool, .{
                    .file = file,
                    .buffer = misaligned,
                    .offset = offset,
                }),
            };

            finish(loop, &frame);

            switch (linux.errno(raw)) {
                .INVAL => try testing.expectError(error.InvalidArgument, frame.getResult()),
                .IO => try testing.expectError(error.InputOutput, frame.getResult()),
                .SUCCESS => {
                    try testing.expectEqual(raw, try frame.getResult());
                    if (is_read) {
                        try testing.expectEqual(@min(misaligned.len, alignment - offset), raw);
                        for (misaligned[0..raw]) |byte| try testing.expectEqual(0x5a, byte);
                    } else {
                        try testing.expectEqual(misaligned.len, raw);
                        const written = storage[start..][0 .. alignment * 2];
                        @memset(written, 0);
                        try testing.expectEqual(alignment + 1, try file.readPositional(testing.io, &.{written}, 0));
                        for (written[offset..][0..raw]) |byte| try testing.expectEqual(0xa5, byte);
                    }
                },
                else => |err| try testing.expectEqual(linux.E.SUCCESS, err),
            }
        }
    }

    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "direct",
            .direct = true,
        }),
    };

    finish(loop, &open);

    const reopened = try open.getResult();
    defer reopened.close(testing.io);
    const reopened_flags: linux.O = @bitCast(@as(u32, @intCast(linux.fcntl(reopened.handle, linux.F.GETFL, 0))));
    try testing.expect(reopened_flags.DIRECT);

    create = .{
        .coroutine = .init(&pool, .{ .dir = tmp.dir, .path = "buffered" }),
    };

    finish(loop, &create);

    const buffered = try create.getResult();
    defer buffered.close(testing.io);
    const buffered_flags: linux.O = @bitCast(@as(u32, @intCast(linux.fcntl(buffered.handle, linux.F.GETFL, 0))));
    try testing.expect(!buffered_flags.DIRECT);
    try testing.expectEqual(0, pool.outstanding);
    try support.drained(loop);
}

test "filesystem Linux unsupported direct setup reports unsupported operation" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var pool: WorkerPool = undefined;
    var open: fs.Open = .init(&pool, .{ .dir = .cwd(), .path = "/dev/null", .direct = true });

    try testing.expectError(error.UnsupportedOperation, open.action.run(testing.io));
}

test "filesystem Linux Allocate fills holes without truncation and preserves OS errors" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "holes", .{ .read = true });
    defer file.close(testing.io);
    const size = 1024 * 1024;
    try file.setLength(testing.io, size);
    try testing.expectEqual(4, try file.writePositional(testing.io, &.{"tail"}, size - 4));

    var before: std.os.linux.Statx = undefined;
    try testing.expectEqual(std.os.linux.E.SUCCESS, std.os.linux.errno(std.os.linux.statx(
        file.handle,
        "",
        std.os.linux.AT.EMPTY_PATH,
        .{ .BLOCKS = true },
        &before,
    )));

    var pool: WorkerPool = undefined;
    var allocate: fs.Allocate = .init(&pool, .{ .file = file, .offset = size / 4, .len = size / 2 });

    try allocate.action.run(testing.io);

    var after: std.os.linux.Statx = undefined;
    try testing.expectEqual(std.os.linux.E.SUCCESS, std.os.linux.errno(std.os.linux.statx(
        file.handle,
        "",
        std.os.linux.AT.EMPTY_PATH,
        .{ .BLOCKS = true },
        &after,
    )));
    try testing.expect(after.blocks >= before.blocks + (size / 2) / 512);
    try testing.expectEqual(size, (try file.stat(testing.io)).size);

    var tail: [4]u8 = undefined;
    try testing.expectEqual(4, try file.readPositional(testing.io, &.{&tail}, size - 4));
    try testing.expectEqualStrings("tail", &tail);

    const readonly = try tmp.dir.openFile(testing.io, "holes", .{});
    defer readonly.close(testing.io);
    allocate = .init(&pool, .{ .file = readonly, .len = size * 2 });

    try testing.expectError(error.NotOpenForWriting, allocate.action.run(testing.io));

    allocate = .init(&pool, .{
        .file = .{ .handle = -1, .flags = .{ .nonblocking = false } },
        .len = 1,
    });

    try testing.expectError(error.NotOpenForWriting, allocate.action.run(testing.io));
}
