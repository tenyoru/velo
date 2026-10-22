const std = @import("std");
const builtin = @import("builtin");
const io = @import("../event/root.zig");
const co = @import("../coroutines/root.zig");
const workers = @import("../workers/root.zig");
const WorkerPool = workers.Pool;
const fs = @import("root.zig");
const support = @import("../event/tests/support.zig");
const testing = std.testing;

fn finish(loop: *io.Loop, frame: anytype) void {
    loop.executor.spawn(&frame.task);
    support.run(loop);

    std.debug.assert(frame.task.outstanding_io == 0);

    frame.coroutine.deinit();
}

test "filesystem create positional roundtrip sync close and reopen" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        try roundtrip(&loop);
    }
}

fn roundtrip(loop: *io.Loop) !void {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, loop, 2);
    defer pool.deinit();

    var create: co.Frame(fs.Create) = .{
        .coroutine = fs.Create.init(&pool, .{
            .dir = tmp.dir,
            .path = "roundtrip",
            .flags = .{ .read = true },
        }),
    };

    finish(loop, &create);

    const file = try create.getResult();
    var closed = false;
    defer if (!closed) file.close(testing.io);

    const bytes = "prefix:hello world";
    var offset: usize = 0;

    while (offset < bytes.len) {
        var write: co.Frame(fs.WriteAt) = .{
            .coroutine = fs.WriteAt.init(&pool, .{
                .file = file,
                .buffer = bytes[offset..],
                .offset = offset,
            }),
        };

        finish(loop, &write);

        const n = try write.getResult();
        try testing.expect(n > 0 and n <= bytes.len - offset);
        offset += n;
    }

    var patch: co.Frame(fs.WriteAt) = .{
        .coroutine = fs.WriteAt.init(&pool, .{
            .file = file,
            .buffer = "Z",
            .offset = 7,
        }),
    };

    finish(loop, &patch);

    try testing.expectEqual(1, try patch.getResult());

    var sync: co.Frame(fs.Sync) = .{ .coroutine = fs.Sync.init(&pool, .{ .file = file }) };

    finish(loop, &sync);

    try sync.getResult();

    var buffer: [64]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = fs.ReadAt.init(&pool, .{
            .file = file,
            .buffer = &buffer,
            .offset = 7,
        }),
    };

    finish(loop, &read);

    const n = try read.getResult();
    try testing.expectEqual(11, n);
    try testing.expectEqualStrings("Zello world", buffer[0..n]);

    var eof: co.Frame(fs.ReadAt) = .{
        .coroutine = fs.ReadAt.init(&pool, .{
            .file = file,
            .buffer = &buffer,
            .offset = bytes.len,
        }),
    };

    finish(loop, &eof);

    try testing.expectEqual(0, try eof.getResult());

    var empty_write: co.Frame(fs.WriteAt) = .{
        .coroutine = fs.WriteAt.init(&pool, .{
            .file = file,
            .buffer = "",
            .offset = bytes.len,
        }),
    };

    finish(loop, &empty_write);

    try testing.expectEqual(0, try empty_write.getResult());

    var empty_read: co.Frame(fs.ReadAt) = .{
        .coroutine = fs.ReadAt.init(&pool, .{
            .file = file,
            .buffer = buffer[0..0],
            .offset = std.math.maxInt(i64),
        }),
    };
    finish(loop, &empty_read);
    try testing.expectEqual(0, try empty_read.getResult());

    var close: co.Frame(fs.Close) = .{ .coroutine = fs.Close.init(&pool, .{ .file = file }) };

    finish(loop, &close);

    try close.getResult();
    closed = true;

    var open: co.Frame(fs.Open) = .{
        .coroutine = fs.Open.init(&pool, .{
            .dir = tmp.dir,
            .path = "roundtrip",
        }),
    };

    finish(loop, &open);

    const reopened = try open.getResult();
    defer reopened.close(testing.io);

    var short_buffer: [3]u8 = undefined;
    read = .{
        .coroutine = fs.ReadAt.init(&pool, .{
            .file = reopened,
            .buffer = &short_buffer,
            .offset = 7,
        }),
    };

    finish(loop, &read);

    try testing.expectEqual(3, try read.getResult());
    try testing.expectEqualStrings("Zel", &short_buffer);
    try testing.expectEqual(0, pool.outstanding);
    try testing.expect(!loop.control.hasWork());
    try testing.expectEqual(null, loop.control.registrations.first);
}

test "filesystem missing paths propagate errors and cancellation before open acquires nothing" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var open: co.Frame(fs.Open) = .{
        .coroutine = fs.Open.init(&pool, .{
            .dir = tmp.dir,
            .path = "absent",
        }),
    };

    finish(&loop, &open);

    try testing.expectError(error.FileNotFound, open.getResult());

    var create: co.Frame(fs.Create) = .{
        .coroutine = fs.Create.init(&pool, .{
            .dir = tmp.dir,
            .path = "absent/file",
        }),
    };

    finish(&loop, &create);

    try testing.expectError(error.FileNotFound, create.getResult());

    create = .{
        .coroutine = fs.Create.init(&pool, .{
            .dir = tmp.dir,
            .path = "canceled",
        }),
    };

    create.coroutine.cancel();
    finish(&loop, &create);

    try testing.expectError(error.OperationCanceled, create.getResult());
    try testing.expectError(error.FileNotFound, tmp.dir.openFile(testing.io, "canceled", .{}));
}

test "filesystem completed open wins cancellation and preserves caller owned handle" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const original = try tmp.dir.createFile(testing.io, "existing", .{});
    original.close(testing.io);

    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var open: co.Frame(fs.Open) = .{
        .coroutine = fs.Open.init(&pool, .{
            .dir = tmp.dir,
            .path = "existing",
        }),
    };

    loop.executor.spawn(&open.task);
    loop.executor.run();

    // Do not service the post until cancellation races with the published result.
    while (loop.control.head.load(.acquire) == null) std.Thread.yield() catch {};
    open.coroutine.cancel();
    support.run(&loop);

    const file = try open.getResult();
    var close: co.Frame(fs.Close) = .{ .coroutine = fs.Close.init(&pool, .{ .file = file }) };

    finish(&loop, &close);

    try close.getResult();

    open.coroutine.deinit();

    try testing.expectEqual(0, open.task.outstanding_io);
}

test "filesystem sync propagates the standard I/O durability error" {
    const Failure = struct {
        fn sync(_: ?*anyopaque, _: fs.File) fs.File.SyncError!void {
            return error.InputOutput;
        }
    };

    var vtable = testing.io.vtable.*;
    vtable.fileSync = Failure.sync;
    var pool: WorkerPool = undefined;
    var sync = fs.worker.Sync.init(&pool, .{ .file = .stdout() });

    try testing.expectError(
        error.InputOutput,
        sync.action.run(.{ .vtable = &vtable, .userdata = null }),
    );
}

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
        var read = fs.ReadAt.init(&pool, .{ .file = .stdout(), .buffer = &buffer });
        var write = fs.WriteAt.init(&pool, .{ .file = .stdout(), .buffer = "x" });
        var sync = fs.Sync.init(&pool, .{ .file = .stdout() });
        try testing.expectEqual(std.mem.eql(u8, capability, "read"), read == .worker);
        try testing.expectEqual(std.mem.eql(u8, capability, "write"), write == .worker);
        try testing.expectEqual(std.mem.eql(u8, capability, "sync"), sync == .worker);
        read.deinit();
        write.deinit();
        sync.deinit();
        @field(loop.file_ops, capability) = true;
    }

    loop.file_ops = .{};
    try roundtrip(&loop);
}

test "filesystem offsets and platform access errors are preserved" {
    for (support.modes) |mode| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();
        const write_only = try tmp.dir.createFile(testing.io, "access", .{});
        defer write_only.close(testing.io);
        const read_only = try tmp.dir.openFile(testing.io, "access", .{});
        defer read_only.close(testing.io);

        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 1);
        defer pool.deinit();

        var buffer: [1]u8 = undefined;
        var read: co.Frame(fs.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = write_only, .buffer = &buffer }),
        };
        finish(&loop, &read);
        try testing.expectError(
            if (builtin.os.tag == .windows) error.AccessDenied else error.NotOpenForReading,
            read.getResult(),
        );
        var write: co.Frame(fs.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = read_only, .buffer = "x" }),
        };
        finish(&loop, &write);
        try testing.expectError(
            if (builtin.os.tag == .windows) error.AccessDenied else error.NotOpenForWriting,
            write.getResult(),
        );

        for ([_]u64{ @as(u64, std.math.maxInt(i64)) + 1, std.math.maxInt(u64) }) |offset| {
            read = .{ .coroutine = .init(&pool, .{
                .file = read_only,
                .buffer = &buffer,
                .offset = offset,
            }) };
            write = .{ .coroutine = .init(&pool, .{
                .file = write_only,
                .buffer = "x",
                .offset = offset,
            }) };
            finish(&loop, &read);
            finish(&loop, &write);
            try testing.expectError(error.InvalidArgument, read.getResult());
            try testing.expectError(error.InvalidArgument, write.getResult());
        }
        try support.drained(&loop);
    }
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
    var sync: co.Frame(fs.Sync) = .{ .coroutine = .init(&pool, .{ .file = full }) };
    inline for (.{ &read, &denied, &write, &sync }) |frame| {
        try testing.expect(frame.coroutine == .native);
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
        try testing.expect(frame.coroutine == .native);
        try testing.expect(frame.coroutine.native.inner.operation.primary.? < 0);
        frame.coroutine.deinit();
    }
    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try support.drained(&loop);
}

test "filesystem native file kinds reject unsupported loops before queuing" {
    var loop = if (builtin.os.tag == .linux)
        try io.Loop.initWithBackend(.{ .entries = 8 }, .epoll)
    else
        try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();
    loop.file_ops = .{ .read = false, .write = false, .sync = false };
    var buffer: [1]u8 = undefined;
    inline for (.{ io.Kind.read_at, io.Kind.write_at, io.Kind.sync }) |kind| {
        const C = io.Operation.Coroutine(kind);
        var frame: co.Frame(C) = .{
            .coroutine = .init(&loop, if (kind == .sync)
                .{ .file = fs.File.stdout() }
            else
                .{ .file = fs.File.stdout(), .buffer = &buffer }),
        };
        loop.executor.spawn(&frame.task);
        loop.executor.run();
        try testing.expectError(error.UnsupportedOperation, frame.getResult());
        try testing.expectEqual(null, loop.pending.first);
        try testing.expectEqual(0, frame.task.outstanding_io);
        frame.coroutine.deinit();
    }
}

fn simulatedLoop() io.Loop {
    return .{
        .file_ops = .{ .read = true, .write = true, .sync = true },
        .control = .{ .signal = struct {
            fn signal(_: *io.Control) void {}
        }.signal },
    };
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
            try testing.expect(frame.coroutine == .native);
            loop.executor.spawn(&frame.task);
            loop.executor.run();
            const op = &frame.coroutine.native.inner.operation;
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
            try testing.expectEqual(null, frame.coroutine.native.inner.operation.primary);
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
            const op = &frame.coroutine.native.inner.operation;
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
            @import("../event/backends/Uring.zig").cancelComplete(op);
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
        write.* = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = "x", .offset = offset }) };
        timer.* = .{ .coroutine = .init(&loop, .{ .duration = .{ .sec = 0, .nsec = 1 } }) };
        try testing.expect(write.coroutine == .native);
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
    var sync: co.Frame(fs.Sync) = .{ .coroutine = .init(&pool, .{ .file = file }) };
    try testing.expect(sync.coroutine == .native);
    finish(&loop, &sync);
    try sync.getResult();
    var bytes: [writes.len]u8 = undefined;
    try testing.expectEqual(bytes.len, try file.readPositional(testing.io, &.{&bytes}, 0));
    const expected: [writes.len]u8 = @splat('x');
    try testing.expectEqualStrings(&expected, &bytes);
    try support.drained(&loop);

    for (&writes) |*write| {
        write.* = .{ .coroutine = .init(&pool, .{ .file = file, .buffer = "never written" }) };
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
