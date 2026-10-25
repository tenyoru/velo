const std = @import("std");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const workers = @import("../../workers/root.zig");
const fs = @import("../root.zig");
const support = @import("../../event/tests/support.zig");
const finish = @import("support.zig").finish;
const DarwinAio = @import("../../event/backends/DarwinAio.zig");
const testing = std.testing;

fn nativeLoop(entries: u16) !io.Loop {
    var loop = try io.Loop.init(.{ .entries = entries });

    if (!loop.file_ops.read or !loop.file_ops.write) {
        loop.deinit();
        return error.SkipZigTest;
    }

    return loop;
}

test "macOS AIO probe drains its completion and leaves sync on workers" {
    var loop = try io.Loop.init(.{ .entries = 2 });
    defer loop.deinit();

    try testing.expectEqual(loop.file_ops.read, loop.file_ops.write);
    try testing.expect(!loop.file_ops.sync);
    try testing.expect(!DarwinAio.probe(0));
    try testing.expectEqual(0, loop.file_inflight);

    var event: [1]std.c.kevent64_s = undefined;
    const timeout: std.c.timespec = .{ .sec = 0, .nsec = 0 };
    const count = std.c.kevent64(loop.kqueue, &.{}, 0, &event, 1, .{}, &timeout);

    try testing.expectEqual(std.c.E.SUCCESS, std.c.errno(count));
    try testing.expectEqual(0, count);

    var timer: co.Frame(io.Timeout) = .{
        .coroutine = .init(&loop, .{ .duration = .{ .sec = 0, .nsec = 1 } }),
    };

    finish(&loop, &timer);

    try timer.getResult();
    try support.drained(&loop);
}

test "macOS filesystem forced worker fallback roundtrip" {
    var loop = try io.Loop.init(.{ .entries = 2 });
    defer loop.deinit();

    loop.file_ops = .{};

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "fallback", .{ .read = true });
    defer file.close(testing.io);

    var write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "worker", .offset = 3 }),
    };
    try testing.expect(write.coroutine.tag == .worker);

    finish(&loop, &write);

    try testing.expectEqual(6, try write.getResult());

    var sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };
    try testing.expect(sync.coroutine.tag == .worker);

    finish(&loop, &sync);

    try sync.getResult();

    var buffer: [6]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 3 }),
    };
    try testing.expect(read.coroutine.tag == .worker);

    finish(&loop, &read);

    try testing.expectEqual(buffer.len, try read.getResult());
    try testing.expectEqualStrings("worker", &buffer);
    try testing.expectEqual(0, pool.outstanding);
    try support.drained(&loop);
}

test "macOS native positional roundtrip offsets EOF empty and dispatch" {
    var loop = try nativeLoop(2);
    defer loop.deinit();

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "native", .{ .read = true });
    defer file.close(testing.io);

    const bytes = "prefix:hello world";
    var write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = bytes }),
    };
    try testing.expect(write.coroutine.tag == .native);

    finish(&loop, &write);

    try testing.expectEqual(bytes.len, try write.getResult());

    write = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "Z", .offset = 7 }),
    };

    finish(&loop, &write);

    try testing.expectEqual(1, try write.getResult());

    var buffer: [32]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = 7 }),
    };
    try testing.expect(read.coroutine.tag == .native);

    finish(&loop, &read);

    try testing.expectEqual(11, try read.getResult());
    try testing.expectEqualStrings("Zello world", buffer[0..11]);

    read = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer, .offset = bytes.len }),
    };

    finish(&loop, &read);

    try testing.expectEqual(0, try read.getResult());

    read = .{
        .coroutine = .init(&pool, .{
            .file = file,
            .buffer = buffer[0..0],
            .offset = std.math.maxInt(i64),
        }),
    };
    write = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "" }),
    };

    inline for (.{ &read, &write }) |frame| {
        finish(&loop, frame);

        try testing.expectEqual(0, try frame.getResult());
        try testing.expectEqual(null, frame.coroutine.storage.native.inner.operation.primary);
    }
    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);

    var sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };
    try testing.expect(sync.coroutine.tag == .worker);

    finish(&loop, &sync);

    try sync.getResult();

    loop.file_ops.read = false;
    read = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer }),
    };
    write = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "" }),
    };

    try testing.expect(read.coroutine.tag == .worker);
    try testing.expect(write.coroutine.tag == .native);

    read.coroutine.deinit();
    write.coroutine.deinit();
    loop.file_ops.read = true;

    loop.file_ops.write = false;
    read = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer }),
    };
    write = .{
        .coroutine = .init(&pool, .{ .file = file, .buffer = "" }),
    };

    try testing.expect(read.coroutine.tag == .native);
    try testing.expect(write.coroutine.tag == .worker);

    read.coroutine.deinit();
    write.coroutine.deinit();

    try support.drained(&loop);
}

test "macOS native rejection preserves access descriptor stream and offset errors" {
    var loop = try nativeLoop(2);
    defer loop.deinit();

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const write_only = try tmp.dir.createFile(testing.io, "access", .{});
    defer write_only.close(testing.io);
    const read_only = try tmp.dir.openFile(testing.io, "access", .{});
    defer read_only.close(testing.io);
    const fd = try io.tcpSocket(.ipv4);
    defer support.close(fd);
    const invalid: fs.File = .{ .handle = -1, .flags = .{ .nonblocking = false } };
    const socket: fs.File = .{ .handle = fd, .flags = .{ .nonblocking = true } };

    var buffer: [1]u8 = undefined;

    for ([_]fs.File{ write_only, invalid, socket }, [_]anyerror{
        error.NotOpenForReading, error.NotOpenForReading, error.Unseekable,
    }) |file, expected| {
        var read: co.Frame(fs.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer }),
        };
        try testing.expect(read.coroutine.tag == .native);

        finish(&loop, &read);

        try testing.expectError(expected, read.getResult());
        try testing.expect(read.coroutine.storage.native.inner.operation.primary.? < 0);
    }

    for ([_]fs.File{ read_only, invalid, socket }, [_]anyerror{
        error.NotOpenForWriting, error.NotOpenForWriting, error.Unseekable,
    }) |file, expected| {
        var write: co.Frame(fs.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = file, .buffer = "x" }),
        };
        try testing.expect(write.coroutine.tag == .native);

        finish(&loop, &write);

        try testing.expectError(expected, write.getResult());
        try testing.expect(write.coroutine.storage.native.inner.operation.primary.? < 0);
    }

    for ([_]u64{ @as(u64, std.math.maxInt(i64)) + 1, std.math.maxInt(u64) }) |offset| {
        var read: co.Frame(fs.ReadAt) = .{
            .coroutine = .init(&pool, .{ .file = read_only, .buffer = &buffer, .offset = offset }),
        };
        var write: co.Frame(fs.WriteAt) = .{
            .coroutine = .init(&pool, .{ .file = write_only, .buffer = "x", .offset = offset }),
        };
        inline for (.{ &read, &write }) |frame| {
            finish(&loop, frame);

            try testing.expectError(error.InvalidArgument, frame.getResult());
            try testing.expectEqual(null, frame.coroutine.storage.native.inner.operation.primary);
        }
    }

    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try testing.expectEqual(0, loop.file_inflight);
    try support.drained(&loop);
}

test "macOS native bounded same-file requests coexist with socket and timer events" {
    var loop = try nativeLoop(2);
    defer loop.deinit();

    var pool: workers.Pool = undefined;
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

    loop.executor.spawn(&recv.task);

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
    loop.executor.spawn(&send.task);
    loop.executor.run();
    support.run(&loop);

    for (&writes, &timers) |*write, *timer| {
        try testing.expectEqual(1, try write.getResult());
        try timer.getResult();

        write.coroutine.deinit();
        timer.coroutine.deinit();
    }
    try testing.expectEqual(1, try send.getResult());
    try testing.expectEqual(1, try recv.getResult());
    try testing.expectEqualStrings("s", &byte);

    send.coroutine.deinit();
    recv.coroutine.deinit();

    var buffer: [writes.len]u8 = undefined;
    try testing.expectEqual(buffer.len, try file.readPositional(testing.io, &.{&buffer}, 0));
    const expected: [writes.len]u8 = @splat('x');
    try testing.expectEqualStrings(&expected, &buffer);
    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(null, pool.queue.first);
    try testing.expectEqual(0, loop.file_inflight);
    try testing.expectEqual(null, loop.control.registrations.first);
    try support.drained(&loop);
}

test "macOS native cancellation before submission acquires no AIO ownership" {
    var loop = try nativeLoop(2);
    defer loop.deinit();

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "cancel", .{ .read = true });
    defer file.close(testing.io);

    var buffer: [1]u8 = undefined;

    inline for (.{ fs.ReadAt, fs.WriteAt }) |C| {
        for ([_]bool{ false, true }) |queued| {
            var frame: co.Frame(C) = .{
                .coroutine = .init(&pool, .{ .file = file, .buffer = &buffer }),
            };
            try testing.expect(frame.coroutine.tag == .native);
            if (queued) {
                loop.executor.spawn(&frame.task);
                loop.executor.run();
            }

            frame.coroutine.cancel();
            frame.coroutine.cancel();
            if (!queued) loop.executor.spawn(&frame.task);
            support.run(&loop);

            try testing.expectError(error.OperationCanceled, frame.getResult());
            try testing.expectEqual(null, frame.coroutine.storage.native.inner.operation.primary);
            try testing.expectEqual(0, frame.task.outstanding_io);

            frame.coroutine.deinit();
        }
    }

    try testing.expectEqual(0, pool.outstanding);
    try testing.expectEqual(0, loop.file_inflight);
    try testing.expectEqual(null, loop.control.registrations.first);
    try support.drained(&loop);
}

test "macOS direct cache hint supports worker and available native transfers" {
    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var create: co.Frame(fs.Create) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "nocache",
            .flags = .{ .read = true },
            .direct = true,
        }),
    };

    finish(&loop, &create);

    const file = try create.getResult();
    defer file.close(testing.io);

    const FNOCACHE = 0x00040000;
    const flags = std.c.fcntl(file.handle, std.c.F.GETFL, @as(c_int, 0));
    try testing.expect(flags >= 0 and flags & FNOCACHE != 0);

    inline for (.{ fs.WriteAt, fs.worker.WriteAt }) |C| {
        var write: co.Frame(C) = .{
            .coroutine = .init(&pool, .{
                .file = file,
                .buffer = "cache hint",
                .offset = 3,
            }),
        };

        finish(&loop, &write);

        try testing.expectEqual(10, try write.getResult());
    }

    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "nocache",
            .direct = true,
        }),
    };

    finish(&loop, &open);

    const reopened = try open.getResult();
    defer reopened.close(testing.io);
    const reopened_flags = std.c.fcntl(reopened.handle, std.c.F.GETFL, @as(c_int, 0));
    try testing.expect(reopened_flags >= 0 and reopened_flags & FNOCACHE != 0);

    inline for (.{ fs.ReadAt, fs.worker.ReadAt }) |C| {
        var buffer: [10]u8 = undefined;
        var read: co.Frame(C) = .{
            .coroutine = .init(&pool, .{ .file = reopened, .buffer = &buffer, .offset = 3 }),
        };

        finish(&loop, &read);

        try testing.expectEqual(10, try read.getResult());
        try testing.expectEqualStrings("cache hint", &buffer);
    }

    try testing.expectEqual(0, pool.outstanding);
    try support.drained(&loop);
}

test "macOS Allocate rejects existing holes instead of reserving unrelated physical tail" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "holes", .{ .read = true });
    defer file.close(testing.io);

    const size = 1024 * 1024;
    var buffer: [size]u8 = @splat(0x5a);
    try testing.expectEqual(size, try file.writePositional(testing.io, &.{&buffer}, 0));
    try file.sync(testing.io);

    var hole: extern struct {
        flags: u32 = 0,
        reserved: u32 = 0,
        offset: i64 = size / 4,
        length: i64 = size / 2,
    } = .{};
    switch (std.c.errno(std.c.fcntl(file.handle, std.c.F.PUNCHHOLE, @intFromPtr(&hole)))) {
        .SUCCESS => {},
        .OPNOTSUPP, .INVAL, .NOSYS => return error.SkipZigTest,
        else => return error.Unexpected,
    }

    var loop = try io.Loop.init(.{ .entries = 8 });
    defer loop.deinit();

    var pool: workers.Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();

    var allocate: co.Frame(fs.Allocate) = .{
        .coroutine = .init(&pool, .{ .file = file, .len = size * 2 }),
    };

    finish(&loop, &allocate);

    try testing.expectError(error.UnsupportedOperation, allocate.getResult());
    try testing.expectEqual(size, (try file.stat(testing.io)).size);

    // A hole outside a non-growing request must not invalidate that request.
    for ([_]u64{ 3, size - 8 }) |offset| {
        allocate = .{ .coroutine = .init(&pool, .{ .file = file, .offset = offset, .len = 5 }) };
        finish(&loop, &allocate);
        try allocate.getResult();
        try testing.expectEqual(size, (try file.stat(testing.io)).size);
    }
    allocate = .{ .coroutine = .init(&pool, .{ .file = file, .offset = size / 4, .len = 5 }) };
    finish(&loop, &allocate);
    try testing.expectError(error.UnsupportedOperation, allocate.getResult());
    try testing.expectEqual(size, (try file.stat(testing.io)).size);

    var tail: [1]u8 = undefined;
    try testing.expectEqual(1, try file.readPositional(testing.io, &.{&tail}, size - 1));
    try testing.expectEqual(0x5a, tail[0]);

    const readonly = try tmp.dir.openFile(testing.io, "holes", .{});
    defer readonly.close(testing.io);

    allocate = .{
        .coroutine = .init(&pool, .{ .file = readonly, .len = size * 2 }),
    };

    finish(&loop, &allocate);

    try testing.expectError(error.UnsupportedOperation, allocate.getResult());
    try testing.expectEqual(0, pool.outstanding);
    try support.drained(&loop);
}
