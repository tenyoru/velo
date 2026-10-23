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

test "filesystem create positional roundtrip sync close and reopen" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        try roundtrip(&loop);
    }
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
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "absent",
        }),
    };

    finish(&loop, &open);

    try testing.expectError(error.FileNotFound, open.getResult());

    var create: co.Frame(fs.Create) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "absent/file",
        }),
    };

    finish(&loop, &create);

    try testing.expectError(error.FileNotFound, create.getResult());

    create = .{
        .coroutine = .init(&pool, .{
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
        .coroutine = .init(&pool, .{
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
    var close: co.Frame(fs.Close) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };

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
    var sync: fs.worker.Sync = .init(&pool, .{ .file = .stdout() });

    try testing.expectError(
        error.InputOutput,
        sync.action.run(.{ .vtable = &vtable, .userdata = null }),
    );

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const file = try tmp.dir.createFile(testing.io, "data", .{});
    defer file.close(testing.io);

    var data_sync: fs.worker.Sync = .init(&pool, .{ .file = file, .data_only = true });
    const failing: std.Io = .{ .vtable = &vtable, .userdata = null };

    if (builtin.os.tag != .linux) {
        try testing.expectError(error.InputOutput, data_sync.action.run(failing));
        return;
    }

    // Linux bypasses std's fsync, so the injected failure must not surface.
    try data_sync.action.run(failing);

    const full = try std.Io.Dir.cwd().openFile(testing.io, "/dev/full", .{ .mode = .write_only });
    defer full.close(testing.io);

    data_sync = .init(&pool, .{ .file = full, .data_only = true });
    try testing.expectError(error.InvalidArgument, data_sync.action.run(failing));
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
            read = .{
                .coroutine = .init(&pool, .{
                    .file = read_only,
                    .buffer = &buffer,
                    .offset = offset,
                }),
            };
            write = .{
                .coroutine = .init(&pool, .{
                    .file = write_only,
                    .buffer = "x",
                    .offset = offset,
                }),
            };

            finish(&loop, &read);
            finish(&loop, &write);

            try testing.expectError(error.InvalidArgument, read.getResult());
            try testing.expectError(error.InvalidArgument, write.getResult());
        }

        try support.drained(&loop);
    }
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

test "filesystem failed direct setup closes the opened handle" {
    const Fake = struct {
        fn open(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.OpenFileOptions) fs.File.OpenError!fs.File {
            return .{
                .handle = if (builtin.os.tag == .windows) @ptrFromInt(std.math.maxInt(usize)) else -1,
                .flags = .{ .nonblocking = false },
            };
        }

        fn create(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.CreateFileOptions) fs.File.OpenError!fs.File {
            return .{
                .handle = if (builtin.os.tag == .windows) @ptrFromInt(std.math.maxInt(usize)) else -1,
                .flags = .{ .nonblocking = false },
            };
        }

        fn close(data: ?*anyopaque, files: []const fs.File) void {
            const count: *usize = @ptrCast(@alignCast(data.?));
            count.* += files.len;
        }
    };

    var count: usize = 0;
    var vtable = testing.io.vtable.*;
    vtable.dirOpenFile = Fake.open;
    vtable.dirCreateFile = Fake.create;
    vtable.fileClose = Fake.close;
    const fake_io: std.Io = .{ .vtable = &vtable, .userdata = &count };
    var pool: WorkerPool = undefined;
    var open: fs.Open = .init(&pool, .{ .dir = .cwd(), .path = "ignored", .direct = true });
    var create: fs.Create = .init(&pool, .{ .dir = .cwd(), .path = "ignored", .direct = true });

    try testing.expectError(error.BadFileDescriptor, open.action.run(fake_io));
    try testing.expectError(error.BadFileDescriptor, create.action.run(fake_io));
    try testing.expectEqual(2, count);
}

test "filesystem Allocate reserves grows preserves data and never shrinks" {
    try testing.expect(fs.Allocate == fs.worker.Allocate);

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 1);
        defer pool.deinit();

        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        var create: co.Frame(fs.Create) = .{
            .coroutine = .init(&pool, .{
                .dir = tmp.dir,
                .path = "allocated",
                .flags = .{ .read = true },
            }),
        };

        finish(&loop, &create);

        const file = try create.getResult();
        defer file.close(testing.io);
        const size = 16 * 1024;
        const readonly = try tmp.dir.openFile(testing.io, "allocated", .{});
        defer readonly.close(testing.io);

        var denied: co.Frame(fs.Allocate) = .{
            .coroutine = .init(&pool, .{ .file = readonly, .len = size }),
        };

        finish(&loop, &denied);

        try testing.expectError(
            if (builtin.os.tag == .windows) error.AccessDenied else error.NotOpenForWriting,
            denied.getResult(),
        );
        try testing.expectEqual(0, (try file.stat(testing.io)).size);

        var allocate: co.Frame(fs.Allocate) = .{
            .coroutine = .init(&pool, .{ .file = file, .len = size }),
        };

        finish(&loop, &allocate);

        try allocate.getResult();
        try testing.expectEqual(size, (try file.stat(testing.io)).size);

        var buffer: [size]u8 = @splat(0x5a);
        buffer[size - 1] = 0x2a;
        var offset: usize = 0;

        while (offset < size) {
            var write: co.Frame(fs.WriteAt) = .{
                .coroutine = .init(&pool, .{
                    .file = file,
                    .buffer = buffer[offset..],
                    .offset = offset,
                }),
            };

            finish(&loop, &write);

            const n = try write.getResult();
            try testing.expect(n > 0);
            offset += n;
        }

        var sync: co.Frame(fs.Sync) = .{
            .coroutine = .init(&pool, .{ .file = file }),
        };

        finish(&loop, &sync);

        try sync.getResult();

        for ([_]struct { offset: u64, len: u64, expected_size: u64 }{
            .{ .offset = 0, .len = 1, .expected_size = size },
            .{ .offset = size * 2, .len = size, .expected_size = size * 3 },
            .{ .offset = 3, .len = 5, .expected_size = size * 3 },
            .{ .offset = std.math.maxInt(i64), .len = 0, .expected_size = size * 3 },
        }) |case| {
            allocate = .{
                .coroutine = .init(&pool, .{ .file = file, .offset = case.offset, .len = case.len }),
            };

            finish(&loop, &allocate);

            allocate.getResult() catch |err| {
                std.debug.print("Allocate offset={d} len={d} expected_size={d}: {s}\n", .{ case.offset, case.len, case.expected_size, @errorName(err) });
                return err;
            };
            try testing.expectEqual(case.expected_size, (try file.stat(testing.io)).size);
        }

        @memset(&buffer, 0);
        offset = 0;

        while (offset < size) {
            var read: co.Frame(fs.ReadAt) = .{
                .coroutine = .init(&pool, .{
                    .file = file,
                    .buffer = buffer[offset..],
                    .offset = offset,
                }),
            };

            finish(&loop, &read);

            const n = try read.getResult();
            try testing.expect(n > 0);
            offset += n;
        }

        for (buffer[0 .. size - 1]) |byte| try testing.expectEqual(0x5a, byte);
        try testing.expectEqual(0x2a, buffer[size - 1]);

        var zeros: [32]u8 = undefined;
        var read: co.Frame(fs.ReadAt) = .{
            .coroutine = .init(&pool, .{
                .file = file,
                .buffer = &zeros,
                .offset = size * 2,
            }),
        };

        finish(&loop, &read);

        try testing.expectEqual(zeros.len, try read.getResult());
        for (zeros) |byte| try testing.expectEqual(0, byte);

        allocate = .{
            .coroutine = .init(&pool, .{ .file = file, .len = size * 4 }),
        };

        allocate.coroutine.cancel();
        finish(&loop, &allocate);

        try testing.expectError(error.OperationCanceled, allocate.getResult());
        try testing.expectEqual(size * 3, (try file.stat(testing.io)).size);
        try testing.expectEqual(0, pool.outstanding);
        try support.drained(&loop);
    }
}

test "filesystem Allocate validates signed ranges and empty requests do no I/O" {
    var pool: WorkerPool = undefined;

    for ([_]struct { offset: u64, len: u64 }{
        .{ .offset = std.math.maxInt(u64), .len = 0 },
        .{ .offset = @as(u64, std.math.maxInt(i64)) + 1, .len = 1 },
        .{ .offset = std.math.maxInt(i64), .len = 1 },
        .{ .offset = 1, .len = std.math.maxInt(i64) },
        .{ .offset = 0, .len = std.math.maxInt(u64) },
    }) |case| {
        var allocate: fs.Allocate = .init(&pool, .{ .file = .stdout(), .offset = case.offset, .len = case.len });

        try testing.expectError(error.InvalidArgument, allocate.action.run(testing.io));
    }

    var allocate: fs.Allocate = .init(&pool, .{ .file = .stdout(), .offset = std.math.maxInt(i64), .len = 0 });

    try allocate.action.run(testing.io);
}
