const std = @import("std");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const WorkerPool = @import("../WorkerPool.zig");
const fs = @import("../fs.zig");
const support = @import("support.zig");
const testing = std.testing;

fn finish(loop: *io.Loop, frame: anytype) void {
    loop.executor.spawn(&frame.task);
    support.run(loop);

    std.debug.assert(frame.task.outstanding_io == 0);

    frame.coroutine.deinit();
}

test "filesystem create positional roundtrip sync close and reopen" {
    for (support.modes) |mode| {
        var tmp = testing.tmpDir(.{});
        defer tmp.cleanup();

        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var pool: WorkerPool = undefined;
        try pool.init(testing.allocator, &loop, 2);
        defer pool.deinit();

        var create: co.Frame(fs.Create) = .{
            .coroutine = fs.Create.init(&pool, .{
                .dir = tmp.dir,
                .path = "roundtrip",
                .flags = .{ .read = true },
            }),
        };

        finish(&loop, &create);

        const file = try create.result.?;
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

            finish(&loop, &write);

            const n = try write.result.?;
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

        finish(&loop, &patch);

        try testing.expectEqual(1, try patch.result.?);

        var sync: co.Frame(fs.Sync) = .{ .coroutine = fs.Sync.init(&pool, .{ .file = file }) };

        finish(&loop, &sync);

        try sync.result.?;

        var buffer: [64]u8 = undefined;
        var read: co.Frame(fs.ReadAt) = .{
            .coroutine = fs.ReadAt.init(&pool, .{
                .file = file,
                .buffer = &buffer,
                .offset = 7,
            }),
        };

        finish(&loop, &read);

        const n = try read.result.?;
        try testing.expectEqual(11, n);
        try testing.expectEqualStrings("Zello world", buffer[0..n]);

        var eof: co.Frame(fs.ReadAt) = .{
            .coroutine = fs.ReadAt.init(&pool, .{
                .file = file,
                .buffer = &buffer,
                .offset = bytes.len,
            }),
        };

        finish(&loop, &eof);

        try testing.expectEqual(0, try eof.result.?);

        var empty_write: co.Frame(fs.WriteAt) = .{
            .coroutine = fs.WriteAt.init(&pool, .{
                .file = file,
                .buffer = "",
                .offset = bytes.len,
            }),
        };

        finish(&loop, &empty_write);

        try testing.expectEqual(0, try empty_write.result.?);

        var close: co.Frame(fs.Close) = .{ .coroutine = fs.Close.init(&pool, .{ .file = file }) };

        finish(&loop, &close);

        try close.result.?;
        closed = true;

        var open: co.Frame(fs.Open) = .{
            .coroutine = fs.Open.init(&pool, .{
                .dir = tmp.dir,
                .path = "roundtrip",
            }),
        };

        finish(&loop, &open);

        const reopened = try open.result.?;
        defer reopened.close(testing.io);

        var short_buffer: [3]u8 = undefined;
        read = .{
            .coroutine = fs.ReadAt.init(&pool, .{
                .file = reopened,
                .buffer = &short_buffer,
                .offset = 7,
            }),
        };

        finish(&loop, &read);

        try testing.expectEqual(3, try read.result.?);
        try testing.expectEqualStrings("Zel", &short_buffer);
        try testing.expectEqual(0, pool.outstanding);
        try testing.expect(!loop.control.hasWork());
        try testing.expectEqual(null, loop.control.registrations.first);
    }
}

test "filesystem missing paths propagate errors and cancellation before open acquires nothing" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var loop = try io.Loop.init(8);
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

    try testing.expectError(error.FileNotFound, open.result.?);

    var create: co.Frame(fs.Create) = .{
        .coroutine = fs.Create.init(&pool, .{
            .dir = tmp.dir,
            .path = "absent/file",
        }),
    };

    finish(&loop, &create);

    try testing.expectError(error.FileNotFound, create.result.?);

    create = .{
        .coroutine = fs.Create.init(&pool, .{
            .dir = tmp.dir,
            .path = "canceled",
        }),
    };

    create.coroutine.cancel();
    finish(&loop, &create);

    try testing.expectError(error.OperationCanceled, create.result.?);
    try testing.expectError(error.FileNotFound, tmp.dir.openFile(testing.io, "canceled", .{}));
}

test "filesystem completed open wins cancellation and preserves caller owned handle" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const original = try tmp.dir.createFile(testing.io, "existing", .{});
    original.close(testing.io);

    var loop = try io.Loop.init(8);
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

    const file = try open.result.?;
    var close: co.Frame(fs.Close) = .{ .coroutine = fs.Close.init(&pool, .{ .file = file }) };

    finish(&loop, &close);

    try close.result.?;

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
    var sync = fs.Sync.init(&pool, .{ .file = .stdout() });

    try testing.expectError(
        error.InputOutput,
        sync.action.run(.{ .vtable = &vtable, .userdata = null }),
    );
}
