const std = @import("std");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const WorkerPool = @import("../../workers/root.zig").Pool;
const fs = @import("../root.zig");
const support = @import("../../event/tests/support.zig");
const testing = std.testing;

pub fn finish(loop: *io.Loop, frame: anytype) void {
    loop.executor.spawn(&frame.task);
    support.run(loop);

    std.debug.assert(frame.task.outstanding_io == 0);

    frame.coroutine.deinit();
}

pub fn roundtrip(loop: *io.Loop) !void {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    var pool: WorkerPool = undefined;
    try pool.init(testing.allocator, loop, 2);
    defer pool.deinit();

    var create: co.Frame(fs.Create) = .{
        .coroutine = .init(&pool, .{
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
            .coroutine = .init(&pool, .{
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
        .coroutine = .init(&pool, .{
            .file = file,
            .buffer = "Z",
            .offset = 7,
        }),
    };

    finish(loop, &patch);

    try testing.expectEqual(1, try patch.getResult());

    var sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };

    finish(loop, &sync);

    try sync.getResult();

    var data_sync: co.Frame(fs.Sync) = .{
        .coroutine = .init(&pool, .{ .file = file, .data_only = true }),
    };

    finish(loop, &data_sync);

    try data_sync.getResult();

    var buffer: [64]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{
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
        .coroutine = .init(&pool, .{
            .file = file,
            .buffer = &buffer,
            .offset = bytes.len,
        }),
    };

    finish(loop, &eof);

    try testing.expectEqual(0, try eof.getResult());

    var empty_write: co.Frame(fs.WriteAt) = .{
        .coroutine = .init(&pool, .{
            .file = file,
            .buffer = "",
            .offset = bytes.len,
        }),
    };

    finish(loop, &empty_write);

    try testing.expectEqual(0, try empty_write.getResult());

    var empty_read: co.Frame(fs.ReadAt) = .{
        .coroutine = .init(&pool, .{
            .file = file,
            .buffer = buffer[0..0],
            .offset = std.math.maxInt(i64),
        }),
    };

    finish(loop, &empty_read);

    try testing.expectEqual(0, try empty_read.getResult());

    var close: co.Frame(fs.Close) = .{
        .coroutine = .init(&pool, .{ .file = file }),
    };

    finish(loop, &close);

    try close.getResult();
    closed = true;

    var open: co.Frame(fs.Open) = .{
        .coroutine = .init(&pool, .{
            .dir = tmp.dir,
            .path = "roundtrip",
        }),
    };

    finish(loop, &open);

    const reopened = try open.getResult();
    defer reopened.close(testing.io);

    var short_buffer: [3]u8 = undefined;
    read = .{
        .coroutine = .init(&pool, .{
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
