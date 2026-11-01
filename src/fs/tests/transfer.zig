const std = @import("std");
const builtin = @import("builtin");
const fs = @import("../root.zig");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const net = @import("../../net/root.zig");
const Pool = @import("../../workers/Pool.zig");
const support = @import("../../event/tests/support.zig");
const testing = std.testing;

test "transfer file and stream combinations preserve bytes, offsets and progress" {
    const data = try testing.allocator.alloc(u8, 192 * 1024 + 17);
    defer testing.allocator.free(data);
    for (data, 0..) |*byte, i| byte.* = @truncate(i *% 71);
    const received = try testing.allocator.alloc(u8, data.len);
    defer testing.allocator.free(received);

    for (support.modes) |mode| {
        for ([_]bool{ false, true }) |buffered| {
            for ([_]bool{ false, true }) |source_stream| {
                for ([_]bool{ false, true }) |destination_stream| {
                    var tmp = testing.tmpDir(.{});
                    defer tmp.cleanup();
                    const source = try tmp.dir.createFile(testing.io, "source", .{ .read = true });
                    defer source.close(testing.io);
                    const destination = try tmp.dir.createFile(testing.io, "destination", .{ .read = true });
                    defer destination.close(testing.io);
                    try source.writePositionalAll(testing.io, "prefix", 0);
                    try source.writePositionalAll(testing.io, data, 6);
                    try destination.writePositionalAll(testing.io, "pre", 0);
                    try destination.writePositionalAll(testing.io, "tail", 3 + data.len);

                    var loop = try support.initLoop(8, mode);
                    defer loop.deinit();
                    var pool: Pool = undefined;
                    try pool.init(testing.allocator, &loop, 1);
                    defer pool.deinit();
                    const input = try support.pair(&loop, .ipv4);
                    defer for (input) |fd| support.close(fd);
                    const output = try support.pair(&loop, .ipv4);
                    defer for (output) |fd| support.close(fd);

                    const until = try io.deadlineIn(io.seconds(10));
                    var scratch: [8192]u8 = undefined;
                    var transfer: fs.Transfer = .init(&pool, .{
                        .source = if (source_stream) .{ .stream = .init(&loop, input[1]) } else .{ .file = .{ .file = source, .offset = 6 } },
                        .destination = if (destination_stream) .{ .stream = .init(&loop, output[0]) } else .{ .file = .{ .file = destination, .offset = 3 } },
                        .len = data.len,
                        .buffer = &scratch,
                    });
                    if (buffered) transfer.fast = false;
                    const Work = co.Join(.{ .transfer = io.WithTimeout(fs.Transfer), .send = net.SendAll, .recv = net.ReadExact });
                    var work: co.Frame(Work) = .init(.init(.{
                        .transfer = .init(&loop, transfer, .{ .duration = io.seconds(10) }),
                        .send = .init(&loop, .{ .fd = input[0], .buffer = if (source_stream) data else "", .deadline = until }),
                        .recv = .init(&loop, .{ .fd = output[1], .buffer = if (destination_stream) received else received[0..0], .deadline = until }),
                    }));
                    const result = loop.block(&work);
                    try result.send;
                    try result.recv;
                    try testing.expectEqual(data.len, try result.transfer);
                    const copied = &work.coroutine.children.transfer.child;
                    try testing.expectEqual(data.len, copied.read);
                    try testing.expectEqual(data.len, copied.written);
                    try testing.expectEqual(0, copied.buffered().len);
                    copied.deinit();
                    if (builtin.os.tag == .windows) {
                        // Synchronous Windows handles update their cursor even for explicit offsets.
                        try expectWindowsPosition(source, 6 + data.len);
                        try expectWindowsPosition(destination, 3 + data.len + @as(u64, if (destination_stream) 4 else 0));
                    } else {
                        var prefix: [6]u8 = undefined;
                        try testing.expectEqual(6, try source.readStreaming(testing.io, &.{&prefix}));
                        try testing.expectEqualStrings("prefix", &prefix);
                        try testing.expectEqual(3, try destination.readStreaming(testing.io, &.{prefix[0..3]}));
                        try testing.expectEqualStrings("pre", prefix[0..3]);
                    }
                    if (!destination_stream) {
                        try testing.expectEqual(data.len, try destination.readPositionalAll(testing.io, received, 3));
                        var edge: [4]u8 = undefined;
                        _ = try destination.readPositionalAll(testing.io, edge[0..3], 0);
                        try testing.expectEqualStrings("pre", edge[0..3]);
                        _ = try destination.readPositionalAll(testing.io, &edge, 3 + data.len);
                        try testing.expectEqualStrings("tail", &edge);
                    }
                    try testing.expectEqualSlices(u8, data, received);
                    try testing.expectEqual(0, pool.outstanding);
                    try support.drained(&loop);
                }
            }
        }
    }
}

fn expectWindowsPosition(file: fs.File, expected: u64) !void {
    const w = std.os.windows;
    var status: w.IO_STATUS_BLOCK = undefined;
    var position: w.FILE.POSITION_INFORMATION = undefined;
    try testing.expectEqual(w.NTSTATUS.SUCCESS, w.ntdll.NtQueryInformationFile(
        file.handle,
        &status,
        &position,
        @sizeOf(w.FILE.POSITION_INFORMATION),
        .Position,
    ));
    try testing.expectEqual(expected, @as(u64, @intCast(position.CurrentByteOffset)));
}

test "transfer validates ranges and scratch, preserves empty requests, and stops at EOF" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const source = try tmp.dir.createFile(testing.io, "source", .{ .read = true });
    defer source.close(testing.io);
    const destination = try tmp.dir.createFile(testing.io, "destination", .{ .read = true });
    defer destination.close(testing.io);
    try source.writePositionalAll(testing.io, "abc", 0);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var scratch: [2]u8 = undefined;
    const options: fs.Transfer.Options = .{
        .source = .{ .file = .{ .file = source } },
        .destination = .{ .file = .{ .file = destination } },
        .buffer = &scratch,
        .len = 10,
    };
    var eof: co.Frame(fs.Transfer) = .init(.init(&pool, options));
    try testing.expectEqual(3, try loop.block(&eof));
    try testing.expectEqual(3, eof.coroutine.read);
    var contents: [4]u8 = undefined;
    const n = try destination.readPositionalAll(testing.io, &contents, 0);
    try testing.expectEqualStrings("abc", contents[0..n]);

    var empty_options = options;
    empty_options.len = 0;
    empty_options.buffer = &.{};
    var empty: co.Frame(fs.Transfer) = .init(.init(&pool, empty_options));
    try testing.expectEqual(0, try loop.block(&empty));
    var other = try io.Loop.init(.{});
    defer other.deinit();
    for (0..5) |case| {
        var invalid = options;
        switch (case) {
            0 => invalid.buffer = &.{},
            1 => invalid.source.file.offset = std.math.maxInt(u64),
            2 => invalid.destination.file.offset = std.math.maxInt(i64) - 1,
            3 => invalid.destination = invalid.source,
            4 => invalid.source = .{ .stream = .init(&other, 0) },
            else => unreachable,
        }
        var frame: co.Frame(fs.Transfer) = .init(.init(&pool, invalid));
        try testing.expectError(error.InvalidArgument, loop.block(&frame));
        try testing.expectEqual(0, frame.coroutine.read);
    }
    var canceled: co.Frame(fs.Transfer) = .init(.init(&pool, options));
    canceled.cancel();
    try testing.expectError(error.OperationCanceled, loop.block(&canceled));
    try testing.expectEqual(0, canceled.coroutine.read);
    loop.shutdown();
    canceled = .init(.init(&pool, options));
    try testing.expectError(error.OperationCanceled, loop.block(&canceled));
    try testing.expectEqual(0, canceled.coroutine.read);
    try support.drained(&loop);
}

test "transfer fallback uses native-compatible file wrappers on associated handles" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var create: co.Frame(fs.Create) = .init(.init(&pool, .{ .dir = tmp.dir, .path = "source", .flags = .{ .read = true } }));
    const source = try loop.block(&create);
    defer source.close(testing.io);
    create = .init(.init(&pool, .{ .dir = tmp.dir, .path = "destination", .flags = .{ .read = true } }));
    const destination = try loop.block(&create);
    defer destination.close(testing.io);
    var seed: co.Frame(fs.WriteAt) = .init(.init(&pool, .{ .file = source, .buffer = "native handles" }));
    try testing.expectEqual(14, try loop.block(&seed));
    seed = .init(.init(&pool, .{ .file = destination, .buffer = "." }));
    try testing.expectEqual(1, try loop.block(&seed));

    var scratch: [3]u8 = undefined;
    var copy: co.Frame(fs.Transfer) = .init(.init(&pool, .{
        .source = .{ .file = .{ .file = source } },
        .destination = .{ .file = .{ .file = destination } },
        .buffer = &scratch,
        .len = 14,
    }));
    copy.coroutine.fast = false;
    try testing.expectEqual(14, try loop.block(&copy));
    var bytes: [14]u8 = undefined;
    var read: co.Frame(fs.ReadAt) = .init(.init(&pool, .{ .file = destination, .buffer = &bytes }));
    try testing.expectEqual(14, try loop.block(&read));
    try testing.expectEqualStrings("native handles", &bytes);
    if (builtin.os.tag == .windows) {
        try testing.expect(source.flags.nonblocking and destination.flags.nonblocking);
        try expectWindowsPosition(source, 0);
        try expectWindowsPosition(destination, 0);
    }
    try support.drained(&loop);
}

fn published(loop: *io.Loop) void {
    while (loop.control.head.load(.acquire) == null) std.Thread.yield() catch {};
}

test "transfer cancellation preserves a claimed read without writing or losing buffered input" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const source = try tmp.dir.createFile(testing.io, "source", .{ .read = true });
    defer source.close(testing.io);
    const destination = try tmp.dir.createFile(testing.io, "destination", .{ .read = true });
    defer destination.close(testing.io);
    try source.writePositionalAll(testing.io, "keep me", 0);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    loop.file_ops.read = false;
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var scratch: [16]u8 = undefined;
    var frame: co.Frame(fs.Transfer) = .init(.init(&pool, .{
        .source = .{ .file = .{ .file = source } },
        .destination = .{ .file = .{ .file = destination } },
        .len = 7,
        .buffer = &scratch,
    }));
    frame.coroutine.fast = false;
    loop.spawn(&frame);
    loop.executor.run();
    published(&loop);
    _ = loop.control.service(&loop.executor);
    loop.executor.run();
    std.debug.assert(frame.coroutine.state == .read_file);
    published(&loop);
    frame.cancel();
    support.run(&loop);
    try testing.expectError(error.OperationCanceled, frame.getResult());
    try testing.expectEqual(7, frame.coroutine.read);
    try testing.expectEqual(0, frame.coroutine.written);
    try testing.expectEqualStrings("keep me", frame.coroutine.buffered());
    try testing.expectEqual(0, (try destination.stat(testing.io)).size);
    try support.drained(&loop);
}

test "transfer failed writes preserve consumed input" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const source = try tmp.dir.createFile(testing.io, "source", .{ .read = true });
    defer source.close(testing.io);
    try source.writePositionalAll(testing.io, "retry", 0);
    const destination = try tmp.dir.createFile(testing.io, "destination", .{});
    destination.close(testing.io);
    const read_only = try tmp.dir.openFile(testing.io, "destination", .{});
    defer read_only.close(testing.io);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var scratch: [5]u8 = undefined;
    var copy: co.Frame(fs.Transfer) = .init(.init(&pool, .{
        .source = .{ .file = .{ .file = source } },
        .destination = .{ .file = .{ .file = read_only } },
        .buffer = &scratch,
        .len = 5,
    }));
    copy.coroutine.fast = false;
    if (loop.block(&copy)) |_| return error.ExpectedWriteFailure else |err| switch (err) {
        error.NotOpenForWriting, error.AccessDenied => {},
        else => return err,
    }
    try testing.expectEqual(5, copy.coroutine.read);
    try testing.expectEqual(0, copy.coroutine.written);
    try testing.expectEqualStrings("retry", copy.coroutine.buffered());
    try support.drained(&loop);
}

test "transfer stream EOF returns a short count without closing either endpoint" {
    for (support.modes) |mode| {
        for ([_]bool{ false, true }) |buffered| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();
            var pool: Pool = undefined;
            try pool.init(testing.allocator, &loop, 1);
            defer pool.deinit();
            const pair = try support.pair(&loop, .ipv4);
            defer for (pair) |fd| support.close(fd);
            var send: co.Frame(net.SendAll) = .init(.init(&loop, .{ .fd = pair[0], .buffer = "eof" }));
            try loop.block(&send);
            try io.shutdownSocket(pair[0], .write);
            var scratch: [10]u8 = undefined;
            var copy: co.Frame(fs.Transfer) = .init(.init(&pool, .{
                .source = .{ .stream = .init(&loop, pair[1]) },
                .destination = .{ .stream = .init(&loop, pair[1]) },
                .buffer = &scratch,
                .len = 10,
            }));
            if (buffered) copy.coroutine.fast = false;
            try testing.expectEqual(3, try loop.block(&copy));
            var reply: [3]u8 = undefined;
            var recv: co.Frame(net.ReadExact) = .init(.init(&loop, .{
                .fd = pair[0],
                .buffer = &reply,
                .deadline = try io.deadlineIn(io.seconds(5)),
            }));
            try loop.block(&recv);
            try testing.expectEqualStrings("eof", &reply);
            try support.drained(&loop);
        }
    }
}

test "transfer stream deadlines and timeout cancellation drain idle reads" {
    for (support.modes) |mode| {
        for ([_]bool{ false, true }) |deadline| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();
            var pool: Pool = undefined;
            try pool.init(testing.allocator, &loop, 1);
            defer pool.deinit();
            const pair = try support.pair(&loop, .ipv4);
            defer for (pair) |fd| support.close(fd);
            var input: net.Stream = .init(&loop, pair[0]);
            if (deadline) input.deadline = try io.deadlineIn(io.milliseconds(10));
            var scratch: [16]u8 = undefined;
            const copy: fs.Transfer = .init(&pool, .{
                .source = .{ .stream = input },
                .destination = .{ .stream = .init(&loop, pair[0]) },
                .len = 1,
                .buffer = &scratch,
            });
            if (deadline) try testing.expect(!copy.fast);
            var frame: co.Frame(io.WithTimeout(fs.Transfer)) = .init(.init(&loop, copy, .{
                .duration = if (deadline) io.seconds(5) else io.milliseconds(10),
            }));
            try testing.expectError(error.Timeout, loop.block(&frame));
            try testing.expectEqual(0, frame.coroutine.child.read);
            try testing.expectEqual(0, frame.coroutine.child.written);
            try support.drained(&loop);
        }
    }
}
