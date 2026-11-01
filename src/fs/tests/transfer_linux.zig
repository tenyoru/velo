const std = @import("std");
const linux = std.os.linux;
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const fs = @import("../root.zig");
const net = @import("../../net/root.zig");
const Pool = @import("../../workers/Pool.zig");
const native = @import("../transfer_linux.zig");
const support = @import("../../event/tests/support.zig");
const testing = std.testing;

test "transfer Linux kernel copy paths move data without buffered fallback" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = try tmp.dir.createFile(testing.io, "input", .{ .read = true });
    defer input.close(testing.io);
    const output = try tmp.dir.createFile(testing.io, "output", .{ .read = true });
    defer output.close(testing.io);
    const data = "native copy";
    try input.writePositionalAll(testing.io, data, 0);
    var scratch: [data.len]u8 = undefined;
    var got: [data.len]u8 = undefined;
    const copied = try native.copy(.{ .file = .{ .file = input } }, .{ .file = .{ .file = output } }, &scratch);
    if (copied.fallback) return error.SkipZigTest;
    try testing.expectEqual(data.len, copied.written);
    try testing.expectEqual(data.len, try output.readPositionalAll(testing.io, &got, 0));
    try testing.expectEqualStrings(data, &got);

    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    const pair = try support.pair(&loop, .ipv4);
    defer for (pair) |fd| support.close(fd);
    for (pair) |fd| try native.nonblocking(fd);

    const sent = try native.copy(.{ .file = .{ .file = input } }, .{ .stream = .init(&loop, pair[0]) }, &scratch);
    try testing.expectEqual(data.len, sent.written);
    try testing.expect(!sent.fallback);
    var receive: co.Frame(net.ReadExact) = .init(.init(&loop, .{
        .fd = pair[1],
        .buffer = &got,
        .deadline = try io.deadlineIn(io.seconds(5)),
    }));
    try loop.block(&receive);
    try testing.expectEqualStrings(data, &got);

    var send: co.Frame(net.SendAll) = .init(.init(&loop, .{ .fd = pair[0], .buffer = data }));
    try loop.block(&send);
    const spliced = try native.copy(.{ .stream = .init(&loop, pair[1]) }, .{ .file = .{ .file = output, .offset = data.len } }, &scratch);
    try testing.expectEqual(data.len, spliced.written);
    try testing.expect(!spliced.fallback);
    try testing.expectEqual(data.len, try output.readPositionalAll(testing.io, &got, data.len));
    try testing.expectEqualStrings(data, &got);
}

test "transfer Linux broken outputs preserve spliced input and do not deliver SIGPIPE" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = try tmp.dir.createFile(testing.io, "input", .{ .read = true });
    defer input.close(testing.io);
    const data = "recover me";
    try input.writePositionalAll(testing.io, data, 0);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    const source = try support.pair(&loop, .ipv4);
    defer for (source) |fd| support.close(fd);
    const destination = try support.pair(&loop, .ipv4);
    defer for (destination) |fd| support.close(fd);
    try native.nonblocking(source[1]);
    try native.nonblocking(destination[0]);
    try io.shutdownSocket(destination[0], .write);
    var send: co.Frame(net.SendAll) = .init(.init(&loop, .{ .fd = source[0], .buffer = data }));
    try loop.block(&send);

    const Child = struct {
        fn check(file: std.Io.File, incoming: net.Stream, outgoing: net.Stream) !void {
            const action: linux.Sigaction = .{ .handler = .{ .handler = linux.SIG.DFL }, .mask = linux.sigemptyset(), .flags = 0 };
            if (linux.errno(linux.sigaction(.PIPE, &action, null)) != .SUCCESS) return error.SignalSetup;
            var mask = linux.sigemptyset();
            linux.sigaddset(&mask, .PIPE);
            if (linux.errno(linux.sigprocmask(linux.SIG.UNBLOCK, &mask, null)) != .SUCCESS) return error.SignalSetup;
            var scratch: [data.len]u8 = undefined;
            if (native.copy(.{ .file = .{ .file = file } }, .{ .stream = outgoing }, &scratch)) |_| {
                return error.ExpectedBrokenPipe;
            } else |err| if (err != error.BrokenPipe) return err;
            const progress = try native.copy(.{ .stream = incoming }, .{ .stream = outgoing }, &scratch);
            if (progress.failure == null or progress.failure.? != error.BrokenPipe or progress.written != 0 or progress.read != data.len or
                !std.mem.eql(u8, &scratch, data)) return error.LostInput;
            var restored: linux.sigset_t = undefined;
            _ = linux.sigprocmask(linux.SIG.BLOCK, null, &restored);
            if (linux.sigismember(&restored, .PIPE)) return error.MaskNotRestored;

            // A signal already pending before the call belongs to the caller, not us.
            _ = linux.sigprocmask(linux.SIG.BLOCK, &mask, null);
            _ = linux.tgkill(linux.getpid(), linux.gettid(), .PIPE);
            _ = native.copy(.{ .file = .{ .file = file } }, .{ .stream = outgoing }, &scratch) catch {};
            var pending: linux.sigset_t = undefined;
            _ = linux.syscall2(.rt_sigpending, @intFromPtr(&pending), linux.NSIG / 8);
            if (!linux.sigismember(&pending, .PIPE)) return error.StolePendingSignal;
        }
    };
    const pid = linux.fork();
    if (linux.errno(pid) != .SUCCESS) return error.TestForkFailed;
    if (pid == 0) {
        Child.check(input, .init(&loop, source[1]), .init(&loop, destination[0])) catch linux.exit_group(1);
        linux.exit_group(0);
    }
    var status: i32 = undefined;
    while (true) {
        const e = linux.errno(linux.waitpid(@intCast(pid), &status, 0));
        if (e == .INTR) continue;
        try testing.expectEqual(linux.E.SUCCESS, e);
        break;
    }
    const code: u32 = @bitCast(status);
    try testing.expect(linux.W.IFEXITED(code));
    try testing.expectEqual(0, linux.W.EXITSTATUS(code));
}

test "transfer Linux backpressure recovers pipe bytes for buffered sending" {
    var incoming: [2]linux.fd_t = undefined;
    var outgoing: [2]linux.fd_t = undefined;
    const flags = linux.SOCK.STREAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.socketpair(linux.AF.UNIX, flags, 0, &incoming)));
    defer for (incoming) |fd| {
        _ = linux.close(fd);
    };
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.socketpair(linux.AF.UNIX, flags, 0, &outgoing)));
    defer for (outgoing) |fd| {
        _ = linux.close(fd);
    };
    const data = "buffered after splice";
    try testing.expectEqual(data.len, linux.sendto(incoming[0], data.ptr, data.len, linux.MSG.NOSIGNAL, null, 0));
    const filler: [8192]u8 = @splat(0);
    while (true) {
        const rc = linux.sendto(outgoing[0], &filler, filler.len, linux.MSG.NOSIGNAL, null, 0);
        if (linux.errno(rc) == .AGAIN) break;
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
        try testing.expect(rc != 0);
    }
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var scratch: [data.len]u8 = undefined;
    const progress = try native.copy(.{ .stream = .init(&loop, incoming[1]) }, .{ .stream = .init(&loop, outgoing[0]) }, &scratch);
    try testing.expectEqual(data.len, progress.read);
    try testing.expectEqual(0, progress.written);
    try testing.expectEqual(null, progress.failure);
    try testing.expect(progress.fallback);
    try testing.expectEqualStrings(data, &scratch);
}

test "transfer Linux cross-filesystem fallback reads virtual files and rejects append and aliases" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const input = try std.Io.Dir.cwd().openFile(testing.io, "/proc/version", .{});
    defer input.close(testing.io);
    const output = try tmp.dir.createFile(testing.io, "output", .{ .read = true });
    defer output.close(testing.io);
    var expected: [512]u8 = undefined;
    const count = try input.readPositionalAll(testing.io, &expected, 0);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var scratch: [71]u8 = undefined;
    const options: fs.Transfer.Options = .{
        .source = .{ .file = .{ .file = input } },
        .destination = .{ .file = .{ .file = output } },
        .buffer = &scratch,
        .len = count,
    };
    var frame: co.Frame(fs.Transfer) = .init(.init(&pool, options));
    try testing.expectEqual(count, try loop.block(&frame));
    try testing.expect(!frame.coroutine.fast);
    var got: [512]u8 = undefined;
    try testing.expectEqual(count, try output.readPositionalAll(testing.io, got[0..count], 0));
    try testing.expectEqualSlices(u8, expected[0..count], got[0..count]);

    const alias = try tmp.dir.openFile(testing.io, "output", .{ .mode = .read_write });
    defer alias.close(testing.io);
    var overlap = options;
    overlap.source = .{ .file = .{ .file = output } };
    overlap.destination = .{ .file = .{ .file = alias, .offset = 1 } };
    overlap.len = count - 1;
    frame = .init(.init(&pool, overlap));
    try testing.expectError(error.InvalidArgument, loop.block(&frame));
    try testing.expectEqual(0, frame.coroutine.read);

    var flags: linux.O = @bitCast(@as(u32, @intCast(linux.fcntl(output.handle, linux.F.GETFL, 0))));
    flags.APPEND = true;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.fcntl(output.handle, linux.F.SETFL, @as(u32, @bitCast(flags)))));
    frame = .init(.init(&pool, options));
    try testing.expectError(error.InvalidArgument, loop.block(&frame));
    try testing.expectEqual(count, (try output.stat(testing.io)).size);
    try support.drained(&loop);
}

test "transfer Linux a completed native chunk wins cancellation and retains progress" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const source = try tmp.dir.createFile(testing.io, "source", .{ .read = true });
    defer source.close(testing.io);
    const destination = try tmp.dir.createFile(testing.io, "destination", .{ .read = true });
    defer destination.close(testing.io);
    try source.writePositionalAll(testing.io, "done", 0);
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    var pool: Pool = undefined;
    try pool.init(testing.allocator, &loop, 1);
    defer pool.deinit();
    var scratch: [4]u8 = undefined;
    var frame: co.Frame(fs.Transfer) = .init(.init(&pool, .{
        .source = .{ .file = .{ .file = source } },
        .destination = .{ .file = .{ .file = destination } },
        .len = 4,
        .buffer = &scratch,
    }));
    loop.spawn(&frame);
    loop.executor.run();
    while (loop.control.head.load(.acquire) == null) std.Thread.yield() catch {};
    _ = loop.control.service(&loop.executor);
    loop.executor.run();
    std.debug.assert(frame.coroutine.state == .native);
    while (loop.control.head.load(.acquire) == null) std.Thread.yield() catch {};
    const progress = frame.coroutine.child.native.result;
    frame.cancel();
    support.run(&loop);
    const result = try progress;
    try testing.expectEqual(result.written, frame.coroutine.written);
    if (result.written == 4) {
        try testing.expectEqual(4, try frame.getResult());
    } else {
        try testing.expectError(error.OperationCanceled, frame.getResult());
    }
    try support.drained(&loop);
}
