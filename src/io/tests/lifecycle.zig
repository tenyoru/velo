const std = @import("std");
const builtin = @import("builtin");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const support = @import("support.zig");
const testing = std.testing;

fn listener() !io.Socket {
    const fd = try io.tcpSocket(.ipv4);
    errdefer support.close(fd);
    var address: io.SockAddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };

    try io.bind(fd, @ptrCast(&address), @sizeOf(@TypeOf(address)));
    try io.listen(fd, 1);

    return fd;
}

fn cancelUnsubmitted(operation: anytype, queued: bool) !void {
    const loop = operation.operation.loop;
    var frame: co.Frame(@TypeOf(operation)) = .{ .coroutine = operation };

    if (queued) {
        loop.executor.spawn(&frame.task);

        std.debug.assert(loop.executor.runOne());
        std.debug.assert(frame.result == null and frame.coroutine.operation.state == .queued);
        std.debug.assert(frame.task.outstanding_io == 1 and loop.active == 1);
    }

    frame.coroutine.cancel();
    frame.coroutine.cancel();
    if (!queued) loop.executor.spawn(&frame.task);
    support.run(loop);
    defer frame.coroutine.deinit();

    try testing.expectError(error.OperationCanceled, frame.result.?);

    frame.coroutine.cancel();
    frame.coroutine.cancel();

    try testing.expectError(error.OperationCanceled, frame.coroutine.poll(&frame.task).ready);
    try testing.expectEqual(0, frame.task.outstanding_io);
    try support.drained(loop);
}

fn registered(op: *const io.Operation) bool {
    if (op.state != .inflight) return false;

    return switch (builtin.os.tag) {
        .linux => op.loop.epoll == null or op.data == .timeout or op.native.epoll.waiting,
        .macos => op.data == .timeout or op.native.registered,
        .windows => true,
        else => unreachable,
    };
}

fn cancelRegistered(loop: *io.Loop, frames: anytype) !void {
    const Canceller = struct {
        targets: @TypeOf(frames),
        timer: io.Timeout,
        inflight: usize = 0,
        first_registered: bool = false,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
            switch (self.timer.poll(task)) {
                .pending => return .pending,
                .ready => |result| result catch |err| return .{ .ready = err },
            }

            self.first_registered = registered(&self.targets[0].coroutine.operation);
            for (self.targets) |*frame| {
                if (frame.coroutine.operation.state == .inflight) self.inflight += 1;
                frame.coroutine.cancel();
                frame.coroutine.cancel();
            }

            return .{ .ready = {} };
        }
    };

    var canceller: co.Frame(Canceller) = .{
        .coroutine = .{
            .targets = frames,
            .timer = io.Timeout.init(loop, .{
                .duration = .{ .sec = 0, .nsec = 5_000_000 },
            }),
        },
    };

    for (frames) |*frame| loop.executor.spawn(&frame.task);
    loop.executor.spawn(&canceller.task);
    support.run(loop);
    defer canceller.coroutine.timer.deinit();

    for (frames) |*frame| frame.coroutine.deinit();

    try canceller.result.?;
    try testing.expect(canceller.coroutine.first_registered);
    try testing.expectEqual(frames.len, canceller.coroutine.inflight);
    try testing.expectEqual(0, canceller.task.outstanding_io);

    for (frames) |*frame| {
        try testing.expectError(error.OperationCanceled, frame.result.?);

        frame.coroutine.cancel();
        frame.coroutine.cancel();

        try testing.expectEqual(0, frame.task.outstanding_io);
    }

    try support.drained(loop);
}

test "lifecycle cancel before polling and while queued is idempotent" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const accepting = try listener();
        defer support.close(accepting);
        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);
        var buffer: [1]u8 = undefined;

        for ([_]bool{ false, true }) |queued| {
            try cancelUnsubmitted(io.Accept.init(&loop, .{ .fd = accepting }), queued);
            try cancelUnsubmitted(
                io.Recv.init(&loop, .{ .fd = fds[0], .buffer = &buffer }),
                queued,
            );
            try cancelUnsubmitted(
                io.Timeout.init(&loop, .{ .duration = .{ .sec = 120, .nsec = 0 } }),
                queued,
            );
        }
    }
}

test "lifecycle cancel registered accept receives and long timer drains tiny queues" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const accepting = try listener();
        defer support.close(accepting);
        var accepts = [_]co.Frame(io.Accept){
            .{ .coroutine = io.Accept.init(&loop, .{ .fd = accepting }) },
        };

        try cancelRegistered(&loop, &accepts);

        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);
        var buffers: [6][1]u8 = undefined;
        var recvs: [buffers.len]co.Frame(io.Recv) = undefined;
        const until = try support.deadline(3_000);

        for (&recvs, &buffers, 0..) |*recv, *buffer, i| {
            recv.* = .{
                .coroutine = io.Recv.init(&loop, .{
                    .fd = fds[0],
                    .buffer = buffer,
                    .deadline = if (i % 2 == 0) null else until,
                }),
            };
        }

        try cancelRegistered(&loop, &recvs);

        var timers = [_]co.Frame(io.Timeout){
            .{
                .coroutine = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 120, .nsec = 0 },
                }),
            },
        };

        try cancelRegistered(&loop, &timers);
    }
}

test "lifecycle join waits for both real timers and completed cancellation is a noop" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const before = try io.now();
        var joined: co.Frame(co.Join(io.Timeout, io.Timeout)) = .{
            .coroutine = .{
                .a = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 2_000_000 },
                }),
                .b = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 12_000_000 },
                }),
            },
        };

        loop.executor.spawn(&joined.task);
        support.run(&loop);
        defer joined.coroutine.a.deinit();
        defer joined.coroutine.b.deinit();

        const result = joined.result.?;
        try result.a;
        try result.b;
        const after = try io.now();
        try testing.expect(
            (@as(i128, after.sec) - before.sec) * 1_000_000_000 +
                after.nsec - before.nsec >= 12_000_000,
        );

        joined.coroutine.a.cancel();
        joined.coroutine.a.cancel();
        joined.coroutine.b.cancel();
        joined.coroutine.b.cancel();

        try testing.expect(!joined.coroutine.a.operation.cancel_requested);
        try testing.expect(!joined.coroutine.b.operation.cancel_requested);
        try joined.coroutine.a.poll(&joined.task).ready;
        try joined.coroutine.b.poll(&joined.task).ready;
        try testing.expectEqual(0, joined.task.outstanding_io);
        try support.drained(&loop);
    }
}

test "lifecycle race keeps timer success drains canceled receive and permits socket reuse" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);

        var buffer: [1]u8 = undefined;
        var raced: co.Frame(co.Race(io.Recv, io.Timeout)) = .{
            .coroutine = .{
                .a = io.Recv.init(&loop, .{ .fd = fds[0], .buffer = &buffer }),
                .b = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 5_000_000 },
                }),
            },
        };

        loop.executor.spawn(&raced.task);
        support.run(&loop);

        raced.coroutine.a.deinit();
        raced.coroutine.b.deinit();

        const result = raced.result.?;
        try testing.expectEqual(.b, result.winner);
        try result.b;
        try testing.expectError(error.OperationCanceled, result.a);
        try testing.expectEqual(0, raced.task.outstanding_io);
        try support.drained(&loop);

        const until = try support.deadline(3_000);
        var transfer: co.Frame(co.Join(io.Send, io.Recv)) = .{
            .coroutine = .{
                .a = io.Send.init(&loop, .{
                    .fd = fds[1],
                    .buffer = "x",
                    .deadline = until,
                }),
                .b = io.Recv.init(&loop, .{
                    .fd = fds[0],
                    .buffer = &buffer,
                    .deadline = until,
                }),
            },
        };

        loop.executor.spawn(&transfer.task);
        support.run(&loop);
        defer transfer.coroutine.a.deinit();
        defer transfer.coroutine.b.deinit();

        try testing.expectEqual(1, try transfer.result.?.a);
        try testing.expectEqual(1, try transfer.result.?.b);
        try testing.expectEqualStrings("x", &buffer);
        try testing.expectEqual(0, transfer.task.outstanding_io);
        try support.drained(&loop);
    }
}
