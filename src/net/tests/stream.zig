const std = @import("std");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const net = @import("../root.zig");
const support = @import("../../event/tests/support.zig");
const testing = std.testing;

test "stream helpers loop preserve partial EOF and allow reverse half-close traffic" {
    const CancelAfterProgress = struct {
        read: net.ReadExact,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(@import("../stream.zig").Error!void) {
            const result = self.read.poll(task);
            if (result == .pending and self.read.offset != 0) self.read.cancel();

            return result;
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);

        const sent = try testing.allocator.alloc(u8, 4 * 1024 * 1024);
        defer testing.allocator.free(sent);
        for (sent, 0..) |*byte, i| byte.* = @truncate(i * 31);
        const received = try testing.allocator.alloc(u8, sent.len);
        defer testing.allocator.free(received);
        const until = try support.deadline(3_000);
        var send: co.Frame(net.SendAll) = .{
            .coroutine = net.SendAll.init(&loop, .{
                .fd = fds[0],
                .buffer = "",
            }),
        };

        send.coroutine.cancel();
        loop.executor.spawn(&send.task);
        support.run(&loop);

        try testing.expectError(error.OperationCanceled, send.getResult());

        send = .{
            .coroutine = net.SendAll.init(&loop, .{
                .fd = fds[0],
                .buffer = sent,
                .deadline = until,
            }),
        };
        var read: co.Frame(net.ReadExact) = .{
            .coroutine = net.ReadExact.init(&loop, .{
                .fd = fds[1],
                .buffer = received,
                .deadline = until,
            }),
        };

        loop.executor.spawn(&read.task);
        loop.executor.spawn(&send.task);
        support.run(&loop);

        try send.getResult();
        try read.getResult();
        try testing.expectEqual(sent.len, send.coroutine.offset);
        try testing.expectEqual(sent.len, read.coroutine.offset);
        try testing.expectEqualSlices(u8, sent, received);

        send.coroutine.deinit();
        read.coroutine.deinit();

        send = .{
            .coroutine = net.SendAll.init(&loop, .{
                .fd = fds[0],
                .buffer = "end",
                .deadline = until,
            }),
        };

        loop.executor.spawn(&send.task);
        support.run(&loop);

        try send.getResult();

        send.coroutine.deinit();

        try io.shutdownSocket(fds[0], .write);

        var partial: [8]u8 = undefined;
        read = .{
            .coroutine = net.ReadExact.init(&loop, .{
                .fd = fds[1],
                .buffer = &partial,
                .deadline = until,
            }),
        };

        loop.executor.spawn(&read.task);
        support.run(&loop);

        try testing.expectError(error.EndOfStream, read.getResult());
        try testing.expectEqual(3, read.coroutine.offset);
        try testing.expectEqualStrings("end", partial[0..3]);

        read.coroutine.deinit();

        send = .{
            .coroutine = net.SendAll.init(&loop, .{
                .fd = fds[1],
                .buffer = "back",
                .deadline = until,
            }),
        };
        read = .{
            .coroutine = net.ReadExact.init(&loop, .{
                .fd = fds[0],
                .buffer = partial[0..4],
                .deadline = until,
            }),
        };

        loop.executor.spawn(&read.task);
        loop.executor.spawn(&send.task);
        support.run(&loop);

        try send.getResult();
        try read.getResult();
        try testing.expectEqualStrings("back", partial[0..4]);

        send.coroutine.deinit();
        read.coroutine.deinit();

        send = .{
            .coroutine = net.SendAll.init(&loop, .{
                .fd = fds[1],
                .buffer = "cut",
                .deadline = until,
            }),
        };

        loop.executor.spawn(&send.task);
        support.run(&loop);

        try send.getResult();

        send.coroutine.deinit();

        var canceled: co.Frame(CancelAfterProgress) = .{
            .coroutine = .{
                .read = net.ReadExact.init(&loop, .{
                    .fd = fds[0],
                    .buffer = &partial,
                    .deadline = until,
                }),
            },
        };

        loop.executor.spawn(&canceled.task);
        support.run(&loop);

        try testing.expectError(error.OperationCanceled, canceled.getResult());
        try testing.expectEqual(3, canceled.coroutine.read.offset);
        try testing.expectEqualStrings("cut", partial[0..3]);

        canceled.coroutine.read.deinit();

        try testing.expectEqual(0, canceled.task.outstanding_io);
        try testing.expectEqual(0, send.task.outstanding_io);
        try support.drained(&loop);
        try testing.expect(!loop.control.hasWork());
        try testing.expectEqual(null, loop.control.registrations.first);
        try testing.expectEqual(null, loop.executor.all_tasks.first);
    }
}
