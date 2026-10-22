const std = @import("std");
const builtin = @import("builtin");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const w = @import("../backends/win32.zig");
const support = @import("support.zig");
const testing = std.testing;

fn invalidSocket() io.Socket {
    return if (builtin.os.tag == .windows) w.INVALID_SOCKET else -1;
}

test "portable io timer reuse and immediate invalid descriptor" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        support.run(&loop);

        const before = try io.now();
        var timer: co.Frame(io.Timeout) = .{
            .coroutine = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 2_000_000 },
            }),
        };

        loop.executor.spawn(&timer.task);
        support.run(&loop);

        try timer.getResult();
        const after = try io.now();
        try testing.expect(
            (@as(i128, after.sec) - before.sec) * 1_000_000_000 +
                after.nsec - before.nsec >= 2_000_000,
        );
        try testing.expectEqual(0, timer.task.outstanding_io);

        timer.coroutine.deinit();

        timer = .{
            .coroutine = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 1 },
            }),
        };

        loop.executor.spawn(&timer.task);
        support.run(&loop);

        try timer.getResult();

        timer.coroutine.deinit();

        try testing.expectEqual(0, timer.task.outstanding_io);

        var buffer: [1]u8 = undefined;
        const invalid = invalidSocket();
        var recv: co.Frame(io.Recv) = .{
            .coroutine = io.Recv.init(&loop, .{
                .fd = invalid,
                .buffer = &buffer,
                .deadline = try support.deadline(3_000),
            }),
        };

        loop.executor.spawn(&recv.task);
        support.run(&loop);

        try testing.expectError(error.BadFileDescriptor, recv.getResult());

        recv.coroutine.deinit();

        try testing.expectEqual(0, recv.task.outstanding_io);
        try support.drained(&loop);
    }
}

test "portable io receive timeout success with deadline async close and EOF" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(4, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv4);
        var open = [2]bool{ true, true };
        defer for (fds, open) |fd, is_open| {
            if (is_open) support.close(fd);
        };

        var buffer: [1]u8 = undefined;
        var recv: co.Frame(io.Recv) = .{
            .coroutine = io.Recv.init(&loop, .{
                .fd = fds[0],
                .buffer = &buffer,
                .deadline = try support.deadline(10),
            }),
        };

        loop.executor.spawn(&recv.task);
        support.run(&loop);

        try testing.expectError(error.Timeout, recv.getResult());

        recv.coroutine.deinit();

        try testing.expectEqual(0, recv.task.outstanding_io);
        try support.drained(&loop);

        recv = .{
            .coroutine = io.Recv.init(&loop, .{
                .fd = fds[0],
                .buffer = &buffer,
                .deadline = try support.deadline(3_000),
            }),
        };
        var send: co.Frame(io.Send) = .{
            .coroutine = io.Send.init(&loop, .{
                .fd = fds[1],
                .buffer = "x",
                .deadline = try support.deadline(3_000),
            }),
        };

        loop.executor.spawn(&recv.task);
        loop.executor.spawn(&send.task);
        support.run(&loop);

        try testing.expectEqual(1, try send.getResult());
        try testing.expectEqual(1, try recv.getResult());
        try testing.expectEqualStrings("x", &buffer);

        send.coroutine.deinit();
        recv.coroutine.deinit();

        try testing.expectEqual(0, send.task.outstanding_io);
        try testing.expectEqual(0, recv.task.outstanding_io);

        var closing: co.Frame(io.Close) = .{ .coroutine = io.Close.init(&loop, .{ .fd = fds[1] }) };

        loop.executor.spawn(&closing.task);
        support.run(&loop);

        try closing.getResult();
        open[1] = false;

        closing.coroutine.deinit();

        try testing.expectEqual(0, closing.task.outstanding_io);

        recv = .{
            .coroutine = io.Recv.init(&loop, .{
                .fd = fds[0],
                .buffer = &buffer,
                .deadline = try support.deadline(3_000),
            }),
        };

        loop.executor.spawn(&recv.task);
        support.run(&loop);

        try testing.expectEqual(0, try recv.getResult());

        recv.coroutine.deinit();

        try testing.expectEqual(0, recv.task.outstanding_io);
        try support.drained(&loop);
    }
}

test "portable io binary TCP echo with client and server transfer loops" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);

        var sent: [2048]u8 = undefined;
        for (&sent, 0..) |*byte, i| byte.* = @truncate(i * 37);
        const expected = sent;
        var echoed: [sent.len]u8 = undefined;
        const until = try support.deadline(3_000);
        var client: co.Frame(support.Echo) = .{
            .coroutine = .{
                .loop = &loop,
                .fd = fds[0],
                .buffer = &sent,
                .sending = true,
                .until = until,
            },
        };
        var server: co.Frame(support.Echo) = .{
            .coroutine = .{
                .loop = &loop,
                .fd = fds[1],
                .buffer = &echoed,
                .sending = false,
                .until = until,
            },
        };

        loop.executor.spawn(&client.task);
        loop.executor.spawn(&server.task);
        support.run(&loop);

        try client.getResult();
        try server.getResult();
        try testing.expectEqualSlices(u8, &expected, &sent);
        try testing.expectEqualSlices(u8, &expected, &echoed);
        try testing.expectEqual(0, client.task.outstanding_io);
        try testing.expectEqual(0, server.task.outstanding_io);
        try support.drained(&loop);
    }
}

test "portable io queued receives on one socket progress after delayed sends" {
    const Sender = struct {
        loop: *io.Loop,
        fd: io.Socket,
        timer: io.Timeout,
        send: io.Send = undefined,
        waiting: bool = true,
        pending: bool = false,
        offset: usize = 0,
        until: io.Timespec,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
            if (self.waiting) {
                switch (self.timer.poll(task)) {
                    .pending => return .pending,
                    .ready => |value| value catch |err| return .{ .ready = err },
                }
                self.waiting = false;
            }

            while (self.offset < 8) {
                if (!self.pending) {
                    self.send = io.Send.init(self.loop, .{
                        .fd = self.fd,
                        .buffer = "abcdefgh"[self.offset..][0..1],
                        .deadline = self.until,
                    });
                    self.pending = true;
                }

                const n = switch (self.send.poll(task)) {
                    .pending => return .pending,
                    .ready => |value| value catch |err| return .{ .ready = err },
                };
                self.pending = false;

                if (n != 1) return .{ .ready = error.UnexpectedZeroTransfer };

                self.offset += n;
            }

            return .{ .ready = {} };
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);

        const until = try support.deadline(3_000);
        var buffers: [8][1]u8 = undefined;
        var recvs: [8]co.Frame(io.Recv) = undefined;
        var sender: co.Frame(Sender) = .{
            .coroutine = .{
                .loop = &loop,
                .fd = fds[1],
                .timer = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 10_000_000 },
                }),
                .until = until,
            },
        };

        for (&recvs, &buffers) |*recv, *buffer| {
            recv.* = .{
                .coroutine = io.Recv.init(&loop, .{
                    .fd = fds[0],
                    .buffer = buffer,
                    .deadline = until,
                }),
            };

            loop.executor.spawn(&recv.task);
        }

        loop.executor.spawn(&sender.task);
        support.run(&loop);

        try sender.getResult();
        var seen: [8]bool = @splat(false);
        for (&recvs, &buffers) |*recv, *buffer| {
            try testing.expectEqual(1, try recv.getResult());
            try testing.expect(buffer[0] >= 'a' and buffer[0] <= 'h');
            const index = buffer[0] - 'a';
            try testing.expect(!seen[index]);
            seen[index] = true;

            recv.coroutine.deinit();

            try testing.expectEqual(0, recv.task.outstanding_io);
        }

        sender.coroutine.timer.deinit();
        sender.coroutine.send.deinit();

        try testing.expectEqual(0, sender.task.outstanding_io);
        try support.drained(&loop);
    }
}

test "portable io IPv6 accept transfers ownership of its socket" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(2, mode);
        defer loop.deinit();

        const fds = try support.pair(&loop, .ipv6);
        defer for (fds) |fd| support.close(fd);

        var address: io.SockAddr.in6 = undefined;
        var len: io.SockLen = @sizeOf(@TypeOf(address));

        try io.localAddress(fds[1], @ptrCast(&address), &len);

        try testing.expectEqual(@sizeOf(@TypeOf(address)), len);
        try testing.expect(address.port != 0);
        try testing.expectEqualSlices(
            u8,
            &.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
            &address.addr,
        );
        try support.drained(&loop);
    }
}
