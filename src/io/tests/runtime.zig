const std = @import("std");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const support = @import("support.zig");
const testing = std.testing;

fn drained(loop: *io.Loop) !void {
    try support.drained(loop);
    try testing.expect(!loop.control.hasWork());
    try testing.expectEqual(null, loop.control.registrations.first);
    try testing.expectEqual(null, loop.executor.all_tasks.first);
}

test "runtime MPSC posts preserve producer order and coalesce queued duplicates" {
    const State = struct {
        loop: *io.Loop,
        thread: std.Thread.Id,
        next: [4]usize = @splat(0),
        seen: [4][16]bool = @splat(@splat(false)),
        count: usize = 0,
        bad: bool = false,
    };

    const Message = struct {
        post: io.Control.Post = .{ .callback = callback },
        state: *State,
        producer: usize,
        sequence: usize,

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            const s = self.state;
            s.bad = s.bad or std.Thread.getCurrentId() != s.thread or
                s.next[self.producer] != self.sequence or s.seen[self.producer][self.sequence];
            s.seen[self.producer][self.sequence] = true;
            s.next[self.producer] += 1;
            s.count += 1;
        }

        fn produce(messages: *[16]@This(), gate: *std.Io.Event) void {
            gate.waitUncancelable(testing.io);
            const loop = messages[0].state.loop;

            for (messages) |*message| loop.post(&message.post);

            loop.release();
        }
    };

    const Nested = struct {
        post: io.Control.Post = .{ .callback = callback },
        loop: *io.Loop,
        next: ?*@This() = null,
        thread: std.Thread.Id,
        calls: usize = 0,
        wrong_thread: bool = false,

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.calls += 1;
            self.wrong_thread = std.Thread.getCurrentId() != self.thread;
            if (self.next) |next| self.loop.post(&next.post);
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var state: State = .{ .loop = &loop, .thread = std.Thread.getCurrentId() };
        var messages: [4][16]Message = undefined;
        var gate: std.Io.Event = .unset;
        var threads: [4]std.Thread = undefined;

        for (&messages, &threads, 0..) |*batch, *thread, producer| {
            for (batch, 0..) |*message, sequence| message.* = .{
                .state = &state,
                .producer = producer,
                .sequence = sequence,
            };

            loop.retain();
            thread.* = std.Thread.spawn(
                .{},
                Message.produce,
                .{ batch, &gate },
            ) catch @panic("producer spawn failed");
        }

        var second: Nested = .{ .loop = &loop, .thread = state.thread };
        var first: Nested = .{ .loop = &loop, .thread = state.thread, .next = &second };

        loop.post(&first.post);
        loop.post(&first.post);
        gate.set(testing.io);
        support.run(&loop);
        for (threads) |thread| thread.join();

        try testing.expect(!state.bad);
        try testing.expectEqual(64, state.count);
        for (state.next, state.seen) |next, seen| {
            try testing.expectEqual(16, next);
            for (seen) |value| try testing.expect(value);
        }
        try testing.expectEqual(1, first.calls);
        try testing.expectEqual(1, second.calls);
        try testing.expect(!first.wrong_thread and !second.wrong_thread);
        try drained(&loop);
    }
}

const SignalTimer = struct {
    timer: io.Timeout,
    event: *std.Io.Event,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
        const result = switch (self.timer.poll(task)) {
            .pending => return .pending,
            .ready => |result| result,
        };
        self.event.set(testing.io);
        return .{ .ready = result };
    }
};

test "runtime external stop preserves live timer and shutdown drains task registry" {
    const Stopper = struct {
        fn run(loop: *io.Loop, gate: *std.Io.Event) void {
            gate.waitUncancelable(testing.io);
            loop.stop();
            loop.release();
        }
    };

    const Wait = struct {
        child: co.Event.Wait,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(error{Canceled}!void) {
            return self.child.poll(task);
        }
    };

    const Timer = struct {
        child: io.Timeout,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
            return self.child.poll(task);
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var event: co.Event = .{};
        defer event.deinit();
        var waiter: co.Frame(Wait) = .{ .coroutine = .{ .child = event.wait() } };
        var timer: co.Frame(Timer) = .{
            .coroutine = .{
                .child = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 60, .nsec = 0 },
                }),
            },
        };
        var gate: std.Io.Event = .unset;
        var signal: co.Frame(SignalTimer) = .{
            .coroutine = .{
                .timer = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 1_000_000 },
                }),
                .event = &gate,
            },
        };

        loop.executor.spawn(&waiter.task);
        loop.executor.spawn(&timer.task);
        loop.executor.spawn(&signal.task);
        loop.retain();
        const thread = std.Thread.spawn(
            .{},
            Stopper.run,
            .{ &loop, &gate },
        ) catch @panic("stopper spawn failed");
        support.run(&loop);
        thread.join();

        try signal.getResult();

        signal.coroutine.timer.deinit();

        try testing.expectEqual(null, timer.result);
        try testing.expectEqual(null, waiter.result);
        try testing.expectEqual(.inflight, timer.coroutine.child.operation.state);
        try testing.expectEqual(1, loop.active);
        try testing.expectEqual(1, timer.task.outstanding_io);
        try testing.expectEqual(1, waiter.task.outstanding_io);
        try testing.expectEqual(1, loop.control.registrations.len());
        try testing.expect(!timer.task.cancel_requested and !waiter.task.cancel_requested);
        try testing.expectEqual(null, waiter.task.cancel_fn);
        try testing.expectEqual(null, timer.task.cancel_fn);

        loop.shutdown();
        support.run(&loop);

        try testing.expectError(error.OperationCanceled, timer.getResult());
        try testing.expectError(error.Canceled, waiter.getResult());

        timer.coroutine.child.deinit();

        try testing.expectEqual(0, timer.task.outstanding_io);
        try testing.expectEqual(0, waiter.task.outstanding_io);

        var rejected: co.Frame(io.Timeout) = .{
            .coroutine = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 1 },
            }),
        };

        loop.executor.spawn(&rejected.task);
        support.run(&loop);

        try testing.expectError(error.OperationCanceled, rejected.getResult());

        rejected.coroutine.deinit();

        try testing.expect(loop.control.isShuttingDown());
        try drained(&loop);
    }
}

test "runtime shutdown from the last runnable task cancels parked tasks before idle exit" {
    const Shutter = struct {
        loop: *io.Loop,

        pub fn poll(self: *@This(), _: *co.Task) co.Poll(void) {
            self.loop.shutdown();
            return .{ .ready = {} };
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var event: co.Event = .{};
        defer event.deinit();
        var waiter: co.Frame(co.Event.Wait) = .{ .coroutine = event.wait() };
        var shutter: co.Frame(Shutter) = .{ .coroutine = .{ .loop = &loop } };

        loop.executor.spawn(&waiter.task);
        loop.executor.spawn(&shutter.task);
        support.run(&loop);

        try testing.expectError(error.Canceled, waiter.getResult());
        try drained(&loop);
    }
}

test "runtime shutdown resumes cancellation for tasks spawned by later posts" {
    const Driver = struct {
        loop: *io.Loop,
        waiters: *[2]co.Frame(co.Event.Wait),
        post: io.Control.Post = .{ .callback = callback },
        task: ?*co.Task = null,
        posted: bool = false,
        calls: usize = 0,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            self.task = task;
            if (self.calls == self.waiters.len) return .{ .ready = {} };
            if (task.cancel_requested and !self.posted) {
                self.posted = true;
                self.loop.post(&self.post);
                self.loop.stop();
            }
            return .pending;
        }

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.loop.executor.spawn(&self.waiters[self.calls].task);
            self.calls += 1;
            if (self.calls < self.waiters.len) {
                self.loop.post(&self.post);
            } else {
                self.task.?.wake();
            }
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var event: co.Event = .{};
        defer event.deinit();
        var waiters: [2]co.Frame(co.Event.Wait) = @splat(.{ .coroutine = event.wait() });
        var driver: co.Frame(Driver) = .{ .coroutine = .{ .loop = &loop, .waiters = &waiters } };

        loop.executor.spawn(&driver.task);
        loop.executor.run();
        loop.shutdown();
        support.run(&loop);

        try testing.expectEqual(1, driver.coroutine.calls);
        try testing.expect(driver.result == null and waiters[0].result == null);
        try testing.expect(waiters[0].task.cancel_requested);
        try testing.expect(waiters[0].task.registered and driver.task.registered);
        try testing.expect(!waiters[1].task.registered);

        support.run(&loop);

        driver.getResult();
        try testing.expectEqual(2, driver.coroutine.calls);
        for (&waiters) |*waiter| try testing.expectError(error.Canceled, waiter.getResult());
        try drained(&loop);
    }
}

test "runtime retained idle loop wakes for a post and exits after external release" {
    const Driver = struct {
        loop: *io.Loop,
        post: io.Control.Post = .{ .callback = callback },
        observed: std.Io.Event = .unset,
        returned: std.atomic.Value(bool) = .init(false),
        thread: std.Thread.Id = undefined,
        callback_thread: std.Thread.Id = undefined,

        fn run(self: *@This()) void {
            self.thread = std.Thread.getCurrentId();
            support.run(self.loop);
            self.returned.store(true, .release);
        }

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.callback_thread = std.Thread.getCurrentId();
            self.observed.set(testing.io);
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var driver: Driver = .{ .loop = &loop };

        loop.retain();
        const thread = std.Thread.spawn(
            .{},
            Driver.run,
            .{&driver},
        ) catch @panic("loop thread spawn failed");
        loop.post(&driver.post);
        driver.observed.waitUncancelable(testing.io);
        const returned_early = driver.returned.load(.acquire);
        loop.release();
        thread.join();

        try testing.expect(!returned_early);
        try testing.expectEqual(driver.thread, driver.callback_thread);
        try testing.expect(driver.thread != std.Thread.getCurrentId());
        try testing.expect(driver.returned.load(.acquire));
        try drained(&loop);
    }
}

test "runtime independent loop threads exchange bounded posts" {
    const Peer = struct {
        loop: *io.Loop,
        other: *@This() = undefined,
        post: io.Control.Post = .{ .callback = callback },
        thread: std.Thread.Id = undefined,
        calls: usize = 0,
        wrong_thread: bool = false,
        first: bool,

        fn run(self: *@This()) void {
            self.thread = std.Thread.getCurrentId();
            support.run(self.loop);
        }

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.wrong_thread = self.wrong_thread or std.Thread.getCurrentId() != self.thread;
            self.calls += 1;
            if (self.first or self.calls < 16) self.other.loop.post(&self.other.post);
            if (self.calls == 16) self.loop.release();
        }
    };

    for (support.modes) |mode| {
        var a = try support.initLoop(8, mode);
        defer a.deinit();
        var b = try support.initLoop(8, mode);
        defer b.deinit();

        var left: Peer = .{ .loop = &a, .first = true };
        var right: Peer = .{ .loop = &b, .first = false };
        left.other = &right;
        right.other = &left;

        a.retain();
        b.retain();
        const t1 = std.Thread.spawn(
            .{},
            Peer.run,
            .{&left},
        ) catch @panic("first loop spawn failed");
        const t2 = std.Thread.spawn(
            .{},
            Peer.run,
            .{&right},
        ) catch @panic("second loop spawn failed");
        a.post(&left.post);
        t1.join();
        t2.join();

        try testing.expectEqual(16, left.calls);
        try testing.expectEqual(16, right.calls);
        try testing.expect(!left.wrong_thread and !right.wrong_thread);
        try testing.expect(left.thread != right.thread);
        try drained(&a);
        try drained(&b);
    }
}

test "runtime datagrams preserve IPv4 IPv6 peers empty packets and truncation errors" {
    for (support.modes) |mode| {
        inline for (.{ io.AddressFamily.ipv4, io.AddressFamily.ipv6 }) |family| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();

            const A = if (family == .ipv4) io.SockAddr.in else io.SockAddr.in6;
            const a = try io.udpSocket(family);
            defer support.close(a);
            const b = try io.udpSocket(family);
            defer support.close(b);

            var local_a: A = if (family == .ipv4)
                .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) }
            else
                .{
                    .port = 0,
                    .flowinfo = 0,
                    .addr = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
                    .scope_id = 0,
                };
            var local_b = local_a;
            var len_a: io.SockLen = @sizeOf(A);
            var len_b: io.SockLen = @sizeOf(A);

            try io.bind(a, @ptrCast(&local_a), len_a);
            try io.bind(b, @ptrCast(&local_b), len_b);
            try io.localAddress(a, @ptrCast(&local_a), &len_a);
            try io.localAddress(b, @ptrCast(&local_b), &len_b);

            var peer: A = undefined;
            var peer_len: io.SockLen = @sizeOf(A);
            var buffer: [32]u8 = undefined;
            const until = try support.deadline(3_000);
            var receive: co.Frame(io.RecvFrom) = .{
                .coroutine = io.RecvFrom.init(&loop, .{
                    .fd = b,
                    .buffer = &buffer,
                    .address = @ptrCast(&peer),
                    .address_len = &peer_len,
                    .deadline = until,
                }),
            };
            var send: co.Frame(io.SendTo) = .{
                .coroutine = io.SendTo.init(&loop, .{
                    .fd = a,
                    .buffer = "hello",
                    .address = @ptrCast(&local_b),
                    .address_len = len_b,
                    .deadline = until,
                }),
            };

            loop.executor.spawn(&receive.task);
            loop.executor.spawn(&send.task);
            support.run(&loop);

            try testing.expectEqual(5, try send.getResult());
            try testing.expectEqual(5, try receive.getResult());
            try testing.expectEqualStrings("hello", buffer[0..5]);
            try testing.expectEqual(@sizeOf(A), peer_len);
            try testing.expectEqual(local_a.port, peer.port);
            try testing.expectEqual(local_a.family, peer.family);
            try testing.expectEqualDeep(local_a.addr, peer.addr);

            receive.coroutine.deinit();
            send.coroutine.deinit();

            // Send first so recvmsg can complete immediately with MSG_TRUNC set.
            for ([_][]const u8{ "reply", "", "too large" }, 0..) |payload, index| {
                send = .{
                    .coroutine = io.SendTo.init(&loop, .{
                        .fd = b,
                        .buffer = payload,
                        .address = @ptrCast(&peer),
                        .address_len = peer_len,
                        .deadline = until,
                    }),
                };

                loop.executor.spawn(&send.task);
                support.run(&loop);

                try testing.expectEqual(payload.len, try send.getResult());

                send.coroutine.deinit();

                var reply_peer: A = undefined;
                var reply_len: io.SockLen = @sizeOf(A);
                receive = .{
                    .coroutine = io.RecvFrom.init(&loop, .{
                        .fd = a,
                        .buffer = buffer[0..if (index == 2) 1 else buffer.len],
                        .address = @ptrCast(&reply_peer),
                        .address_len = &reply_len,
                        .deadline = until,
                    }),
                };

                loop.executor.spawn(&receive.task);
                support.run(&loop);

                if (index == 2) {
                    try testing.expectError(error.MessageTooBig, receive.getResult());
                } else {
                    try testing.expectEqual(payload.len, try receive.getResult());
                    try testing.expectEqualSlices(u8, payload, buffer[0..payload.len]);
                    try testing.expectEqual(local_b.port, reply_peer.port);
                    try testing.expectEqualDeep(local_b.addr, reply_peer.addr);
                    try testing.expectEqual(@sizeOf(A), reply_len);
                }

                receive.coroutine.deinit();

                try testing.expectEqual(0, receive.task.outstanding_io);
                try testing.expectEqual(0, send.task.outstanding_io);
                try drained(&loop);
            }
        }
    }
}

test "runtime pending datagram receive cancels and shutdown rejects new receives" {
    const Cancel = struct {
        timer: io.Timeout,
        receive: *co.Frame(io.RecvFrom),
        loop: *io.Loop,
        shutdown: bool,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
            const result = switch (self.timer.poll(task)) {
                .pending => return .pending,
                .ready => |result| result,
            };
            if (self.shutdown) self.loop.shutdown() else self.receive.cancel();
            return .{ .ready = result };
        }
    };

    for (support.modes) |mode| {
        for ([_]bool{ false, true }) |shutdown| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();

            const fd = try io.udpSocket(.ipv4);
            defer support.close(fd);
            var address: io.SockAddr.in = .{
                .port = 0,
                .addr = std.mem.nativeToBig(u32, 0x7f000001),
            };

            try io.bind(fd, @ptrCast(&address), @sizeOf(@TypeOf(address)));

            var len: io.SockLen = @sizeOf(@TypeOf(address));
            var buffer: [8]u8 = undefined;
            var receive: co.Frame(io.RecvFrom) = .{
                .coroutine = io.RecvFrom.init(&loop, .{
                    .fd = fd,
                    .buffer = &buffer,
                    .address = @ptrCast(&address),
                    .address_len = &len,
                }),
            };
            var cancel: co.Frame(Cancel) = .{
                .coroutine = .{
                    .timer = io.Timeout.init(&loop, .{
                        .duration = .{ .sec = 0, .nsec = 1_000_000 },
                    }),
                    .receive = &receive,
                    .loop = &loop,
                    .shutdown = shutdown,
                },
            };

            loop.executor.spawn(&receive.task);
            loop.executor.spawn(&cancel.task);
            support.run(&loop);

            try cancel.getResult();
            try testing.expectError(error.OperationCanceled, receive.getResult());

            receive.coroutine.deinit();
            cancel.coroutine.timer.deinit();

            try testing.expectEqual(0, receive.task.outstanding_io);

            if (shutdown) {
                receive = .{
                    .coroutine = io.RecvFrom.init(&loop, .{
                        .fd = fd,
                        .buffer = &buffer,
                        .address = @ptrCast(&address),
                        .address_len = &len,
                    }),
                };

                loop.executor.spawn(&receive.task);
                support.run(&loop);

                try testing.expectError(error.OperationCanceled, receive.getResult());

                receive.coroutine.deinit();
            }

            try drained(&loop);
        }
    }
}

test "runtime stream helpers loop preserve partial EOF and allow reverse half-close traffic" {
    const CancelAfterProgress = struct {
        read: io.ReadExact,

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
        var send: co.Frame(io.SendAll) = .{
            .coroutine = io.SendAll.init(&loop, .{
                .fd = fds[0],
                .buffer = "",
            }),
        };

        send.coroutine.cancel();
        loop.executor.spawn(&send.task);
        support.run(&loop);

        try testing.expectError(error.OperationCanceled, send.getResult());

        send = .{
            .coroutine = io.SendAll.init(&loop, .{
                .fd = fds[0],
                .buffer = sent,
                .deadline = until,
            }),
        };
        var read: co.Frame(io.ReadExact) = .{
            .coroutine = io.ReadExact.init(&loop, .{
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
            .coroutine = io.SendAll.init(&loop, .{
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
            .coroutine = io.ReadExact.init(&loop, .{
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
            .coroutine = io.SendAll.init(&loop, .{
                .fd = fds[1],
                .buffer = "back",
                .deadline = until,
            }),
        };
        read = .{
            .coroutine = io.ReadExact.init(&loop, .{
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
            .coroutine = io.SendAll.init(&loop, .{
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
                .read = io.ReadExact.init(&loop, .{
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
        try drained(&loop);
    }
}
