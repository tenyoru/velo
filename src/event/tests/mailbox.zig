const std = @import("std");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const support = @import("support.zig");
const testing = std.testing;

fn cleanup(mailbox: anytype, loops: []const *io.Loop) void {
    mailbox.close();
    while (mailbox.tryRecv()) |_| {} else |_| {}
    for (loops) |loop| support.run(loop);
    mailbox.deinit();
}

fn drained(loop: *io.Loop) !void {
    try support.drained(loop);
    try testing.expectEqual(null, loop.control.registrations.first);
    try testing.expect(!loop.control.hasWork());
    try testing.expectEqual(null, loop.executor.all_tasks.first);
}

test "mailbox validates capacity, wraps FIFO storage and drains after close" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var mailbox: io.Mailbox(u32) = undefined;
        try testing.expectError(error.InvalidCapacity, mailbox.init(&loop, &.{}));
        try drained(&loop);
        var buffer: [2]u32 = undefined;
        try mailbox.init(&loop, &buffer);
        defer cleanup(&mailbox, &.{&loop});
        try testing.expect(loop.control.hasWork());
        try testing.expectError(error.Empty, mailbox.tryRecv());
        try mailbox.trySend(1);
        try mailbox.trySend(2);
        try testing.expectError(error.Full, mailbox.trySend(3));
        try testing.expectEqual(1, try mailbox.tryRecv());
        try mailbox.trySend(3);
        mailbox.close();
        mailbox.close();
        try testing.expectError(error.Closed, mailbox.trySend(4));
        var receive: co.Frame(io.Mailbox(u32).Recv) = .init(mailbox.recv());
        try testing.expectEqual(2, try loop.block(&receive));
        try testing.expectEqual(3, try mailbox.tryRecv());
        try testing.expectError(error.Closed, mailbox.tryRecv());
        receive = .init(mailbox.recv());
        try testing.expectError(error.Closed, loop.block(&receive));
        try drained(&loop);
    }
}

test "mailbox same-loop Join accounts for delayed sends and empty payloads" {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    const M = io.Mailbox(void);
    var buffer: [1]void = undefined;
    var mailbox: M = undefined;
    try mailbox.init(&loop, &buffer);
    defer cleanup(&mailbox, &.{&loop});
    try mailbox.trySend({});
    const Both = co.Join(.{ .send = M.Send, .recv = M.Recv });
    var frame: co.Frame(Both) = .init(.init(.{ .send = mailbox.send(&loop, {}), .recv = mailbox.recv() }));
    loop.spawn(&frame);
    loop.executor.run();
    try testing.expectEqual(null, frame.result);
    try testing.expectEqual(1, frame.task.outstanding_io);
    mailbox.close();
    support.run(&loop);
    const result = frame.getResult();
    try result.send;
    try result.recv;
    try mailbox.tryRecv();
    try testing.expectError(error.Closed, mailbox.tryRecv());
    try testing.expectEqual(0, frame.task.outstanding_io);
    try drained(&loop);
}

test "mailbox completed operations can be canceled after the mailbox and loop are gone" {
    const M = io.Mailbox(u32);
    var send: co.Frame(M.Send) = undefined;
    var recv: co.Frame(M.Recv) = undefined;
    {
        var loop = try io.Loop.init(.{});
        defer loop.deinit();
        var buffer: [1]u32 = undefined;
        var mailbox: M = undefined;
        try mailbox.init(&loop, &buffer);
        defer cleanup(&mailbox, &.{&loop});
        send = .init(mailbox.send(&loop, 42));
        loop.spawn(&send);
        loop.executor.run();
        try send.getResult();
        mailbox.close();
        recv = .init(mailbox.recv());
        try testing.expectEqual(42, try loop.block(&recv));
    }
    send.coroutine.cancel();
    recv.coroutine.cancel();
    try send.getResult();
    try testing.expectEqual(42, try recv.getResult());
}

test "mailbox close wakes empty receivers and initialization during shutdown closes immediately" {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    const M = io.Mailbox(u32);
    var buffer: [1]u32 = undefined;
    var mailbox: M = undefined;
    try mailbox.init(&loop, &buffer);
    var receive: co.Frame(M.Recv) = .init(mailbox.recv());
    loop.spawn(&receive);
    loop.executor.run();
    mailbox.close();
    support.run(&loop);
    defer mailbox.deinit();
    try testing.expectError(error.Closed, receive.getResult());
    try drained(&loop);

    loop.shutdown();
    var later: M = undefined;
    try later.init(&loop, &buffer);
    defer cleanup(&later, &.{&loop});
    try testing.expectError(error.Closed, later.trySend(1));
    receive = .init(later.recv());
    try testing.expectError(error.Canceled, loop.block(&receive));
    try drained(&loop);
}

test "mailbox backpressure is FIFO and successful sends drain their post before cancellation completes" {
    const Value = struct { id: u64 align(128), owned: *bool };
    const M = io.Mailbox(Value);
    var receiver = try io.Loop.init(.{});
    defer receiver.deinit();
    var a = try io.Loop.init(.{});
    defer a.deinit();
    var b = try io.Loop.init(.{});
    defer b.deinit();
    var buffer: [1]Value = undefined;
    var mailbox: M = undefined;
    try mailbox.init(&receiver, &buffer);
    defer cleanup(&mailbox, &.{ &receiver, &a, &b });
    var owned = true;
    try mailbox.trySend(.{ .id = 1, .owned = &owned });
    var first: co.Frame(M.Send) = .init(mailbox.send(&a, .{ .id = 2, .owned = &owned }));
    var second: co.Frame(M.Send) = .init(mailbox.send(&b, .{ .id = 3, .owned = &owned }));
    a.spawn(&first);
    b.spawn(&second);
    a.executor.run();
    b.executor.run();
    try testing.expectEqual(1, first.task.outstanding_io);
    try testing.expectEqual(1, second.task.outstanding_io);
    try testing.expectEqual(1, (try mailbox.tryRecv()).id);
    try testing.expectError(error.Full, mailbox.trySend(.{ .id = 4, .owned = &owned }));
    first.cancel();
    a.executor.run();
    try testing.expectEqual(null, first.result);
    try testing.expectEqual(1, first.task.outstanding_io);
    support.run(&a);
    try first.getResult();
    try testing.expectEqual(2, (try mailbox.tryRecv()).id);
    second.cancel();
    support.run(&b);
    try second.getResult();
    const last = try mailbox.tryRecv();
    try testing.expectEqual(3, last.id);
    try testing.expect(last.owned == &owned);
    try testing.expectEqual(0, mailbox.active_waits);
    try drained(&a);
    try drained(&b);
}

test "mailbox receive grants survive cancellation and waiting receivers stay FIFO" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        const M = io.Mailbox(u32);
        var buffer: [1]u32 = undefined;
        var mailbox: M = undefined;
        try mailbox.init(&loop, &buffer);
        defer cleanup(&mailbox, &.{&loop});
        var first: co.Frame(M.Recv) = .init(mailbox.recv());
        var canceled: co.Frame(M.Recv) = .init(mailbox.recv());
        var last: co.Frame(M.Recv) = .init(mailbox.recv());
        inline for (.{ &first, &canceled, &last }) |frame| loop.spawn(frame);
        loop.executor.run();
        canceled.cancel();
        canceled.cancel();
        try mailbox.trySend(7);
        try mailbox.trySend(9);
        first.cancel();
        loop.executor.run();
        inline for (.{ &first, &canceled, &last }) |frame| {
            try testing.expectEqual(null, frame.result);
            try testing.expectEqual(1, frame.task.outstanding_io);
        }
        mailbox.close();
        support.run(&loop);
        try testing.expectEqual(7, try first.getResult());
        try testing.expectError(error.Canceled, canceled.getResult());
        try testing.expectEqual(9, try last.getResult());
        try testing.expectError(error.Closed, mailbox.tryRecv());
        try testing.expectEqual(0, mailbox.active_waits);
        try drained(&loop);
    }
}

test "mailbox close and cancellation preserve uncommitted send values" {
    var receiver = try io.Loop.init(.{});
    defer receiver.deinit();
    var sender = try io.Loop.init(.{});
    defer sender.deinit();
    const M = io.Mailbox(u32);
    var buffer: [1]u32 = undefined;
    var mailbox: M = undefined;
    try mailbox.init(&receiver, &buffer);
    defer cleanup(&mailbox, &.{ &receiver, &sender });
    try mailbox.trySend(1);
    var canceled: co.Frame(M.Send) = .init(mailbox.send(&sender, 2));
    var closed: co.Frame(M.Send) = .init(mailbox.send(&sender, 3));
    sender.spawn(&canceled);
    sender.spawn(&closed);
    sender.executor.run();
    canceled.cancel();
    mailbox.close();
    support.run(&sender);
    support.run(&receiver);
    try testing.expectError(error.Canceled, canceled.getResult());
    try testing.expectError(error.Closed, closed.getResult());
    try testing.expectEqual(2, canceled.coroutine.value);
    try testing.expectEqual(3, closed.coroutine.value);
    try testing.expectEqual(1, try mailbox.tryRecv());
    try testing.expectError(error.Closed, mailbox.tryRecv());
    try drained(&sender);
    try drained(&receiver);
}

test "mailbox pre-poll cancellation touches no values and sender shutdown leaves the mailbox open" {
    var receiver = try io.Loop.init(.{});
    defer receiver.deinit();
    var sender = try io.Loop.init(.{});
    defer sender.deinit();
    const M = io.Mailbox(u32);
    var buffer: [1]u32 = undefined;
    var mailbox: M = undefined;
    try mailbox.init(&receiver, &buffer);
    defer cleanup(&mailbox, &.{ &receiver, &sender });
    var send: co.Frame(M.Send) = .init(mailbox.send(&sender, 7));
    var recv: co.Frame(M.Recv) = .init(mailbox.recv());
    send.cancel();
    recv.cancel();
    sender.spawn(&send);
    receiver.spawn(&recv);
    sender.executor.run();
    receiver.executor.run();
    try testing.expectError(error.Canceled, send.getResult());
    try testing.expectError(error.Canceled, recv.getResult());
    try testing.expectEqual(0, mailbox.active_waits);
    try mailbox.trySend(8);
    send = .init(mailbox.send(&sender, 9));
    sender.spawn(&send);
    sender.executor.run();
    sender.shutdown();
    support.run(&sender);
    try testing.expectError(error.Canceled, send.getResult());
    try testing.expectEqual(9, send.coroutine.value);
    try testing.expectEqual(8, try mailbox.tryRecv());
    try mailbox.trySend(10);
    try testing.expectEqual(10, try mailbox.tryRecv());
    send = .init(mailbox.send(&sender, 11));
    try testing.expectError(error.Canceled, sender.block(&send));
    try testing.expectError(error.Empty, mailbox.tryRecv());
    try drained(&sender);
}

test "mailbox timeout drains a pending notification before releasing the receive frame" {
    const M = io.Mailbox(u32);
    const Consumer = struct {
        mailbox: *M,
        child: io.WithTimeout(M.Recv),

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!u32) {
            const result = self.child.poll(task).value() orelse return .pending;
            self.mailbox.close();
            return .{ .ready = result };
        }
    };
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var buffer: [1]u32 = undefined;
        var mailbox: M = undefined;
        try mailbox.init(&loop, &buffer);
        defer cleanup(&mailbox, &.{&loop});
        var frame: co.Frame(Consumer) = .init(.{
            .mailbox = &mailbox,
            .child = .init(&loop, mailbox.recv(), .{ .duration = io.milliseconds(1) }),
        });
        try testing.expectError(error.Timeout, loop.block(&frame));
        try testing.expectEqual(0, mailbox.active_waits);
        try testing.expectEqual(0, frame.task.outstanding_io);
        try drained(&loop);
    }
}

test "mailbox ordinary producer threads preserve every value and per-producer ordering" {
    const producer_count = 4;
    const messages = 1000;
    const Message = struct { producer: usize, sequence: usize };
    const M = io.Mailbox(Message);
    const Producer = struct {
        mailbox: *M,
        remaining: *std.atomic.Value(usize),
        index: usize,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            defer if (self.remaining.fetchSub(1, .acq_rel) == 1) self.mailbox.close();
            var sequence: usize = 0;
            while (sequence < messages) {
                self.mailbox.trySend(.{ .producer = self.index, .sequence = sequence }) catch |err| switch (err) {
                    error.Full => {
                        std.Thread.yield() catch {};
                        continue;
                    },
                    error.Closed => {
                        self.failure = err;
                        return;
                    },
                };
                sequence += 1;
            }
        }
    };
    const Consumer = struct {
        const Self = @This();
        pub const Result = anyerror!void;
        pub const State = union(enum) { receive: M.Recv, pause: co.Yield };
        mailbox: *M,
        counts: [producer_count]usize = @splat(0),
        count: usize = 0,

        pub fn start(self: *Self) co.Next(Self) {
            return .{ .go = .{ .receive = self.mailbox.recv() } };
        }

        pub fn receive(self: *Self, result: M.Error!Message) co.Next(Self) {
            const message = result catch |err| {
                self.mailbox.close();
                return .{ .done = if (err == error.Closed) {} else err };
            };
            if (message.producer >= producer_count or message.sequence != self.counts[message.producer]) {
                self.mailbox.close();
                return .{ .done = error.OutOfOrder };
            }
            self.counts[message.producer] += 1;
            self.count += 1;
            return if (self.count % 64 == 0) .{ .go = .{ .pause = .{} } } else self.start();
        }

        pub fn pause(self: *Self, _: void) co.Next(Self) {
            return self.start();
        }
    };
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var buffer: [3]Message = undefined;
        var mailbox: M = undefined;
        try mailbox.init(&loop, &buffer);
        var remaining: std.atomic.Value(usize) = .init(producer_count);
        var producers: [producer_count]Producer = undefined;
        var threads: [producer_count]std.Thread = undefined;
        var started: usize = 0;
        defer {
            mailbox.close();
            for (threads[0..started]) |thread| thread.join();
            cleanup(&mailbox, &.{&loop});
        }
        for (&producers, &threads, 0..) |*producer, *thread, index| {
            producer.* = .{ .mailbox = &mailbox, .remaining = &remaining, .index = index };
            thread.* = try std.Thread.spawn(.{}, Producer.run, .{producer});
            started += 1;
        }
        var frame: co.Frame(io.WithTimeout(co.Machine(Consumer))) = .init(.init(&loop, .init(.{
            .mailbox = &mailbox,
        }), .{ .duration = io.seconds(10) }));
        try loop.block(&frame);
        for (threads) |thread| thread.join();
        started = 0;
        for (producers) |producer| try testing.expectEqual(null, producer.failure);
        const consumer = &frame.coroutine.child.ctx;
        try testing.expectEqual(producer_count * messages, consumer.count);
        for (consumer.counts) |count| try testing.expectEqual(messages, count);
        try testing.expectEqual(0, mailbox.active_waits);
        try drained(&loop);
    }
}

test "mailbox receiver shutdown closes admission, rejects waiters and keeps buffered values for cleanup" {
    for ([_]bool{ false, true }) |full| {
        var receiver = try io.Loop.init(.{});
        defer receiver.deinit();
        var sender = try io.Loop.init(.{});
        defer sender.deinit();
        const M = io.Mailbox(u32);
        var buffer: [1]u32 = undefined;
        var mailbox: M = undefined;
        try mailbox.init(&receiver, &buffer);
        defer cleanup(&mailbox, &.{ &receiver, &sender });
        var send: co.Frame(M.Send) = .init(mailbox.send(&sender, 2));
        var recv: co.Frame(M.Recv) = .init(mailbox.recv());
        if (full) {
            try mailbox.trySend(1);
            sender.spawn(&send);
            sender.executor.run();
        } else {
            receiver.spawn(&recv);
            receiver.executor.run();
        }
        receiver.shutdown();
        try testing.expectError(error.Closed, mailbox.trySend(3));
        support.run(&receiver);
        support.run(&sender);
        if (full) {
            try testing.expectError(error.Closed, send.getResult());
            try testing.expectEqual(2, send.coroutine.value);
            try testing.expectEqual(1, try mailbox.tryRecv());
        } else {
            try testing.expectError(error.Closed, recv.getResult());
        }
        try testing.expectError(error.Closed, mailbox.tryRecv());
        try testing.expectEqual(0, mailbox.active_waits);
        try drained(&receiver);
        try drained(&sender);
    }
}
