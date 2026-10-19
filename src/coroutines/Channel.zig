//! FIFO with caller-owned ring storage; zero capacity is a rendezvous.
//! Single-shot sends transfer ownership only on success; otherwise `Send.value` stays caller-owned.
//! Granted receives survive cancellation. `close()` rejects uncommitted sends, not buffered values.
//! Drain buffered values and consume all wait results before `deinit()`.

const std = @import("std");
const co = @import("root.zig");
const states = @import("../state.zig");
const assert = std.debug.assert;

pub fn Channel(comptime T: type) type {
    return struct {
        buffer: []T,
        head: usize = 0,
        tail: usize = 0,
        count: usize = 0,
        closed: bool = false,
        senders: std.DoublyLinkedList = .{},
        receivers: std.DoublyLinkedList = .{},

        const Self = @This();
        pub const Error = error{ Canceled, Closed };

        pub fn init(buffer: []T) Self {
            return .{ .buffer = buffer };
        }

        pub fn send(self: *Self, value: T) Send {
            return .{ .channel = self, .value = value };
        }

        pub fn recv(self: *Self) Recv {
            return .{ .channel = self };
        }

        pub fn close(self: *Self) void {
            self.closed = true;

            while (self.senders.popFirst()) |node| {
                const sender: *Send = @alignCast(@fieldParentPtr("node", node));

                sender.state = .closed;
                sender.task.?.wake();
            }

            while (self.receivers.popFirst()) |node| {
                assert(self.count == 0);
                const receiver: *Recv = @alignCast(@fieldParentPtr("node", node));

                receiver.state = .closed;
                receiver.task.?.wake();
            }
        }

        pub fn deinit(self: *Self) void {
            assert(self.senders.first == null and self.receivers.first == null);
            assert(self.count == 0);
        }

        fn push(self: *Self, value: T) void {
            assert(self.count < self.buffer.len);

            self.buffer[self.tail] = value;
            self.tail = (self.tail + 1) % self.buffer.len;
            self.count += 1;
        }

        pub const Send = struct {
            channel: *Self,
            value: T,
            node: std.DoublyLinkedList.Node = .{},
            task: ?*co.Task = null,
            state: states.Channel = .idle,

            pub fn poll(self: *Send, task: *co.Task) co.Poll(Error!void) {
                if (self.task) |borrowed| assert(borrowed == task);
                if (task.cancel_requested) self.cancel();

                switch (self.state) {
                    .idle => {
                        const channel = self.channel;

                        if (channel.closed) {
                            self.state = .closed;
                        } else if (channel.receivers.popFirst()) |node| {
                            assert(channel.count == 0);
                            const receiver: *Recv = @alignCast(@fieldParentPtr("node", node));

                            receiver.value = self.value;
                            receiver.state = .granted;
                            receiver.task.?.wake();
                            self.state = .granted;
                        } else if (channel.count < channel.buffer.len) {
                            channel.push(self.value);
                            self.state = .granted;
                        } else {
                            self.task = task;
                            task.outstanding_io += 1;
                            channel.senders.append(&self.node);
                            self.state = .waiting;
                            return .pending;
                        }
                    },
                    .waiting => return .pending,
                    .granted, .canceled, .closed => {},
                    .done => unreachable,
                }

                if (self.task != null) task.outstanding_io -= 1;

                const state = self.state;
                self.state = .done;

                return .{ .ready = switch (state) {
                    .granted => {},
                    .canceled => error.Canceled,
                    .closed => error.Closed,
                    else => unreachable,
                } };
            }

            pub fn cancel(self: *Send) void {
                switch (self.state) {
                    .idle => self.state = .canceled,
                    .waiting => {
                        self.channel.senders.remove(&self.node);
                        self.state = .canceled;
                        self.task.?.wake();
                    },
                    .granted, .canceled, .closed, .done => {},
                }
            }
        };

        pub const Recv = struct {
            channel: *Self,
            value: T = undefined,
            node: std.DoublyLinkedList.Node = .{},
            task: ?*co.Task = null,
            state: states.Channel = .idle,

            pub fn poll(self: *Recv, task: *co.Task) co.Poll(Error!T) {
                if (self.task) |borrowed| assert(borrowed == task);
                if (task.cancel_requested) self.cancel();

                switch (self.state) {
                    .idle => {
                        const channel = self.channel;

                        if (channel.count != 0) {
                            self.value = channel.buffer[channel.head];
                            channel.head = (channel.head + 1) % channel.buffer.len;
                            channel.count -= 1;
                            self.state = .granted;

                            if (channel.senders.popFirst()) |node| {
                                assert(!channel.closed);
                                const sender: *Send = @alignCast(@fieldParentPtr("node", node));

                                channel.push(sender.value);
                                sender.state = .granted;
                                sender.task.?.wake();
                            }
                        } else if (channel.closed) {
                            self.state = .closed;
                        } else if (channel.senders.popFirst()) |node| {
                            const sender: *Send = @alignCast(@fieldParentPtr("node", node));

                            self.value = sender.value;
                            self.state = .granted;
                            sender.state = .granted;
                            sender.task.?.wake();
                        } else {
                            self.task = task;
                            task.outstanding_io += 1;
                            channel.receivers.append(&self.node);
                            self.state = .waiting;
                            return .pending;
                        }
                    },
                    .waiting => return .pending,
                    .granted, .canceled, .closed => {},
                    .done => unreachable,
                }

                if (self.task != null) task.outstanding_io -= 1;

                const state = self.state;
                self.state = .done;

                return .{ .ready = switch (state) {
                    .granted => self.value,
                    .canceled => error.Canceled,
                    .closed => error.Closed,
                    else => unreachable,
                } };
            }

            pub fn cancel(self: *Recv) void {
                switch (self.state) {
                    .idle => self.state = .canceled,
                    .waiting => {
                        self.channel.receivers.remove(&self.node);
                        self.state = .canceled;
                        self.task.?.wake();
                    },
                    .granted, .canceled, .closed, .done => {},
                }
            }
        };
    };
}

test "channel buffers FIFO across wrap and grants blocked senders without barging" {
    const Ch = Channel(u32);
    var buffer: [2]u32 = undefined;
    var channel = Ch.init(&buffer);
    defer channel.deinit();

    var ex: co.Executor = .{};
    var sends: [5]co.Frame(Ch.Send) = undefined;

    for (&sends, 1..) |*frame, value| {
        frame.* = .{ .coroutine = channel.send(@intCast(value)) };
    }

    for (sends[0..4]) |*frame| ex.spawn(&frame.task);
    ex.run();

    try sends[0].getResult();
    try sends[1].getResult();
    try std.testing.expectEqual(2, channel.count);
    try std.testing.expectEqual(2, channel.senders.len());

    var first: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

    ex.spawn(&first.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(1, try first.getResult());
    try std.testing.expectEqual(1, sends[2].task.outstanding_io);

    sends[2].cancel();
    ex.spawn(&sends[4].task);
    ex.run();

    try sends[2].getResult();
    try std.testing.expectEqual(null, sends[3].result);
    try std.testing.expectEqual(null, sends[4].result);

    var receives: [4]co.Frame(Ch.Recv) = undefined;

    for (&receives) |*frame| {
        frame.* = .{ .coroutine = channel.recv() };
        ex.spawn(&frame.task);
    }
    ex.run();

    for (&receives, 2..) |*frame, expected| {
        try std.testing.expectEqual(expected, try frame.getResult());
    }

    for (&sends) |*frame| {
        try frame.getResult();
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
    try std.testing.expectEqual(0, channel.count);
}

test "channel receiver FIFO preserves high-alignment resources through late cancellation" {
    const Value = struct { id: u128, owned: *bool };
    const Ch = Channel(Value);
    var buffer: [1]Value = undefined;
    var channel = Ch.init(&buffer);
    defer channel.deinit();

    var ex: co.Executor = .{};
    var receives: [3]co.Frame(Ch.Recv) = undefined;

    for (&receives) |*frame| {
        frame.* = .{ .coroutine = channel.recv() };
        ex.spawn(&frame.task);
    }
    ex.run();

    receives[1].cancel();

    try std.testing.expectEqual(2, channel.receivers.len());

    var a_owned = true;
    var b_owned = true;
    var a: co.Frame(Ch.Send) = .{
        .coroutine = channel.send(.{
            .id = 1 << 100,
            .owned = &a_owned,
        }),
    };
    var b: co.Frame(Ch.Send) = .{
        .coroutine = channel.send(.{
            .id = 2 << 100,
            .owned = &b_owned,
        }),
    };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expectError(error.Canceled, receives[1].getResult());

    try std.testing.expect(ex.runOne());
    try std.testing.expectEqual(1, receives[0].task.outstanding_io);

    receives[0].cancel();
    ex.run();

    try a.getResult();
    try b.getResult();
    const av = try receives[0].getResult();
    const bv = try receives[2].getResult();

    try std.testing.expectEqual(@as(u128, 1 << 100), av.id);
    try std.testing.expectEqual(@as(u128, 2 << 100), bv.id);
    try std.testing.expectEqual(&a_owned, av.owned);
    try std.testing.expectEqual(&b_owned, bv.owned);
    try std.testing.expect(av.owned.* and bv.owned.*);

    av.owned.* = false;
    bv.owned.* = false;
    for (&receives) |*frame| try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "channel close rejects queued sends without losing values and drains buffered resources" {
    const Ch = Channel(*bool);
    var buffer: [2]*bool = undefined;
    var channel = Ch.init(&buffer);
    defer channel.deinit();

    var ex: co.Executor = .{};
    var owned = [_]bool{ true, true, true, true };
    var sends: [4]co.Frame(Ch.Send) = undefined;

    for (&sends, &owned) |*frame, *value| {
        frame.* = .{ .coroutine = channel.send(value) };
        ex.spawn(&frame.task);
    }
    ex.run();

    try std.testing.expectEqual(2, channel.senders.len());

    channel.close();
    channel.close();
    sends[2].cancel();

    try std.testing.expectEqual(null, channel.senders.first);
    try std.testing.expectEqual(1, sends[2].task.outstanding_io);

    ex.run();

    for (sends[2..], owned[2..]) |*frame, value| {
        try std.testing.expectError(error.Closed, frame.getResult());
        try std.testing.expect(value and frame.coroutine.value.*);
        frame.coroutine.value.* = false;
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }

    var rejected: co.Frame(Ch.Send) = .{ .coroutine = channel.send(&owned[2]) };

    ex.spawn(&rejected.task);
    ex.run();

    try std.testing.expectError(error.Closed, rejected.getResult());

    var receives: [3]co.Frame(Ch.Recv) = undefined;

    for (&receives) |*frame| {
        frame.* = .{ .coroutine = channel.recv() };
        ex.spawn(&frame.task);
    }
    ex.run();

    for (receives[0..2], owned[0..2], 0..) |*frame, value, i| {
        const resource = try frame.getResult();

        try std.testing.expectEqual(&owned[i], resource);
        try std.testing.expect(value and resource.*);
        resource.* = false;
    }

    try std.testing.expectError(error.Closed, receives[2].getResult());
    try std.testing.expectEqualSlices(bool, &.{ false, false, false, false }, &owned);
}

test "channel cancellation unlinks pending sends and receives before waking" {
    const Ch = Channel(u8);
    var buffer: [1]u8 = undefined;
    var channel = Ch.init(&buffer);
    defer channel.deinit();

    var ex: co.Executor = .{};
    var waiting: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

    ex.spawn(&waiting.task);
    ex.run();

    waiting.cancel();
    waiting.cancel();

    try std.testing.expectEqual(null, channel.receivers.first);
    try std.testing.expectEqual(1, waiting.task.outstanding_io);

    var fill: co.Frame(Ch.Send) = .{ .coroutine = channel.send(7) };
    var canceled: co.Frame(Ch.Send) = .{ .coroutine = channel.send(8) };
    var next: co.Frame(Ch.Send) = .{ .coroutine = channel.send(9) };

    ex.spawn(&fill.task);
    ex.spawn(&canceled.task);
    ex.spawn(&next.task);
    ex.run();

    try std.testing.expectError(error.Canceled, waiting.getResult());
    try std.testing.expectEqual(0, waiting.task.outstanding_io);

    canceled.cancel();

    try std.testing.expectEqual(1, channel.senders.len());
    try std.testing.expectEqual(1, canceled.task.outstanding_io);

    var a: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };
    var b: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.run();

    try std.testing.expectError(error.Canceled, canceled.getResult());
    try std.testing.expectEqual(8, canceled.coroutine.value);
    try std.testing.expectEqual(0, canceled.task.outstanding_io);
    try next.getResult();
    try std.testing.expectEqual(7, try a.getResult());
    try std.testing.expectEqual(9, try b.getResult());
}

test "channel pre-cancel does not consume available values or buffer space" {
    const Ch = Channel(?u8);
    var buffer: [1]?u8 = undefined;
    var channel = Ch.init(&buffer);
    defer channel.deinit();

    var ex: co.Executor = .{};
    var send: co.Frame(Ch.Send) = .{ .coroutine = channel.send(42) };

    send.cancel();
    ex.spawn(&send.task);
    ex.run();

    try std.testing.expectError(error.Canceled, send.getResult());
    try std.testing.expectEqual(0, channel.count);

    var fill: co.Frame(Ch.Send) = .{ .coroutine = channel.send(null) };

    ex.spawn(&fill.task);
    ex.run();

    try fill.getResult();

    var canceled: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

    canceled.cancel();
    ex.spawn(&canceled.task);
    ex.run();

    try std.testing.expectError(error.Canceled, canceled.getResult());
    try std.testing.expectEqual(1, channel.count);

    var recv: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

    ex.spawn(&recv.task);
    ex.run();

    try std.testing.expectEqual(null, try recv.getResult());
    try std.testing.expectEqual(0, send.task.outstanding_io);
    try std.testing.expectEqual(0, canceled.task.outstanding_io);
}

test "channel rendezvous supports either arrival order and close wakes empty receivers" {
    const Ch = Channel(u128);

    inline for (.{ false, true }) |sender_first| {
        var channel = Ch.init(&.{});
        defer channel.deinit();

        var ex: co.Executor = .{};
        var send: co.Frame(Ch.Send) = .{ .coroutine = channel.send(1 << 100) };
        var recv: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

        ex.spawn(if (sender_first) &send.task else &recv.task);
        ex.run();

        ex.spawn(if (sender_first) &recv.task else &send.task);
        ex.run();

        try send.getResult();
        try std.testing.expectEqual(@as(u128, 1 << 100), try recv.getResult());
        try std.testing.expectEqual(0, send.task.outstanding_io);
        try std.testing.expectEqual(0, recv.task.outstanding_io);

        var closed: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

        ex.spawn(&closed.task);
        ex.run();

        channel.close();

        try std.testing.expectEqual(null, channel.receivers.first);
        try std.testing.expectEqual(1, closed.task.outstanding_io);

        ex.run();

        try std.testing.expectError(error.Closed, closed.getResult());
        try std.testing.expectEqual(0, closed.task.outstanding_io);

        var immediate: co.Frame(Ch.Recv) = .{ .coroutine = channel.recv() };

        ex.spawn(&immediate.task);
        ex.run();

        try std.testing.expectError(error.Closed, immediate.getResult());
    }
}
