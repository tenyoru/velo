const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.io;
const Channel = co.Channel(u32);

// Returning .pending unwinds poll(), so its next call needs a stored resume state.
const State = enum { transfer, yield };

const Producer = struct {
    send: Channel.Send,
    next: u32 = 1,
    yield: co.Yield = .{},
    state: State = .transfer,
    canceled: bool = false,

    // Cancellation requests a stop; subsequent polls still consume the active child's result.
    pub fn cancel(self: *@This()) void {
        self.canceled = true;
        self.send.cancel();
        self.yield.cancel();
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(Channel.Error!void) {
        sw: switch (self.state) {
            .transfer => {
                // A full channel parks the send; a receiver wakes its task when space opens.
                switch (self.send.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result catch |err| {
                        self.send.channel.close();
                        return .{ .ready = err };
                    },
                }

                self.next += 1;

                if (self.next == 101) {
                    // Signal end-of-input without discarding values the consumer has not read.
                    self.send.channel.close();
                    return .{ .ready = {} };
                }

                self.state = .yield;
                continue :sw .yield;
            },
            .yield => {
                // Yield even after immediate sends, giving other work a turn.
                switch (self.yield.poll(task)) {
                    .pending => return .pending,
                    .ready => {},
                }

                if (self.canceled or task.cancel_requested) {
                    self.send.channel.close();
                    return .{ .ready = error.Canceled };
                }

                self.send = self.send.channel.send(self.next);
                self.state = .transfer;
                continue :sw .transfer;
            },
        }
    }
};

const Consumer = struct {
    recv: Channel.Recv,
    sum: u32 = 0,
    yield: co.Yield = .{},
    state: State = .transfer,
    canceled: bool = false,

    pub fn cancel(self: *@This()) void {
        self.canceled = true;
        self.recv.cancel();
        self.yield.cancel();
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(Channel.Error!u32) {
        sw: switch (self.state) {
            .transfer => {
                // Closed means all buffered values were drained, so the accumulated sum is final.
                self.sum += switch (self.recv.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result catch |err| return .{
                        .ready = if (err == error.Closed) self.sum else err,
                    },
                };

                self.state = .yield;
                continue :sw .yield;
            },
            .yield => {
                switch (self.yield.poll(task)) {
                    .pending => return .pending,
                    .ready => {},
                }

                if (self.canceled or task.cancel_requested) return .{ .ready = error.Canceled };

                self.recv = self.recv.channel.recv();
                self.state = .transfer;
                continue :sw .transfer;
            },
        }
    }
};

pub fn main() !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    // Caller-owned storage bounds the queue; senders wait when it fills.
    var storage: [8]u32 = undefined;
    var channel = Channel.init(&storage);
    defer channel.deinit();

    // Join waits for both children; neither may outlive the frame or channel storage.
    var frame: co.Frame(co.Join(Producer, Consumer)) = .{
        .coroutine = .{
            .a = .{ .send = channel.send(1) },
            .b = .{ .recv = channel.recv() },
        },
    };

    loop.executor.spawn(&frame.task);
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    const results = frame.result.?;
    try results.a;
    const sum = try results.b;

    if (sum != 5050) return error.IncorrectSum;

    std.debug.print("Verified sum of 1..100: {d}\n", .{sum});
}

test "pipeline joins both results and drains cancellation" {
    inline for (.{ false, true }) |canceled| {
        var loop = try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();

        // Rendezvous leaves a granted send unconsumed after the first poll, testing cancel-and-drain.
        var storage: [if (canceled) 0 else 8]u32 = undefined;
        var channel = Channel.init(&storage);
        defer channel.deinit();

        var frame: co.Frame(co.Join(Producer, Consumer)) = .{
            .coroutine = .{
                .a = .{ .send = channel.send(1) },
                .b = .{ .recv = channel.recv() },
            },
        };

        loop.executor.spawn(&frame.task);
        try std.testing.expect(loop.executor.runOne());

        if (canceled) {
            try std.testing.expectEqual(1, frame.task.outstanding_io);
            frame.cancel();
        }

        loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

        const results = frame.result.?;

        if (canceled) {
            try std.testing.expectError(error.Canceled, results.a);
            try std.testing.expectError(error.Canceled, results.b);
        } else {
            try results.a;
            try std.testing.expectEqual(5050, try results.b);
        }

        try std.testing.expectEqual(0, frame.task.outstanding_io);
        try std.testing.expectEqual(0, channel.count);
    }
}
