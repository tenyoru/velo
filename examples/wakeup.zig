const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.io;

const Waiter = struct {
    child: co.Event.Wait,
    gate: *std.Io.Event,
    std_io: std.Io,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(error{Canceled}!void) {
        const result = self.child.poll(task);

        // Publish only after the single-threaded event has parked this task.
        if (result == .pending) self.gate.set(self.std_io);
        return result;
    }
};

const Producer = struct {
    post: io.Control.Post = .{ .callback = callback },
    loop: *io.Loop,
    event: *co.Event,
    gate: *std.Io.Event,
    std_io: std.Io,
    producer_thread: ?std.Thread.Id = null,
    callback_thread: ?std.Thread.Id = null,
    calls: usize = 0,

    fn run(self: *@This()) void {
        self.producer_thread = std.Thread.getCurrentId();
        self.gate.waitUncancelable(self.std_io);
        self.loop.post(&self.post);
        self.loop.release();
    }

    fn callback(post: *io.Control.Post) void {
        const self: *@This() = @fieldParentPtr("post", post);

        self.callback_thread = std.Thread.getCurrentId();
        self.calls += 1;
        self.event.set();
        self.loop.stop();
    }
};

fn exercise(loop: *io.Loop, std_io: std.Io) !void {
    const loop_thread = std.Thread.getCurrentId();
    var event: co.Event = .{};
    defer event.deinit();

    var gate: std.Io.Event = .unset;
    var waiter: co.Frame(Waiter) = .{
        .coroutine = .{
            .child = event.wait(),
            .gate = &gate,
            .std_io = std_io,
        },
    };
    var producer: Producer = .{
        .loop = loop,
        .event = &event,
        .gate = &gate,
        .std_io = std_io,
    };

    loop.retain();
    const thread = std.Thread.spawn(.{}, Producer.run, .{&producer}) catch |err| {
        loop.release();
        return err;
    };

    loop.executor.spawn(&waiter.task);

    // Run errors cannot unwind storage still borrowed by the loop or producer.
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});
    thread.join();

    const stopped = waiter.result == null;
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    if (producer.calls != 1) return error.IncorrectCallbackCount;

    if (producer.callback_thread != loop_thread or
        producer.callback_thread == producer.producer_thread)
        return error.IncorrectCallbackThread;

    if (!stopped) return error.StopDidNotPreserveWaiter;
    try (waiter.result orelse return error.WaiterIncomplete);

    if (waiter.task.outstanding_io != 0 or loop.executor.all_tasks.first != null or
        loop.executor.ready.first != null or loop.control.hasWork())
        return error.LoopNotDrained;
}

pub fn main(init: std.process.Init) !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    try exercise(&loop, init.io);
    std.debug.print("PASS: cross-thread post, retain/release, stop/resume\n", .{});
}

test "wakeup posts on the loop thread and resumes a stopped waiter" {
    inline for (if (builtin.os.tag == .linux) .{ false, true } else .{false}) |epoll| {
        var loop = if (epoll)
            try io.Loop.initWithBackend(.{ .entries = 8 }, .epoll)
        else
            try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();

        try exercise(&loop, std.testing.io);
    }
}
