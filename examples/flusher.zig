const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const testing = std.testing;

const Mailbox = struct {
    loop: *io.Loop,
    std_io: std.Io,
    mutex: std.Io.Mutex = .init,
    accepting: bool = true,
    generation: u64 = 0,
    deadline: i128 = 0,
    notice: Notice,
    waiter: ?*co.Task = null,
    notifications: usize = 0,

    const Notice = struct {
        mailbox: *Mailbox,
        post: io.Control.Post = .{ .callback = callback },

        fn callback(post: *io.Control.Post) void {
            const notice: *@This() = @fieldParentPtr("post", post);
            notice.mailbox.notifications += 1;
            if (notice.mailbox.waiter) |task| task.wake();
        }
    };

    // Pin the mailbox through the final callback and producer join, not just frame completion.
    fn init(self: *@This(), loop: *io.Loop, std_io: std.Io) void {
        self.* = .{ .loop = loop, .std_io = std_io, .notice = .{ .mailbox = self } };
        loop.retain();
    }

    fn arm(self: *@This(), deadline: i128) !u64 {
        self.mutex.lockUncancelable(self.std_io);
        defer self.mutex.unlock(self.std_io);
        if (!self.accepting) return error.Closed;
        if (self.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        self.generation += 1;
        self.deadline = deadline;
        // Closing admission must exclude publication as well as the mailbox write.
        self.loop.post(&self.notice.post);
        return self.generation;
    }

    fn close(self: *@This()) void {
        self.mutex.lockUncancelable(self.std_io);
        defer self.mutex.unlock(self.std_io);
        self.closeLocked();
    }

    fn closeLocked(self: *@This()) void {
        if (!self.accepting) return;
        self.accepting = false;
        self.loop.post(&self.notice.post);
        self.loop.release();
    }
};

const Sink = struct {
    started: std.Io.Event = .unset,
    release: std.Io.Event = .is_set,
    active: std.atomic.Value(usize) = .init(0),
    calls: std.atomic.Value(usize) = .init(0),
    fail: bool = false,
};

const Action = struct {
    sink: *Sink,
    generation: u64,

    pub fn run(self: *@This(), std_io: std.Io) error{FlushFailed}!u64 {
        std.debug.assert(self.sink.active.fetchAdd(1, .acq_rel) == 0);
        defer _ = self.sink.active.fetchSub(1, .acq_rel);
        _ = self.sink.calls.fetchAdd(1, .monotonic);
        self.sink.started.set(std_io);
        self.sink.release.waitUncancelable(std_io);
        if (self.sink.fail) return error.FlushFailed;
        return self.generation;
    }
};

const Flusher = struct {
    mailbox: *Mailbox,
    pool: *velo.workers.Pool,
    sink: *Sink,
    timer: ?io.Timeout = null,
    timer_deadline: i128 = 0,
    timer_canceling: bool = false,
    work: ?velo.workers.Pool.Work(Action) = null,
    flushed: u64 = 0,
    jobs: usize = 0,
    timer_starts: usize = 0,
    timer_cancels: usize = 0,
    canceled: bool = false,
    failure: ?anyerror = null,

    pub fn cancel(self: *@This()) void {
        self.canceled = true;
        self.mailbox.close();
        if (self.timer) |*timer| timer.cancel();
        if (self.work) |*work| work.cancel();
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!u64) {
        const mailbox = self.mailbox;
        if (mailbox.waiter == null) {
            mailbox.waiter = task;
            task.outstanding_io += 1;
        }
        if (task.cancel_requested) self.cancel();

        // Snapshot selection and job submission share the producer's linearization lock.
        mailbox.mutex.lockUncancelable(mailbox.std_io);
        defer mailbox.mutex.unlock(mailbox.std_io);
        while (true) {
            if (self.work) |*work| {
                const result = switch (work.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result,
                };
                work.deinit();
                self.work = null;
                if (result) |generation| {
                    self.flushed = generation;
                    self.jobs += 1;
                } else |err| {
                    self.failure = err;
                    mailbox.closeLocked();
                }
                continue;
            }

            if (self.timer) |*timer| {
                // Graceful close flushes dirty generations without waiting for future deadlines.
                if (!self.timer_canceling and (!mailbox.accepting or self.canceled or
                    mailbox.deadline < self.timer_deadline))
                {
                    self.timer_canceling = true;
                    self.timer_cancels += 1;
                    timer.cancel();
                }
                const result = switch (timer.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result,
                };
                timer.deinit();
                self.timer = null;
                result catch |err| {
                    if (err != error.OperationCanceled or !self.timer_canceling) {
                        self.failure = err;
                        mailbox.closeLocked();
                    }
                };
                self.timer_canceling = false;
                continue;
            }

            if (self.canceled or self.failure != null or
                (!mailbox.accepting and self.flushed == mailbox.generation))
            {
                // Admission is closed under this lock; no new post can follow this check.
                if (mailbox.notice.post.queued.load(.acquire)) return .pending;
                mailbox.waiter = null;
                task.outstanding_io -= 1;
                return .{ .ready = if (self.failure) |err|
                    err
                else if (self.canceled)
                    error.OperationCanceled
                else
                    self.flushed };
            }
            if (self.flushed == mailbox.generation) return .pending;

            const time = now() catch |err| {
                self.failure = err;
                mailbox.closeLocked();
                continue;
            };
            if (mailbox.accepting and mailbox.deadline > time) {
                const remaining = mailbox.deadline - time;
                self.timer_deadline = mailbox.deadline;
                self.timer_starts += 1;
                self.timer = io.Timeout.init(mailbox.loop, .{ .duration = .{
                    .sec = @intCast(@divTrunc(remaining, 1_000_000_000)),
                    .nsec = @intCast(@mod(remaining, 1_000_000_000)),
                } });
            } else {
                self.work = velo.workers.Pool.Work(Action).init(self.pool, .{
                    .sink = self.sink,
                    .generation = mailbox.generation,
                });
            }
        }
    }
};

fn now() !i128 {
    const time = try io.now();
    return @as(i128, time.sec) * 1_000_000_000 + time.nsec;
}

fn after(ms: i64) !i128 {
    return try now() + @as(i128, ms) * 1_000_000;
}

fn run(loop: *io.Loop) void {
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});
}

const Producer = struct {
    mailboxes: *[2]Mailbox,
    sinks: *[2]Sink,
    std_io: std.Io,
    failure: ?anyerror = null,

    fn produce(self: *@This()) void {
        defer self.mailboxes[0].loop.release();
        defer for (self.mailboxes) |*mailbox| mailbox.close();
        for (self.mailboxes, self.sinks) |*mailbox, *sink| {
            sink.started.waitUncancelable(self.std_io);
            _ = mailbox.arm(0) catch |err| {
                self.failure = err;
                return;
            };
            mailbox.close();
        }
    }
};

const Summary = struct { generation: u64, jobs: usize };

fn exercise(loop: *io.Loop, allocator: std.mem.Allocator, std_io: std.Io) ![2]Summary {
    var pool: velo.workers.Pool = undefined;
    try pool.init(allocator, loop, 2);
    defer pool.deinit();
    var mailboxes: [2]Mailbox = undefined;
    var sinks: [2]Sink = @splat(.{});
    var frames: [2]co.Frame(Flusher) = undefined;
    for (&mailboxes, &sinks, &frames) |*mailbox, *sink, *frame| {
        mailbox.init(loop, std_io);
        frame.* = .{ .coroutine = .{ .mailbox = mailbox, .pool = &pool, .sink = sink } };
        loop.spawn(frame);
    }
    defer {
        for (&frames) |*frame| frame.cancel();
        while (frames[0].result == null or frames[1].result == null or
            loop.control.hasWork()) run(loop);
    }
    const deadline = try after(10);
    for (&mailboxes) |*mailbox| _ = try mailbox.arm(deadline);

    var producer: Producer = .{ .mailboxes = &mailboxes, .sinks = &sinks, .std_io = std_io };
    loop.retain();
    const thread = std.Thread.spawn(.{}, Producer.produce, .{&producer}) catch |err| {
        loop.release();
        return err;
    };
    run(loop);
    thread.join();
    if (producer.failure) |err| return err;
    var summaries: [2]Summary = undefined;
    for (&frames, &summaries) |*frame, *summary| {
        summary.* = .{ .generation = try frame.getResult(), .jobs = frame.coroutine.jobs };
        if (frame.task.outstanding_io != 0) return error.BorrowNotDrained;
    }
    if (pool.outstanding != 0 or loop.control.registrations.first != null or
        loop.control.hasWork()) return error.LoopNotDrained;
    // Global shutdown would cancel dirty objects; graceful close must finish first.
    loop.shutdown();
    run(loop);
    return summaries;
}

pub fn main(init: std.process.Init) !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();
    const summaries = try exercise(&loop, init.gpa, init.io);
    for (summaries, 0..) |summary, object| {
        std.debug.print("object {d}: flushed generation {d} in {d} worker jobs\n", .{
            object, summary.generation, summary.jobs,
        });
    }
}

const Observed = struct {
    child: Flusher,
    stop_deadline: ?i128 = null,
    stop_generation: ?u64 = null,

    pub fn cancel(self: *@This()) void {
        self.child.cancel();
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!u64) {
        const result = self.child.poll(task);
        if (self.stop_deadline) |deadline| {
            if (self.child.timer != null and self.child.timer_deadline == deadline) {
                self.stop_deadline = null;
                self.child.mailbox.loop.stop();
            }
        }
        if (self.stop_generation) |generation| {
            if (self.child.work) |*work| {
                if (work.action.generation == generation) {
                    self.stop_generation = null;
                    self.child.mailbox.loop.stop();
                }
            }
        }
        return result;
    }
};

const Harness = struct {
    loop: io.Loop,
    pool: velo.workers.Pool,
    mailboxes: [2]Mailbox,
    sinks: [2]Sink,
    frames: [2]co.Frame(Observed),

    fn init(self: *@This(), epoll: bool, workers: usize) !void {
        self.loop = if (builtin.os.tag == .linux and epoll)
            try io.Loop.initWithBackend(.{ .entries = 8 }, .epoll)
        else
            try io.Loop.init(.{ .entries = 8 });
        errdefer self.loop.deinit();
        try self.pool.init(testing.allocator, &self.loop, workers);
        self.sinks = @splat(.{});
        for (&self.mailboxes, &self.sinks, &self.frames) |*mailbox, *sink, *frame| {
            mailbox.init(&self.loop, testing.io);
            frame.* = .{ .coroutine = .{ .child = .{
                .mailbox = mailbox,
                .pool = &self.pool,
                .sink = sink,
            } } };
            self.loop.spawn(frame);
        }
    }

    fn deinit(self: *@This()) void {
        for (&self.frames, &self.sinks) |*frame, *sink| {
            frame.cancel();
            sink.release.set(testing.io);
        }
        self.drain();
        self.pool.deinit();
        self.loop.deinit();
    }

    fn drain(self: *@This()) void {
        for (&self.mailboxes) |*mailbox| mailbox.close();
        while (self.frames[0].result == null or self.frames[1].result == null or
            self.loop.control.hasWork()) run(&self.loop);
    }

    fn expectDrained(self: *@This()) !void {
        try testing.expectEqual(0, self.pool.outstanding);
        try testing.expectEqual(0, self.loop.active);
        try testing.expectEqual(null, self.loop.pending.first);
        try testing.expectEqual(null, self.loop.control.registrations.first);
        try testing.expectEqual(null, self.loop.executor.all_tasks.first);
        try testing.expectEqual(null, self.loop.executor.ready.first);
        try testing.expect(!self.loop.control.hasWork());
        for (&self.frames, &self.mailboxes) |*frame, *mailbox| {
            try testing.expectEqual(0, frame.task.outstanding_io);
            try testing.expectEqual(null, mailbox.waiter);
            try testing.expect(!mailbox.notice.post.queued.load(.acquire));
            try testing.expectError(error.Closed, mailbox.arm(0));
        }
    }
};

const modes = if (builtin.os.tag == .linux) [_]bool{ false, true } else [_]bool{false};

test "flusher timers and cross-thread arms flush before global shutdown" {
    for (modes) |epoll| {
        var loop = if (builtin.os.tag == .linux and epoll)
            try io.Loop.initWithBackend(.{ .entries = 8 }, .epoll)
        else
            try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();
        for (try exercise(&loop, testing.allocator, testing.io)) |summary| {
            try testing.expectEqual(2, summary.generation);
            try testing.expectEqual(2, summary.jobs);
        }
    }
}

test "flusher coalesces accepted arms before submission and rejects late arms" {
    for (modes) |epoll| {
        var h: Harness = undefined;
        try h.init(epoll, 2);
        defer h.deinit();
        try testing.expectEqual(1, try h.mailboxes[0].arm(try after(60_000)));
        try testing.expectEqual(2, try h.mailboxes[0].arm(try after(120_000)));
        try testing.expectEqual(3, try h.mailboxes[0].arm(0));
        h.drain();
        try testing.expectEqual(3, try h.frames[0].getResult());
        try testing.expectEqual(1, h.frames[0].coroutine.child.jobs);
        try testing.expectEqual(1, h.mailboxes[0].notifications);
        try testing.expectEqual(1, h.sinks[0].calls.load(.acquire));
        try h.expectDrained();
    }
}

test "flusher earlier rearm cancels and drains the native timer before work" {
    for (modes) |epoll| {
        var h: Harness = undefined;
        try h.init(epoll, 1);
        defer h.deinit();
        const deadline = try after(60_000);
        _ = try h.mailboxes[0].arm(deadline);
        h.frames[0].coroutine.stop_deadline = deadline;
        run(&h.loop);
        try testing.expectEqual(.inflight, h.frames[0].coroutine.child.timer.?.operation.state);
        try testing.expectEqual(2, h.frames[0].task.outstanding_io);
        _ = try h.mailboxes[0].arm(0);
        h.frames[0].coroutine.stop_generation = 2;
        run(&h.loop);
        const child = &h.frames[0].coroutine.child;
        try testing.expect(child.timer == null);
        try testing.expectEqual(1, child.timer_cancels);
        try testing.expectEqual(2, child.work.?.action.generation);
        h.drain();
        try testing.expectEqual(2, try h.frames[0].getResult());
        try h.expectDrained();
    }
}

test "flusher later rearm waits for the old timer without starting stale work" {
    for (modes) |epoll| {
        var h: Harness = undefined;
        try h.init(epoll, 1);
        defer h.deinit();
        const first = try after(100);
        _ = try h.mailboxes[0].arm(first);
        h.frames[0].coroutine.stop_deadline = first;
        run(&h.loop);
        const later = try after(60_000);
        _ = try h.mailboxes[0].arm(later);
        h.frames[0].coroutine.stop_deadline = later;
        run(&h.loop);
        const child = &h.frames[0].coroutine.child;
        try testing.expectEqual(later, child.timer_deadline);
        try testing.expectEqual(2, child.timer_starts);
        try testing.expectEqual(0, child.timer_cancels);
        try testing.expectEqual(0, h.sinks[0].calls.load(.acquire));
        h.drain();
        try testing.expectEqual(2, try h.frames[0].getResult());
        try testing.expectEqual(1, child.jobs);
        try h.expectDrained();
    }
}

test "flusher keeps updates during claimed work dirty and never overlaps object jobs" {
    for (modes) |epoll| {
        var h: Harness = undefined;
        try h.init(epoll, 2);
        defer h.deinit();
        h.sinks[0].release.reset();
        _ = try h.mailboxes[0].arm(0);
        h.frames[0].coroutine.stop_generation = 1;
        run(&h.loop);
        h.sinks[0].started.waitUncancelable(testing.io);
        _ = try h.mailboxes[0].arm(try after(60_000));
        _ = try h.mailboxes[0].arm(try after(120_000));
        _ = try h.mailboxes[0].arm(0);
        h.frames[0].coroutine.stop_generation = 1;
        run(&h.loop);
        try testing.expectEqual(1, h.sinks[0].active.load(.acquire));
        try testing.expectEqual(1, h.sinks[0].calls.load(.acquire));
        try testing.expectEqual(1, h.pool.outstanding);
        h.mailboxes[0].close();
        h.frames[0].coroutine.stop_generation = 4;
        h.sinks[0].release.set(testing.io);
        run(&h.loop);
        const child = &h.frames[0].coroutine.child;
        try testing.expectEqual(1, child.flushed);
        try testing.expectEqual(4, child.work.?.action.generation);
        try testing.expectEqual(1, h.pool.outstanding);
        h.drain();
        try testing.expectEqual(4, try h.frames[0].getResult());
        try testing.expectEqual(2, child.jobs);
        try testing.expectEqual(2, h.sinks[0].calls.load(.acquire));
        try h.expectDrained();
    }
}

test "flusher surfaces worker failure and closes admission without stale notifications" {
    for (modes) |epoll| {
        for ([_]bool{ false, true }) |cancel_claimed| {
            var h: Harness = undefined;
            try h.init(epoll, 1);
            defer h.deinit();
            h.sinks[0].fail = true;
            h.sinks[0].release.reset();
            _ = try h.mailboxes[0].arm(0);
            h.mailboxes[1].close();
            h.frames[0].coroutine.stop_generation = 1;
            run(&h.loop);
            h.sinks[0].started.waitUncancelable(testing.io);
            if (cancel_claimed) h.frames[0].cancel();
            h.sinks[0].release.set(testing.io);
            run(&h.loop);
            try testing.expectError(error.FlushFailed, h.frames[0].getResult());
            try testing.expectEqual(0, h.frames[0].coroutine.child.flushed);
            try h.expectDrained();
        }
    }
}

test "flusher cancellation skips queued work but consumes the claimed real result" {
    for (modes) |epoll| {
        var h: Harness = undefined;
        try h.init(epoll, 1);
        defer h.deinit();
        h.sinks[0].release.reset();
        for (&h.mailboxes, &h.frames) |*mailbox, *frame| {
            _ = try mailbox.arm(0);
            frame.coroutine.stop_generation = 1;
        }
        run(&h.loop);
        h.sinks[0].started.waitUncancelable(testing.io);
        _ = try h.mailboxes[0].arm(0);
        h.frames[0].cancel();
        h.frames[1].cancel();
        try testing.expectError(error.Closed, h.mailboxes[0].arm(0));
        try testing.expectError(error.Closed, h.mailboxes[1].arm(0));
        h.sinks[0].release.set(testing.io);
        h.drain();
        for (&h.frames) |*frame| try testing.expectError(error.OperationCanceled, frame.getResult());
        try testing.expectEqual(1, h.frames[0].coroutine.child.flushed);
        try testing.expectEqual(0, h.frames[1].coroutine.child.flushed);
        try testing.expectEqual(1, h.sinks[0].calls.load(.acquire));
        try testing.expectEqual(0, h.sinks[1].calls.load(.acquire));
        try h.expectDrained();
    }
}

test "flusher shutdown rejects arms before polling and drains an active timer" {
    for (modes) |epoll| {
        for ([_]bool{ false, true }) |start_timer| {
            var h: Harness = undefined;
            try h.init(epoll, 1);
            defer h.deinit();
            const deadline = try after(60_000);
            _ = try h.mailboxes[0].arm(deadline);
            if (start_timer) {
                h.frames[0].coroutine.stop_deadline = deadline;
                run(&h.loop);
                try testing.expectEqual(2, h.frames[0].task.outstanding_io);
            }
            h.loop.shutdown();
            run(&h.loop);
            for (&h.frames, &h.sinks) |*frame, *sink| {
                try testing.expectError(error.OperationCanceled, frame.getResult());
                try testing.expectEqual(0, sink.calls.load(.acquire));
            }
            try h.expectDrained();
        }
    }
}
