const std = @import("std");
const builtin = @import("builtin");
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

// A lost wake strands the producer here, not the loop, so the producer reports it.
fn awaitAck(acked: *std.atomic.Value(usize), expected: usize, started: io.Timespec) void {
    while (acked.load(.acquire) != expected) {
        const now = io.now() catch @panic("clock failed");
        if (now.sec - started.sec > 60) @panic("post stranded: lost wake");
        std.atomic.spinLoopHint();
    }
}

test "posts racing a parking loop are never stranded" {
    const Producer = struct {
        loop: *io.Loop,
        rounds: usize,
        // Sweeps the post across the window between the loop's last service and its block.
        stride: usize,
        post: io.Control.Post = .{ .callback = callback },
        acked: std.atomic.Value(usize) = .init(0),

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            _ = self.acked.fetchAdd(1, .release);
        }

        fn produce(self: *@This()) void {
            const started = io.now() catch @panic("clock failed");
            for (0..self.rounds) |round| {
                for (0..round * self.stride % 512) |_| std.atomic.spinLoopHint();
                self.loop.post(&self.post);
                awaitAck(&self.acked, round + 1, started);
            }
            self.loop.release();
        }
    };

    for (support.modes) |mode| {
        for ([_]u32{ 0, 2_000 }) |spin_ns| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();
            if (builtin.os.tag == .linux) loop.idle_spin_ns = spin_ns;

            var producers: [3]Producer = undefined;
            var threads: [3]std.Thread = undefined;
            for (&producers, &threads, 0..) |*producer, *thread, index| {
                producer.* = .{ .loop = &loop, .rounds = 4_000, .stride = 3 + index * 2 };
                loop.retain();
                thread.* = try std.Thread.spawn(.{}, Producer.produce, .{producer});
            }

            support.run(&loop);
            for (threads) |thread| thread.join();
            for (&producers) |*producer| try testing.expectEqual(4_000, producer.acked.load(.acquire));
            try drained(&loop);
        }
    }
}

test "posts wake only a parked loop, once per empty stack" {
    const Counter = struct {
        post: io.Control.Post = .{ .callback = callback },
        ran: usize = 0,

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.ran += 1;
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var first: Counter = .{};
        var second: Counter = .{};

        loop.retain();
        loop.post(&first.post);
        try testing.expectEqual(0, loop.stats().wakes);

        // The first sender owns the wake for everything behind it in the stack.
        loop.control.parked.store(true, .seq_cst);
        loop.post(&second.post);
        try testing.expectEqual(0, loop.stats().wakes);
        loop.control.parked.store(false, .seq_cst);

        // Uncounted: a release must not write to a loop that it may have let exit.
        loop.release();
        try testing.expectEqual(0, loop.stats().wakes);
        support.run(&loop);

        loop.retain();
        loop.control.parked.store(true, .seq_cst);
        loop.post(&first.post);
        try testing.expectEqual(1, loop.stats().wakes);
        loop.control.parked.store(false, .seq_cst);
        loop.release();
        try testing.expectEqual(1, loop.stats().wakes);
        support.run(&loop);

        try testing.expectEqual(2, first.ran);
        try testing.expectEqual(1, second.ran);
        try testing.expectEqual(3, loop.stats().posts);
        try drained(&loop);
    }
}

test "idle spin ends on timers, posts and stop instead of waiting out its window" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const window = 4_000_000_000;
    const Stopper = struct {
        fn run(loop: *io.Loop) void {
            std.Io.sleep(testing.io, .fromMilliseconds(1), .awake) catch {};
            loop.stop();
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        loop.idle_spin_ns = window;

        var timer: co.Frame(io.Timeout) = .{ .coroutine = io.Timeout.init(&loop, .{ .duration = io.milliseconds(1) }) };
        const started = try io.now();
        loop.spawn(&timer);
        support.run(&loop);
        try timer.getResult();

        loop.retain();
        const stopper = try std.Thread.spawn(.{}, Stopper.run, .{&loop});
        support.run(&loop);
        stopper.join();
        loop.release();
        support.run(&loop);

        const finished = try io.now();
        try testing.expect(finished.sec - started.sec < 2);
        try drained(&loop);
    }
}

test "an expired idle spin parks and still wakes for a post" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    const Late = struct {
        post: io.Control.Post = .{ .callback = callback },
        loop: *io.Loop,
        ran: bool = false,

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            self.ran = true;
        }

        fn send(self: *@This()) void {
            std.Io.sleep(testing.io, .fromMilliseconds(20), .awake) catch {};
            self.loop.control.postRelease(&self.post);
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        loop.idle_spin_ns = 10_000;
        var late: Late = .{ .loop = &loop };

        loop.retain();
        const sender = try std.Thread.spawn(.{}, Late.send, .{&late});
        support.run(&loop);
        sender.join();

        try testing.expect(late.ran);
        try testing.expect(loop.stats().parks != 0);
        try drained(&loop);
    }
}

fn burn(ms: u64) !void {
    const start = try io.now();
    while (true) {
        const now = try io.now();
        const elapsed = (now.sec - start.sec) * 1_000 + @divTrunc(now.nsec - start.nsec, 1_000_000);
        if (elapsed >= ms) return;
    }
}

test "long polls give I/O a turn within the quota and short ones keep the poll budget" {
    const Spinner = struct {
        left: usize,
        ms: u64,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            self.left -= 1;
            if (self.left == 0) return .{ .ready = {} };
            burn(self.ms) catch @panic("clock failed");
            task.wake();
            return .pending;
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        // A poll budget alone would run 64 of these per turn, however long each took.
        var long: [4]co.Frame(Spinner) = @splat(.init(.{ .left = 100, .ms = 1 }));
        for (&long) |*frame| loop.spawn(frame);
        var before = loop.stats();
        support.run(&loop);
        var after = loop.stats();
        try testing.expect(after.polls - before.polls < (after.turns - before.turns) * 8);
        if (co.Executor.named) try testing.expect(after.longest_poll_ns >= std.time.ns_per_ms);

        var short: [4]co.Frame(Spinner) = @splat(.init(.{ .left = 20_000, .ms = 0 }));
        for (&short) |*frame| loop.spawn(frame);
        before = after;
        support.run(&loop);
        after = loop.stats();
        const short_polls = after.polls - before.polls;
        const short_turns = after.turns - before.turns;
        try testing.expect(short_polls > short_turns * 32 and short_polls <= short_turns * 64);
        try drained(&loop);
    }
}

test "shouldYield hands a long computation's next turn to I/O" {
    const Hog = struct {
        loop: *io.Loop,
        timer: *co.Frame(io.Timeout),
        yields: usize = 0,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            if (self.timer.result != null) return .{ .ready = {} };
            while (!self.loop.shouldYield()) std.atomic.spinLoopHint();
            self.yields += 1;
            task.wake();
            return .pending;
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();
        var timer: co.Frame(io.Timeout) = .{ .coroutine = io.Timeout.init(&loop, .{ .duration = io.milliseconds(5) }) };
        var hog: co.Frame(Hog) = .init(.{ .loop = &loop, .timer = &timer });

        loop.spawn(&timer);
        loop.spawn(&hog);
        support.run(&loop);

        hog.getResult();
        try timer.getResult();
        try testing.expect(hog.coroutine.yields > 1);
        try drained(&loop);
    }
}

const Busy = struct {
    done: *bool,
    polls: usize = 0,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
        if (self.done.*) return .{ .ready = {} };
        self.polls += 1;
        var i: u32 = 0;
        while (i < 2_000) : (i += 1) std.atomic.spinLoopHint();
        task.wake();
        return .pending;
    }
};

const StopAfter = struct {
    timer: io.Timeout,
    done: *bool,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
        switch (self.timer.poll(task)) {
            .pending => return .pending,
            .ready => |result| {
                result catch @panic("timer failed");
                self.done.* = true;
                return .{ .ready = {} };
            },
        }
    }
};

test "classes split loop time by shares and unclassed tasks compete at the default" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var done = false;
        var foreground: co.Class = .{ .shares = 3000 };
        var background: co.Class = .{ .shares = 500 };
        loop.executor.addClass(&foreground);
        loop.executor.addClass(&background);

        var heavy: co.Frame(Busy) = .init(.{ .done = &done });
        var light: co.Frame(Busy) = .init(.{ .done = &done });
        var plain: co.Frame(Busy) = .init(.{ .done = &done });
        var stopper: co.Frame(StopAfter) = .init(.{
            .timer = io.Timeout.init(&loop, .{ .duration = io.milliseconds(120) }),
            .done = &done,
        });
        heavy.task.class = &foreground;
        light.task.class = &background;
        loop.spawn(&heavy);
        loop.spawn(&light);
        loop.spawn(&plain);
        loop.spawn(&stopper);
        support.run(&loop);

        const heavy_polls: f64 = @floatFromInt(heavy.coroutine.polls);
        const light_polls: f64 = @floatFromInt(light.coroutine.polls);
        const plain_polls: f64 = @floatFromInt(plain.coroutine.polls);
        try testing.expect(heavy_polls / light_polls > 3 and heavy_polls / light_polls < 12);
        try testing.expect(plain_polls / light_polls > 1 and plain_polls / light_polls < 4);
        try drained(&loop);
    }
}

test "a class waking from idle does not spend time it banked while asleep" {
    const Late = struct {
        timer: io.Timeout,
        late: *co.Frame(Busy),
        class: *co.Class,
        loop: *io.Loop,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            switch (self.timer.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    result catch @panic("timer failed");
                    self.late.task.class = self.class;
                    self.loop.spawn(self.late);
                    return .{ .ready = {} };
                },
            }
        }
    };

    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var done = false;
        var sleeper: co.Class = .{};
        loop.executor.addClass(&sleeper);

        var busy: co.Frame(Busy) = .init(.{ .done = &done });
        var late: co.Frame(Busy) = .init(.{ .done = &done });
        var starter: co.Frame(Late) = .init(.{
            .timer = io.Timeout.init(&loop, .{ .duration = io.milliseconds(60) }),
            .late = &late,
            .class = &sleeper,
            .loop = &loop,
        });
        var stopper: co.Frame(StopAfter) = .init(.{
            .timer = io.Timeout.init(&loop, .{ .duration = io.milliseconds(120) }),
            .done = &done,
        });
        loop.spawn(&busy);
        loop.spawn(&starter);
        loop.spawn(&stopper);
        support.run(&loop);

        // Banked credit would give the sleeper the second half almost entirely.
        const share = @as(f64, @floatFromInt(late.coroutine.polls)) /
            @as(f64, @floatFromInt(busy.coroutine.polls));
        try testing.expect(share < 0.7);
        try testing.expect(late.coroutine.polls != 0);
        try drained(&loop);
    }
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

test "runtime task cancellation reaches queued and inflight I/O without a parent cancel method" {
    const Timer = struct {
        child: io.Timeout,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
            return self.child.poll(task);
        }
    };
    const Canceller = struct {
        target: *co.Frame(Timer),
        timer: io.Timeout,
        inflight: bool = false,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
            const result = self.timer.poll(task).value() orelse return .pending;
            result catch |err| return .{ .ready = err };
            self.inflight = self.target.coroutine.child.operation.state == .inflight;
            self.target.cancel();
            return .{ .ready = {} };
        }
    };

    for (support.modes) |mode| {
        for ([_]bool{ false, true }) |inflight| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();
            var frame: co.Frame(Timer) = .init(.{
                .child = .init(&loop, .{ .duration = io.seconds(1) }),
            });
            try testing.expectEqual(null, frame.task.cancel_fn);
            loop.spawn(&frame);
            std.debug.assert(loop.executor.runOne());
            std.debug.assert(frame.coroutine.child.operation.state == .queued);

            var canceller: co.Frame(Canceller) = .init(.{
                .target = &frame,
                .timer = .init(&loop, .{ .duration = io.milliseconds(1) }),
            });
            if (inflight) loop.spawn(&canceller) else frame.cancel();
            support.run(&loop);

            if (inflight) {
                try canceller.getResult();
                try testing.expect(canceller.coroutine.inflight);
            }
            try testing.expectError(error.OperationCanceled, frame.getResult());
            try testing.expectEqual(0, frame.task.outstanding_io);
            try drained(&loop);
        }
    }
}

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

test "runtime block drains other spawned work before returning the frame result" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        var short: co.Frame(io.Timeout) = .init(io.Timeout.init(&loop, .{
            .duration = .{ .sec = 0, .nsec = 1_000_000 },
        }));
        var long: co.Frame(io.Timeout) = .init(io.Timeout.init(&loop, .{
            .duration = .{ .sec = 0, .nsec = 5_000_000 },
        }));

        loop.spawn(&long);
        try loop.block(&short);

        try long.getResult();
        try testing.expectEqual(0, long.task.outstanding_io);
        try drained(&loop);
    }
}
