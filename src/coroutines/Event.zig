//! Manual-reset event. `set()` grants all queued waits, even if reset before polling.
//! Waits are single-shot; cancellation unlinks ungranted waits before waking them.

const std = @import("std");
const co = @import("root.zig");
const states = @import("../state.zig");
const assert = std.debug.assert;
const Event = @This();

signaled: bool = false,
waiters: std.DoublyLinkedList = .{},

pub fn wait(self: *Event) Wait {
    return .{ .event = self };
}

pub fn set(self: *Event) void {
    self.signaled = true;

    while (self.waiters.popFirst()) |node| {
        const waiter: *Wait = @alignCast(@fieldParentPtr("node", node));

        waiter.state = .granted;
        waiter.task.?.wake();
    }
}

pub fn reset(self: *Event) void {
    self.signaled = false;
}

pub fn deinit(self: *Event) void {
    assert(self.waiters.first == null);
}

pub const Wait = struct {
    event: *Event,
    node: std.DoublyLinkedList.Node = .{},
    task: ?*co.Task = null,
    state: states.Wait = .idle,

    pub fn poll(self: *Wait, task: *co.Task) co.Poll(error{Canceled}!void) {
        assert(self.state != .done);
        if (self.task) |borrowed| assert(borrowed == task);
        if (task.cancel_requested) self.cancel();

        if (self.state == .idle) {
            if (self.event.signaled) {
                self.state = .granted;
            } else {
                self.task = task;
                task.outstanding_io += 1;
                self.event.waiters.append(&self.node);
                self.state = .waiting;
            }
        }

        if (self.state == .waiting) return .pending;
        if (self.task != null) task.outstanding_io -= 1;

        const canceled = self.state == .canceled;
        self.state = .done;

        return .{ .ready = if (canceled) error.Canceled else {} };
    }

    pub fn cancel(self: *Wait) void {
        switch (self.state) {
            .idle => self.state = .canceled,
            .waiting => {
                self.event.waiters.remove(&self.node);
                self.state = .canceled;
                self.task.?.wake();
            },
            .granted, .canceled, .done => {},
        }
    }
};

test "event broadcast is FIFO and survives reset and late cancellation" {
    var event: Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var a: co.Frame(Wait) = .{ .coroutine = event.wait() };
    var b: co.Frame(Wait) = .{ .coroutine = event.wait() };
    var c: co.Frame(Wait) = .{ .coroutine = event.wait() };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.spawn(&c.task);
    ex.run();

    try std.testing.expectEqual(3, event.waiters.len());

    b.cancel();
    b.cancel();

    try std.testing.expectEqual(2, event.waiters.len());
    try std.testing.expectEqual(1, b.task.outstanding_io);

    event.set();
    event.set();
    event.reset();
    a.cancel();

    try std.testing.expectEqual(1, a.task.outstanding_io);
    try std.testing.expectEqual(null, event.waiters.first);

    try std.testing.expect(ex.runOne());
    try std.testing.expectError(error.Canceled, b.getResult());
    try std.testing.expectEqual(null, a.result);

    try std.testing.expect(ex.runOne());
    try a.getResult();
    try std.testing.expectEqual(null, c.result);

    try std.testing.expect(ex.runOne());
    try c.getResult();

    for ([_]*co.Task{ &a.task, &b.task, &c.task }) |task| {
        try std.testing.expectEqual(0, task.outstanding_io);
    }

    var next: co.Frame(Wait) = .{ .coroutine = event.wait() };

    ex.spawn(&next.task);
    ex.run();

    try std.testing.expectEqual(null, next.result);

    event.set();
    ex.run();

    try next.getResult();

    var immediate: co.Frame(Wait) = .{ .coroutine = event.wait() };

    ex.spawn(&immediate.task);
    ex.run();

    try immediate.getResult();
    try std.testing.expectEqual(0, immediate.task.outstanding_io);
}

test "event honors pre-cancel and task cancellation without a parent cancel method" {
    const Parent = struct {
        child: Wait,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(error{Canceled}!void) {
            return self.child.poll(task);
        }
    };

    var event: Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var parent: co.Frame(Parent) = .{
        .coroutine = .{
            .child = event.wait(),
        },
    };

    try std.testing.expectEqual(null, parent.task.cancel_fn);

    ex.spawn(&parent.task);
    ex.run();

    ex.cancelAll();
    ex.run();

    try std.testing.expectError(error.Canceled, parent.getResult());
    try std.testing.expectEqual(0, parent.task.outstanding_io);

    inline for (.{ false, true }) |signaled| {
        event.signaled = signaled;
        var frame: co.Frame(Wait) = .{ .coroutine = event.wait() };

        frame.cancel();
        ex.spawn(&frame.task);
        ex.run();

        try std.testing.expectError(error.Canceled, frame.getResult());
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}

test "event deterministic action model preserves broadcast snapshots" {
    const Probe = struct {
        op: Wait,
        id: usize,
        trace: *usize,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(error{Canceled}!void) {
            self.trace.* = self.id;
            return self.op.poll(task);
        }

        pub fn cancel(self: *@This()) void {
            self.op.cancel();
        }
    };
    const Model = struct {
        const Entry = struct {
            outcome: enum { pending, granted, canceled } = .pending,
            started: bool = false,
            done: bool = false,
        };
        event: Event = .{},
        signaled: bool = false,
        ex: co.Executor = .{},
        frames: [64]co.Frame(Probe) = undefined,
        entries: [64]Entry = undefined,
        count: usize = 0,
        trace: usize = 0,

        fn spawn(self: *@This()) void {
            const id = self.count;
            self.count += 1;
            self.entries[id] = .{};
            self.frames[id] = .{ .coroutine = .{ .op = self.event.wait(), .id = id, .trace = &self.trace } };
            self.ex.spawn(&self.frames[id].task);
        }

        fn cancel(self: *@This(), id: usize) void {
            if (self.entries[id].outcome == .pending) self.entries[id].outcome = .canceled;
            self.frames[id].cancel();
        }

        fn set(self: *@This()) void {
            self.signaled = true;
            for (self.entries[0..self.count]) |*entry| {
                if (entry.started and entry.outcome == .pending) entry.outcome = .granted;
            }
            self.event.set();
        }

        fn reset(self: *@This()) void {
            self.signaled = false;
            self.event.reset();
        }

        fn runOne(self: *@This()) !void {
            if (!self.ex.runOne()) return;
            const entry = &self.entries[self.trace];
            if (!entry.started) {
                entry.started = true;
                if (entry.outcome == .pending and self.signaled) entry.outcome = .granted;
            }
            if (entry.outcome != .pending) {
                entry.done = true;
                try std.testing.expect(self.frames[self.trace].result != null);
                if (entry.outcome == .granted) {
                    try self.frames[self.trace].getResult();
                } else try std.testing.expectError(error.Canceled, self.frames[self.trace].getResult());
            }
        }

        fn check(self: *@This()) !void {
            var active: usize = 0;
            var waiting: usize = 0;
            for (self.entries[0..self.count], self.frames[0..self.count]) |entry, *frame| {
                try std.testing.expectEqual(entry.done, frame.result != null);
                try std.testing.expectEqual(!entry.done, frame.task.registered);
                try std.testing.expectEqual(@intFromBool(entry.started and !entry.done), frame.task.outstanding_io);
                active += @intFromBool(!entry.done);
                waiting += @intFromBool(entry.started and entry.outcome == .pending);
            }
            try std.testing.expectEqual(active, self.ex.all_tasks.len());
            try std.testing.expectEqual(waiting, self.event.waiters.len());
            try std.testing.expectEqual(self.signaled, self.event.signaled);
        }
    };

    for (0..256) |seed| {
        var model: Model = .{};
        var rng = std.Random.DefaultPrng.init(seed);
        var step: usize = 0;
        errdefer std.debug.print("event seed={d} step={d}\n", .{ seed, step });
        while (step < 64) : (step += 1) {
            if (step < 6) {
                if (step % 2 == 0) model.spawn() else try model.runOne();
            } else switch (step) {
                6 => model.cancel(1),
                7 => model.set(),
                8 => model.reset(),
                9 => model.cancel(0),
                10 => model.spawn(),
                else => switch (rng.random().uintLessThan(u8, 8)) {
                    0, 1 => model.spawn(),
                    2 => model.cancel(rng.random().uintLessThan(usize, model.count)),
                    3 => model.set(),
                    4 => model.reset(),
                    else => try model.runOne(),
                },
            }
            try model.check();
        }
        model.set();
        model.reset();
        for (0..model.count) |id| model.cancel(id);
        for (0..model.count * 2) |_| try model.runOne();
        try model.check();
        try std.testing.expectEqual(null, model.ex.all_tasks.first);
        try std.testing.expectEqual(null, model.ex.ready.first);
        model.event.deinit();
    }
}
