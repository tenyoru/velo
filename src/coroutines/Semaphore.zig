//! FIFO counting semaphore. Use `.init(1)` as a mutex and `release()` after success.
//! A granted permit belongs to the caller even if cancellation arrives before polling.

const std = @import("std");
const co = @import("root.zig");
const states = @import("../state.zig");
const assert = std.debug.assert;
const Semaphore = @This();

permits: usize = 0,
waiters: std.DoublyLinkedList = .{},

pub fn init(permits: usize) Semaphore {
    return .{ .permits = permits };
}

pub fn acquire(self: *Semaphore) Acquire {
    return .{ .semaphore = self };
}

pub fn release(self: *Semaphore) void {
    if (self.waiters.popFirst()) |node| {
        const waiter: *Acquire = @alignCast(@fieldParentPtr("node", node));

        waiter.state = .granted;
        waiter.task.?.wake();
    } else {
        self.permits += 1;
    }
}

pub fn deinit(self: *Semaphore) void {
    assert(self.waiters.first == null);
}

pub const Acquire = struct {
    semaphore: *Semaphore,
    node: std.DoublyLinkedList.Node = .{},
    task: ?*co.Task = null,
    state: states.Wait = .idle,

    pub fn poll(self: *Acquire, task: *co.Task) co.Poll(error{Canceled}!void) {
        assert(self.state != .done);
        if (self.task) |borrowed| assert(borrowed == task);
        if (task.cancel_requested) self.cancel();

        if (self.state == .idle) {
            if (self.semaphore.permits != 0) {
                self.semaphore.permits -= 1;
                self.state = .granted;
            } else {
                self.task = task;
                task.outstanding_io += 1;
                self.semaphore.waiters.append(&self.node);
                self.state = .waiting;
            }
        }

        if (self.state == .waiting) return .pending;
        if (self.task != null) task.outstanding_io -= 1;

        const canceled = self.state == .canceled;
        self.state = .done;

        return .{ .ready = if (canceled) error.Canceled else {} };
    }

    pub fn cancel(self: *Acquire) void {
        switch (self.state) {
            .idle => self.state = .canceled,
            .waiting => {
                self.semaphore.waiters.remove(&self.node);
                self.state = .canceled;
                self.task.?.wake();
            },
            .granted, .canceled, .done => {},
        }
    }
};

test "semaphore grants FIFO without barging and preserves a late-canceled permit" {
    var sem = Semaphore.init(1);
    defer sem.deinit();

    var ex: co.Executor = .{};
    var first: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };

    ex.spawn(&first.task);
    ex.run();

    try first.getResult();
    try std.testing.expectEqual(0, sem.permits);

    var a: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };
    var b: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };
    var c: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.run();

    sem.release();
    a.cancel();

    try std.testing.expectEqual(1, a.task.outstanding_io);

    ex.spawn(&c.task);
    ex.run();

    try a.getResult();
    try std.testing.expectEqual(null, b.result);
    try std.testing.expectEqual(null, c.result);

    b.cancel();

    try std.testing.expectEqual(1, b.task.outstanding_io);
    try std.testing.expectEqual(1, sem.waiters.len());

    ex.run();

    try std.testing.expectError(error.Canceled, b.getResult());

    sem.release();

    try std.testing.expectEqual(1, c.task.outstanding_io);

    ex.run();

    try c.getResult();

    sem.release();

    try std.testing.expectEqual(1, sem.permits);
    for ([_]*co.Task{ &a.task, &b.task, &c.task }) |task| {
        try std.testing.expectEqual(0, task.outstanding_io);
    }
}

test "semaphore pre-cancel leaves permits and cancelAll drains blocked acquisitions" {
    var sem = Semaphore.init(2);
    defer sem.deinit();

    var ex: co.Executor = .{};
    var pre: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };

    pre.cancel();
    ex.spawn(&pre.task);
    ex.run();

    try std.testing.expectError(error.Canceled, pre.getResult());
    try std.testing.expectEqual(2, sem.permits);

    sem.permits = 0;
    var a: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };
    var b: co.Frame(Acquire) = .{ .coroutine = sem.acquire() };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.run();

    ex.cancelAll();

    try std.testing.expectEqual(null, sem.waiters.first);

    ex.run();

    try std.testing.expectError(error.Canceled, a.getResult());
    try std.testing.expectError(error.Canceled, b.getResult());
    try std.testing.expectEqual(0, a.task.outstanding_io);
    try std.testing.expectEqual(0, b.task.outstanding_io);

    sem.release();

    try std.testing.expectEqual(1, sem.permits);
}

test "semaphore deterministic action model conserves permits and FIFO grants" {
    const Probe = struct {
        op: Acquire,
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
            ticket: usize = 0,
            started: bool = false,
            done: bool = false,
            held: bool = false,
        };
        sem: Semaphore,
        free: usize,
        ex: co.Executor = .{},
        frames: [64]co.Frame(Probe) = undefined,
        entries: [64]Entry = undefined,
        count: usize = 0,
        clock: usize = 0,
        trace: usize = 0,

        fn spawn(self: *@This()) void {
            const id = self.count;
            self.count += 1;
            self.entries[id] = .{};
            self.frames[id] = .{ .coroutine = .{ .op = self.sem.acquire(), .id = id, .trace = &self.trace } };
            self.ex.spawn(&self.frames[id].task);
        }

        fn cancel(self: *@This(), id: usize) void {
            if (self.entries[id].outcome == .pending) self.entries[id].outcome = .canceled;
            self.frames[id].cancel();
        }

        fn release(self: *@This(), id: usize) void {
            if (!self.entries[id].held) return;
            self.entries[id].held = false;
            var first: ?usize = null;
            for (self.entries[0..self.count], 0..) |entry, candidate| {
                if (!entry.started or entry.outcome != .pending) continue;
                if (first == null or entry.ticket < self.entries[first.?].ticket) first = candidate;
            }
            if (first) |waiter| self.entries[waiter].outcome = .granted else self.free += 1;
            self.sem.release();
        }

        fn runOne(self: *@This()) !void {
            if (!self.ex.runOne()) return;
            const entry = &self.entries[self.trace];
            if (!entry.started) {
                entry.started = true;
                entry.ticket = self.clock;
                self.clock += 1;
                if (entry.outcome == .pending and self.free > 0) {
                    self.free -= 1;
                    entry.outcome = .granted;
                }
            }
            if (entry.outcome != .pending) {
                entry.done = true;
                try std.testing.expect(self.frames[self.trace].result != null);
                if (entry.outcome == .granted) {
                    try self.frames[self.trace].getResult();
                    entry.held = true;
                } else try std.testing.expectError(error.Canceled, self.frames[self.trace].getResult());
            }
        }

        fn check(self: *@This(), capacity: usize) !void {
            var active: usize = 0;
            var owned: usize = 0;
            for (self.entries[0..self.count], self.frames[0..self.count]) |entry, *frame| {
                try std.testing.expectEqual(entry.done, frame.result != null);
                try std.testing.expectEqual(!entry.done, frame.task.registered);
                try std.testing.expectEqual(@intFromBool(entry.started and !entry.done), frame.task.outstanding_io);
                active += @intFromBool(!entry.done);
                owned += @intFromBool(entry.held or (entry.outcome == .granted and !entry.done));
            }
            try std.testing.expectEqual(active, self.ex.all_tasks.len());
            try std.testing.expectEqual(self.free, self.sem.permits);
            try std.testing.expectEqual(capacity, self.free + owned);
        }
    };

    for ([_]usize{ 0, 1, 3 }) |capacity| {
        for (0..256) |seed| {
            var model: Model = .{ .sem = Semaphore.init(capacity), .free = capacity };
            var rng = std.Random.DefaultPrng.init(seed);
            var step: usize = 0;
            errdefer std.debug.print("semaphore capacity={d} seed={d} step={d}\n", .{ capacity, seed, step });
            const prefix = 2 * (capacity + 3);
            while (step < 64) : (step += 1) {
                if (step < prefix) {
                    if (step % 2 == 0) model.spawn() else try model.runOne();
                } else if (step == prefix) {
                    model.cancel(capacity + 1);
                } else if (step == prefix + 1) {
                    model.release(0);
                } else if (step == prefix + 2) {
                    model.cancel(capacity);
                } else if (step == prefix + 3) {
                    model.spawn();
                } else switch (rng.random().uintLessThan(u8, 8)) {
                    0, 1 => model.spawn(),
                    2 => model.cancel(rng.random().uintLessThan(usize, model.count)),
                    3 => model.release(rng.random().uintLessThan(usize, model.count)),
                    else => try model.runOne(),
                }
                try model.check(capacity);
            }
            for (0..model.count) |id| model.cancel(id);
            for (0..model.count * 2) |_| try model.runOne();
            for (0..model.count) |id| model.release(id);
            try model.check(capacity);
            try std.testing.expectEqual(capacity, model.sem.permits);
            try std.testing.expectEqual(null, model.ex.all_tasks.first);
            try std.testing.expectEqual(null, model.ex.ready.first);
            model.sem.deinit();
        }
    }
}
