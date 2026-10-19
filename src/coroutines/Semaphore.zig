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
