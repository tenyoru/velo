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
    try std.testing.expectError(error.Canceled, b.result.?);
    try std.testing.expectEqual(null, a.result);

    try std.testing.expect(ex.runOne());
    try a.result.?;
    try std.testing.expectEqual(null, c.result);

    try std.testing.expect(ex.runOne());
    try c.result.?;

    for ([_]*co.Task{ &a.task, &b.task, &c.task }) |task| {
        try std.testing.expectEqual(0, task.outstanding_io);
    }

    var next: co.Frame(Wait) = .{ .coroutine = event.wait() };

    ex.spawn(&next.task);
    ex.run();

    try std.testing.expectEqual(null, next.result);

    event.set();
    ex.run();

    try next.result.?;

    var immediate: co.Frame(Wait) = .{ .coroutine = event.wait() };

    ex.spawn(&immediate.task);
    ex.run();

    try immediate.result.?;
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

    try std.testing.expectError(error.Canceled, parent.result.?);
    try std.testing.expectEqual(0, parent.task.outstanding_io);

    inline for (.{ false, true }) |signaled| {
        event.signaled = signaled;
        var frame: co.Frame(Wait) = .{ .coroutine = event.wait() };

        frame.cancel();
        ex.spawn(&frame.task);
        ex.run();

        try std.testing.expectError(error.Canceled, frame.result.?);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}
