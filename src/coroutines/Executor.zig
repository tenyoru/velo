//! Single-threaded; scale with one executor per core.

const std = @import("std");
const assert = std.debug.assert;
const coroutines = @import("root.zig");
const states = @import("../state.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const Executor = @This();

thread_guard: ThreadGuard = .{},
ready: std.DoublyLinkedList = .{},
all_tasks: std.DoublyLinkedList = .{},

/// Type-erased coroutine handle borrowed by wakers.
pub const Task = struct {
    poll_fn: *const fn (*Task) bool,
    executor: ?*Executor = null,
    node: std.DoublyLinkedList.Node = .{},
    all_node: std.DoublyLinkedList.Node = .{},
    registered: bool = false,
    state: states.Task = .idle,
    cancel_requested: bool = false,
    cancel_fn: ?*const fn (*Task) void = null,
    /// Borrowed waits/work, including completed results not yet consumed.
    outstanding_io: usize = 0,

    /// Keep driving after cancel; callbacks must not run the executor or free registered frames.
    pub fn cancel(task: *Task) void {
        if (task.state == .done or task.cancel_requested) return;

        const executor: ?*Executor = if (ThreadGuard.enabled) task.executor else null;
        const entered = if (executor) |ex| ex.thread_guard.enter() else false;
        defer if (executor) |ex| ex.thread_guard.leave(entered);

        task.cancel_requested = true;
        if (task.cancel_fn) |cancel_fn| cancel_fn(task);
        if (task.executor != null and task.state != .done) task.wake();
    }

    pub fn wake(task: *Task) void {
        const executor: ?*Executor = if (ThreadGuard.enabled) task.executor else null;
        const entered = if (executor) |ex| ex.thread_guard.enter() else false;
        defer if (executor) |ex| ex.thread_guard.leave(entered);

        switch (task.state) {
            .idle => {
                task.state = .scheduled;
                task.executor.?.ready.append(&task.node);
            },
            .scheduled => {},
            .done => unreachable, // stale waker: the frame finished and may be reused
        }
    }
};

pub fn spawn(ex: *Executor, task: *Task) void {
    const entered = if (ThreadGuard.enabled) ex.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) ex.thread_guard.leave(entered);

    assert(task.executor == null);
    assert(!task.registered and task.state == .idle);

    task.executor = ex;
    ex.all_tasks.append(&task.all_node);
    task.registered = true;
    task.wake();
}

/// Caller must still drain all unfinished tasks.
pub fn cancelAll(ex: *Executor) void {
    const entered = if (ThreadGuard.enabled) ex.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) ex.thread_guard.leave(entered);

    var next = ex.all_tasks.first;

    while (next) |node| {
        const task: *Task = @alignCast(@fieldParentPtr("all_node", node));

        task.cancel();

        // Read after the callback so tasks it spawns are canceled too; only runOne unlinks.
        next = node.next;
    }
}

pub fn run(ex: *Executor) void {
    const entered = if (ThreadGuard.enabled) ex.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) ex.thread_guard.leave(entered);

    while (ex.runOne()) {}
}

pub fn runOne(ex: *Executor) bool {
    const entered = if (ThreadGuard.enabled) ex.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) ex.thread_guard.leave(entered);

    const node = ex.ready.popFirst() orelse return false;
    const task: *Task = @alignCast(@fieldParentPtr("node", node));

    task.state = .idle;

    if (task.poll_fn(task)) {
        assert(task.outstanding_io == 0);

        // A task can wake itself and still finish, e.g. by dropping a yielded child.
        if (task.state == .scheduled) ex.ready.remove(&task.node);

        if (task.registered) {
            ex.all_tasks.remove(&task.all_node);
            task.registered = false;
        }

        task.state = .done;
    }

    return true;
}

test "wakes are deduplicated and dropped on completion" {
    const Probe = struct {
        polls: u32 = 0,
        finish: bool = false,

        pub fn poll(self: *@This(), task: *Task) coroutines.Poll(u32) {
            self.polls += 1;
            if (!self.finish) return .pending;

            task.wake();
            return .{ .ready = self.polls };
        }
    };

    var ex: Executor = .{};
    var probe: coroutines.Frame(Probe) = .{ .coroutine = .{} };

    ex.spawn(&probe.task);
    ex.run();

    probe.task.wake();
    probe.task.wake();
    ex.run();

    try std.testing.expectEqual(2, probe.coroutine.polls);

    probe.coroutine.finish = true;
    probe.task.wake();
    ex.run();

    try std.testing.expectEqual(3, probe.getResult());
    try std.testing.expectEqual(null, ex.ready.first);
}

test "cancelAll covers parked and scheduled tasks and completion unregisters" {
    const Probe = struct {
        canceled: bool = false,
        cancel_calls: usize = 0,

        pub fn poll(self: *@This(), _: *Task) coroutines.Poll(error{Canceled}!void) {
            return if (self.canceled) .{ .ready = error.Canceled } else .pending;
        }

        pub fn cancel(self: *@This()) void {
            self.cancel_calls += 1;
            self.canceled = true;
        }
    };

    var ex: Executor = .{};
    var a: coroutines.Frame(Probe) = .{ .coroutine = .{} };
    var b: coroutines.Frame(Probe) = .{ .coroutine = .{} };
    var c: coroutines.Frame(Probe) = .{ .coroutine = .{} };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.run();
    ex.spawn(&c.task);

    try std.testing.expectEqual(3, ex.all_tasks.len());

    b.cancel();
    try std.testing.expect(ex.runOne());
    try std.testing.expect(ex.runOne());
    c.task.wake();

    try std.testing.expect(!b.task.registered);
    try std.testing.expectEqual(2, ex.all_tasks.len());

    ex.cancelAll();
    ex.cancelAll();
    ex.run();

    for ([_]*coroutines.Frame(Probe){ &a, &b, &c }) |frame| {
        frame.cancel();

        try std.testing.expectError(error.Canceled, frame.getResult());
        try std.testing.expectEqual(1, frame.coroutine.cancel_calls);
        try std.testing.expect(!frame.task.registered);
    }

    try std.testing.expectEqual(null, ex.all_tasks.first);
    try std.testing.expectEqual(null, ex.ready.first);
}

test "pre-spawn cancellation and manually assigned executor do not corrupt registry" {
    const Probe = struct {
        fn poll(task: *Task) bool {
            return task.cancel_requested;
        }
    };

    var ex: Executor = .{};
    var pre: Task = .{ .poll_fn = Probe.poll };

    pre.cancel();
    pre.cancel();

    try std.testing.expectEqual(null, ex.ready.first);

    ex.spawn(&pre);
    ex.run();

    try std.testing.expectEqual(.done, pre.state);
    try std.testing.expect(!pre.registered);

    var manual: Task = .{ .poll_fn = Probe.poll, .executor = &ex };

    manual.cancel();
    ex.run();

    try std.testing.expectEqual(.done, manual.state);
    try std.testing.expectEqual(null, ex.all_tasks.first);
}

test "cancelAll reaches a task spawned by the last cancel callback" {
    const Spawner = struct {
        var ex: Executor = .{};
        var child: Task = .{ .poll_fn = poll };

        fn poll(task: *Task) bool {
            return task.cancel_requested;
        }

        fn spawnChild(_: *Task) void {
            ex.spawn(&child);
        }
    };

    var parent: Task = .{ .poll_fn = Spawner.poll, .cancel_fn = Spawner.spawnChild };

    Spawner.ex.spawn(&parent);
    Spawner.ex.cancelAll();

    try std.testing.expect(Spawner.child.cancel_requested);

    Spawner.ex.run();

    try std.testing.expectEqual(null, Spawner.ex.all_tasks.first);
}
