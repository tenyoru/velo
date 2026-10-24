//! Single-threaded; scale with one executor per core.

const std = @import("std");
const assert = std.debug.assert;
const coroutines = @import("root.zig");
const states = @import("../state.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const Executor = @This();

/// Safe builds keep coroutine type names on tasks for diagnostics; others store nothing.
pub const named = std.debug.runtime_safety;

thread_guard: ThreadGuard = .{},
ready: std.DoublyLinkedList = .{},
all_tasks: std.DoublyLinkedList = .{},
cancel_next: ?*std.DoublyLinkedList.Node = null,

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
    /// Set by `Frame` to its coroutine type name.
    name: if (named) []const u8 else void = if (named) "task" else {},

    pub fn getName(task: *const Task) []const u8 {
        return if (named) task.name else "task";
    }

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
            // Stale waker: the frame finished and may be reused.
            .done => if (named)
                std.debug.panic("velo: wake of finished task {s}; a waker outlived its wait", .{task.name})
            else
                unreachable,
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
    if (ex.cancel_next == null) ex.cancel_next = &task.all_node;
    task.registered = true;
    task.wake();
}

/// Caller must still drain all unfinished tasks.
pub fn cancelAll(ex: *Executor) void {
    const entered = if (ThreadGuard.enabled) ex.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) ex.thread_guard.leave(entered);

    // Persist progress and advance before callbacks can append or reenter.
    while (ex.cancel_next) |node| {
        ex.cancel_next = node.next;
        const task: *Task = @alignCast(@fieldParentPtr("all_node", node));

        task.cancel();
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
        if (task.outstanding_io != 0) {
            if (named) std.debug.panic(
                "velo: task {s} finished with {d} unconsumed waits; poll every child to ready first",
                .{ task.name, task.outstanding_io },
            ) else unreachable;
        }

        // A task can wake itself and still finish, e.g. by dropping a yielded child.
        if (task.state == .scheduled) ex.ready.remove(&task.node);

        if (task.registered) {
            if (ex.cancel_next == &task.all_node) ex.cancel_next = task.all_node.next;
            ex.all_tasks.remove(&task.all_node);
            task.registered = false;
        }

        task.state = .done;
    }

    return true;
}

/// Writes one line per registered task: name, state, outstanding waits and cancellation.
pub fn writeTasks(ex: *const Executor, w: *std.Io.Writer) std.Io.Writer.Error!usize {
    var count: usize = 0;
    var node = ex.all_tasks.first;

    while (node) |current| : (node = current.next) {
        const task: *const Task = @alignCast(@fieldParentPtr("all_node", current));

        try w.print("  {s}: {s}, {d} outstanding{s}\n", .{
            task.getName(),
            @tagName(task.state),
            task.outstanding_io,
            if (task.cancel_requested) ", cancel requested" else "",
        });
        count += 1;
    }

    return count;
}

/// Prints `writeTasks` to stderr, e.g. while diagnosing a stall or an undrained loop.
pub fn dumpTasks(ex: *const Executor) void {
    var buffer: [256]u8 = undefined;
    const stderr = std.debug.lockStderr(&buffer);
    defer std.debug.unlockStderr();

    const w = &stderr.file_writer.interface;
    const count = ex.writeTasks(w) catch return;
    if (count == 0) w.writeAll("  (no registered tasks)\n") catch return;
    w.flush() catch return;
}

test "safe builds name frames and report registered tasks" {
    var event: coroutines.Event = .{};
    defer event.deinit();

    var ex: Executor = .{};
    var waiting: coroutines.Frame(coroutines.Event.Wait) = .init(event.wait());
    const name = if (named) @typeName(coroutines.Event.Wait) else "task";

    try std.testing.expectEqualStrings(name, waiting.task.getName());

    ex.spawn(&waiting.task);
    ex.run();
    waiting.cancel();

    var buffer: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buffer);

    try std.testing.expectEqual(1, try ex.writeTasks(&w));
    try std.testing.expectEqualStrings(
        "  " ++ name ++ ": scheduled, 1 outstanding, cancel requested\n",
        w.buffered(),
    );

    ex.run();
    w = .fixed(&buffer);

    try std.testing.expectEqual(0, try ex.writeTasks(&w));
    try std.testing.expectError(error.Canceled, waiting.getResult());
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

test "cancelAll skips completed tasks and accepts new work after exhausted passes" {
    const Probe = struct {
        finish: bool = false,
        cancel_calls: usize = 0,

        pub fn poll(self: *@This(), _: *Task) coroutines.Poll(void) {
            return if (self.finish) .{ .ready = {} } else .pending;
        }

        pub fn cancel(self: *@This()) void {
            self.cancel_calls += 1;
        }
    };

    var ex: Executor = .{};
    var frames: [4]coroutines.Frame(Probe) = @splat(.{ .coroutine = .{} });
    frames[0].coroutine.finish = true;
    frames[2].coroutine.finish = true;

    ex.spawn(&frames[0].task);
    ex.spawn(&frames[1].task);
    ex.run();
    ex.cancelAll();
    ex.cancelAll();

    try std.testing.expectEqual(0, frames[0].coroutine.cancel_calls);
    try std.testing.expectEqual(1, frames[1].coroutine.cancel_calls);
    try std.testing.expect(frames[1].task.registered);

    ex.spawn(&frames[2].task);
    ex.run();
    try std.testing.expect(frames[2].result != null);
    try std.testing.expectEqual(0, frames[2].coroutine.cancel_calls);

    ex.spawn(&frames[3].task);
    try std.testing.expect(!frames[3].task.cancel_requested);
    ex.cancelAll();
    ex.cancelAll();

    try std.testing.expectEqual(1, frames[1].coroutine.cancel_calls);
    try std.testing.expectEqual(1, frames[3].coroutine.cancel_calls);
    for ([_]usize{ 1, 3 }) |i| {
        try std.testing.expect(frames[i].task.registered);
        frames[i].coroutine.finish = true;
        frames[i].task.wake();
    }
    ex.run();

    try std.testing.expectEqual(null, ex.all_tasks.first);
}

test "nested cancelAll reaches a task spawned by the last cancel callback" {
    const Spawner = struct {
        var ex: Executor = .{};
        var child: Task = .{ .poll_fn = poll };

        fn poll(task: *Task) bool {
            return task.cancel_requested;
        }

        fn spawnChild(_: *Task) void {
            ex.spawn(&child);
        }

        fn cancelNested(_: *Task) void {
            ex.cancelAll();
        }
    };

    var first: Task = .{ .poll_fn = Spawner.poll, .cancel_fn = Spawner.cancelNested };
    var parent: Task = .{ .poll_fn = Spawner.poll, .cancel_fn = Spawner.spawnChild };

    Spawner.ex.spawn(&first);
    Spawner.ex.spawn(&parent);
    Spawner.ex.cancelAll();

    for ([_]*Task{ &first, &parent, &Spawner.child }) |task| {
        try std.testing.expect(task.cancel_requested);
        try std.testing.expect(task.registered);
    }

    Spawner.ex.run();

    try std.testing.expectEqual(null, Spawner.ex.all_tasks.first);
}
