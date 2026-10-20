//! Cross-thread posts run on the loop thread; retain before producing, release after the last post.
//! Keep nodes/payloads alive and free of concurrent mutation until their callback returns.
//! Posts from one producer keep their order; re-posting a still-queued node coalesces.

const std = @import("std");

const Executor = @import("../coroutines/Executor.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const Control = @This();

head: std.atomic.Value(?*Post) = .init(null),
references: std.atomic.Value(usize) = .init(0),
stop_requested: std.atomic.Value(bool) = .init(false),
shutdown_requested: std.atomic.Value(bool) = .init(false),
signal: *const fn (*Control) void,
registrations: std.DoublyLinkedList = .{},
cancel_next: ?*std.DoublyLinkedList.Node = null,

pub const Post = struct {
    callback: *const fn (*Post) void,
    next: ?*Post = null,
    queued: std.atomic.Value(bool) = .init(false),
};

pub const Registration = struct {
    cancel: *const fn (*Registration) void,
    node: std.DoublyLinkedList.Node = .{},
    registered: bool = false,
};

pub fn post(control: *Control, command: *Post) void {
    if (control.push(command)) control.signal(control);
}

pub fn retain(control: *Control) void {
    _ = control.references.fetchAdd(1, .acq_rel);
}

pub fn release(control: *Control) void {
    if (control.drop()) control.signal(control);
}

/// `post` then `release` with at most one wake.
pub fn postRelease(control: *Control, command: *Post) void {
    const first = control.push(command);
    if (control.drop() or first) control.signal(control);
}

// Only the push onto an empty stack must wake the loop: service takes the whole stack.
fn push(control: *Control, command: *Post) bool {
    if (command.queued.swap(true, .acq_rel)) return false;

    var head = control.head.load(.acquire);

    while (true) {
        command.next = head;
        // Never read the node after publishing it: its callback may already run.
        head = control.head.cmpxchgWeak(head, command, .acq_rel, .acquire) orelse return head == null;
    }
}

// Only the last release can make the loop idle.
fn drop(control: *Control) bool {
    const previous = control.references.fetchSub(1, .acq_rel);
    std.debug.assert(previous > 0);

    return previous == 1;
}

/// Stops without releasing live work, even during shutdown; rerun to resume/drain.
pub fn stop(control: *Control) void {
    control.stop_requested.store(true, .release);
    control.signal(control);
}

/// Permanently rejects new operations and requests cancellation of live work.
pub fn shutdown(control: *Control) void {
    control.shutdown_requested.store(true, .release);
    control.signal(control);
}

pub fn isShuttingDown(control: *const Control) bool {
    return control.shutdown_requested.load(.acquire);
}

pub fn hasWork(control: *const Control) bool {
    return control.references.load(.acquire) != 0 or control.head.load(.acquire) != null;
}

/// With no local work, recheck commands published before the final release before exiting.
pub fn idle(control: *Control, executor: *Executor) bool {
    return control.service(executor) or (executor.ready.first == null and !control.hasWork());
}

pub fn register(control: *Control, registration: *Registration) void {
    std.debug.assert(!registration.registered);

    registration.registered = true;
    control.registrations.append(&registration.node);
    if (control.cancel_next == null) control.cancel_next = &registration.node;
}

pub fn unregister(control: *Control, registration: *Registration) void {
    std.debug.assert(registration.registered);

    if (control.cancel_next == &registration.node) control.cancel_next = registration.node.next;
    control.registrations.remove(&registration.node);
    registration.registered = false;
}

pub fn service(control: *Control, executor: *Executor) bool {
    const entered = if (ThreadGuard.enabled) executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) executor.thread_guard.leave(entered);

    var stack = control.head.swap(null, .acq_rel);
    var fifo: ?*Post = null;

    while (stack) |command| {
        stack = command.next;
        command.next = fifo;
        fifo = command;
    }

    while (fifo) |command| {
        fifo = command.next;
        command.queued.store(false, .release);
        command.callback(command);
    }

    if (control.isShuttingDown()) {
        executor.cancelAll();

        // Persist progress and advance before callbacks can append or unregister.
        while (control.cancel_next) |current| {
            control.cancel_next = current.next;
            const registration: *Registration = @fieldParentPtr("node", current);
            registration.cancel(registration);
        }
    }

    return control.stop_requested.swap(false, .acq_rel);
}

pub fn deinit(control: *const Control) void {
    std.debug.assert(!control.hasWork() and control.registrations.first == null);
}

test "shutdown cancellation tolerates registry changes and only visits new admissions" {
    const Probe = struct {
        control: *Control,
        registration: Registration = .{ .cancel = cancel },
        cancel_calls: usize = 0,
        remove_next: ?*@This() = null,
        append: ?*@This() = null,

        fn signal(_: *Control) void {}

        fn cancel(registration: *Registration) void {
            const self: *@This() = @fieldParentPtr("registration", registration);
            self.cancel_calls += 1;
            if (self.remove_next) |next| {
                self.remove_next = null;
                self.control.unregister(&next.registration);
            }
            if (self.append) |next| {
                self.append = null;
                self.control.register(&next.registration);
            }
        }
    };

    var executor: Executor = .{};
    var control: Control = .{ .signal = Probe.signal };
    var probes: [5]Probe = @splat(.{ .control = &control });
    probes[0].remove_next = &probes[1];
    probes[2].append = &probes[3];
    for (probes[0..3]) |*probe| control.register(&probe.registration);

    control.shutdown();
    try std.testing.expect(!control.service(&executor));
    try std.testing.expect(!control.service(&executor));

    for ([_]usize{ 1, 0, 1, 1, 0 }, &probes) |expected, *probe| {
        try std.testing.expectEqual(expected, probe.cancel_calls);
    }
    try std.testing.expect(!probes[1].registration.registered);
    try std.testing.expectEqual(3, control.registrations.len());

    control.register(&probes[4].registration);
    control.register(&probes[1].registration);
    try std.testing.expect(!control.service(&executor));
    try std.testing.expect(!control.service(&executor));
    for (&probes) |*probe| try std.testing.expectEqual(1, probe.cancel_calls);

    control.unregister(&probes[4].registration);
    control.register(&probes[4].registration);
    try std.testing.expect(!control.service(&executor));
    try std.testing.expectEqual(2, probes[4].cancel_calls);
    for (probes[0..4]) |*probe| try std.testing.expectEqual(1, probe.cancel_calls);

    for (&probes) |*probe| control.unregister(&probe.registration);
    control.deinit();
}
