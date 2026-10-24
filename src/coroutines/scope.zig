//! `Scope(C)`: structured concurrency over caller-owned slots. Each child runs as its own
//! task on one executor, so a wake polls only that child.
//!
//! Children return `void` or `E!void` and report anything else through their own fields;
//! handle per-child failures inside the child when siblings should keep running.
//! The first child error cancels the scope and is returned by `wait`. Cancellation is
//! permanent: it cancels running children, fails queued reservations and rejects `spawn`.
//! `wait` completes when no slot is reserved or running; canceling it cancels the scope and
//! keeps draining. Pin the scope and slots while any slot is busy or waiter is queued.
//! Single-threaded; child cancel callbacks must not spawn, start or release.

const std = @import("std");
const assert = std.debug.assert;
const co = @import("root.zig");
const states = @import("../state.zig");

const Task = co.Task;
const Node = std.DoublyLinkedList.Node;

pub fn Scope(comptime C: type) type {
    const R = co.ResultOf(C);
    const unsupported = "Scope children return void or E!void; keep other results in the child: ";
    const E = switch (@typeInfo(R)) {
        .void => error{},
        .error_union => |info| if (info.payload == void) info.error_set else @compileError(unsupported ++ @typeName(C)),
        else => @compileError(unsupported ++ @typeName(C)),
    };
    if (!@hasDecl(C, "cancel")) @compileError("Scope children need `cancel`: " ++ @typeName(C));

    return struct {
        executor: *co.Executor,
        free: std.DoublyLinkedList = .{},
        running: std.DoublyLinkedList = .{},
        reserves: std.DoublyLinkedList = .{},
        waits: std.DoublyLinkedList = .{},
        /// Reserved or running slots.
        active: usize = 0,
        failure: ?E = null,
        canceled: bool = false,

        const Self = @This();
        pub const Error = E || error{Canceled};

        /// Caller-owned child storage; contents are managed by the scope.
        pub const Slot = struct {
            frame: co.Frame(Member) = undefined,
            node: Node = .{},
            state: enum { free, reserved, running } = .free,
        };

        const Member = struct {
            child: C,
            scope: *Self,
            slot: *Slot,

            pub fn poll(self: *Member, task: *Task) co.Poll(void) {
                const result = self.child.poll(task).value() orelse return .pending;

                self.scope.finished(self.slot, result);
                return .{ .ready = {} };
            }

            pub fn cancel(self: *Member) void {
                self.child.cancel();
            }
        };

        pub fn init(executor: *co.Executor, slots: []Slot) Self {
            var self: Self = .{ .executor = executor };
            for (slots) |*slot| {
                slot.* = .{};
                self.free.append(&slot.node);
            }
            return self;
        }

        pub fn deinit(self: *Self) void {
            assert(self.active == 0);
            assert(self.reserves.first == null and self.waits.first == null);
        }

        /// Starts `child` in a free slot; on error the caller still owns `child`.
        pub fn spawn(self: *Self, child: C) error{ Full, Canceled }!void {
            if (self.canceled) return error.Canceled;

            self.start(self.take() orelse return error.Full, child);
        }

        /// Waits for a free slot; a granted slot wins cancellation and must be started or released.
        pub fn reserve(self: *Self) Reserve {
            return .{ .scope = self };
        }

        /// Starts `child` in a reserved slot. After cancellation the child starts canceled,
        /// so it can still clean up what it owns.
        pub fn start(self: *Self, slot: *Slot, child: C) void {
            assert(slot.state == .reserved);
            slot.state = .running;
            slot.frame = .init(.{ .child = child, .scope = self, .slot = slot });
            self.running.append(&slot.node);
            self.executor.spawn(&slot.frame.task);

            if (self.canceled) slot.frame.cancel();
        }

        /// Returns an unused reservation.
        pub fn release(self: *Self, slot: *Slot) void {
            assert(slot.state == .reserved);
            self.vacate(slot);
        }

        pub fn wait(self: *Self) Wait {
            return .{ .scope = self };
        }

        pub fn cancel(self: *Self) void {
            if (self.canceled) return;
            self.canceled = true;

            while (self.reserves.popFirst()) |node| {
                const waiter: *Reserve = @alignCast(@fieldParentPtr("node", node));

                waiter.state = .canceled;
                waiter.task.?.wake();
            }

            var next = self.running.first;
            while (next) |node| {
                next = node.next;
                const slot: *Slot = @fieldParentPtr("node", node);

                slot.frame.cancel();
            }
        }

        fn take(self: *Self) ?*Slot {
            const slot: *Slot = @fieldParentPtr("node", self.free.popFirst() orelse return null);

            assert(slot.state == .free);
            slot.state = .reserved;
            self.active += 1;
            return slot;
        }

        fn finished(self: *Self, slot: *Slot, result: R) void {
            assert(slot.state == .running);

            // Leave running first: canceling siblings must not reach this completed child.
            self.running.remove(&slot.node);

            if (R != void) {
                result catch |err| {
                    if (self.failure == null) self.failure = err;
                    self.cancel();
                };
            }

            // Reuse waits for a later task poll, after the executor has retired this frame.
            self.vacate(slot);
        }

        fn vacate(self: *Self, slot: *Slot) void {
            if (self.reserves.popFirst()) |node| {
                const waiter: *Reserve = @alignCast(@fieldParentPtr("node", node));

                slot.state = .reserved;
                waiter.slot = slot;
                waiter.state = .granted;
                waiter.task.?.wake();
                return;
            }

            slot.state = .free;
            self.free.append(&slot.node);
            self.active -= 1;
            if (self.active != 0) return;

            while (self.waits.popFirst()) |node| {
                const waiter: *Wait = @alignCast(@fieldParentPtr("node", node));

                waiter.state = .granted;
                waiter.task.?.wake();
            }
        }

        pub const Reserve = struct {
            scope: *Self,
            node: Node = .{},
            task: ?*Task = null,
            slot: ?*Slot = null,
            state: states.Wait = .idle,

            pub fn poll(self: *Reserve, task: *Task) co.Poll(error{Canceled}!*Slot) {
                assert(self.state != .done);
                if (self.task) |borrowed| assert(borrowed == task);
                if (task.cancel_requested) self.cancel();

                if (self.state == .idle) {
                    if (self.scope.canceled) {
                        self.state = .canceled;
                    } else if (self.scope.take()) |slot| {
                        self.slot = slot;
                        self.state = .granted;
                    } else {
                        self.task = task;
                        task.outstanding_io += 1;
                        self.scope.reserves.append(&self.node);
                        self.state = .waiting;
                    }
                }

                if (self.state == .waiting) return .pending;
                if (self.task != null) task.outstanding_io -= 1;

                const canceled = self.state == .canceled;
                self.state = .done;

                return .{ .ready = if (canceled) error.Canceled else self.slot.? };
            }

            pub fn cancel(self: *Reserve) void {
                switch (self.state) {
                    .idle => self.state = .canceled,
                    .waiting => {
                        self.scope.reserves.remove(&self.node);
                        self.state = .canceled;
                        self.task.?.wake();
                    },
                    .granted, .canceled, .done => {},
                }
            }
        };

        /// Completes once no slot is busy: the first child error, `Canceled` after
        /// cancellation, or success.
        pub const Wait = struct {
            scope: *Self,
            node: Node = .{},
            task: ?*Task = null,
            state: states.Wait = .idle,

            pub fn poll(self: *Wait, task: *Task) co.Poll(Error!void) {
                assert(self.state != .done);
                if (self.task) |borrowed| assert(borrowed == task);
                if (task.cancel_requested) self.cancel();

                if (self.state == .idle) {
                    if (self.scope.active == 0) {
                        self.state = .granted;
                    } else {
                        self.task = task;
                        task.outstanding_io += 1;
                        self.scope.waits.append(&self.node);
                        self.state = .waiting;
                    }
                }

                if (self.state == .waiting) return .pending;
                if (self.task != null) task.outstanding_io -= 1;
                self.state = .done;

                if (self.scope.failure) |err| return .{ .ready = err };
                return .{ .ready = if (self.scope.canceled) error.Canceled else {} };
            }

            /// Cancels the scope; the wait still completes only after children drain.
            pub fn cancel(self: *Wait) void {
                self.scope.cancel();
            }
        };
    };
}

const Child = struct {
    wait: co.Event.Wait,
    fail: bool = false,
    polls: *usize,

    pub fn poll(self: *Child, task: *Task) co.Poll(error{ Canceled, Failed }!void) {
        self.polls.* += 1;
        if (self.fail) return .{ .ready = error.Failed };

        const result = self.wait.poll(task).value() orelse return .pending;
        return .{ .ready = result };
    }

    pub fn cancel(self: *Child) void {
        self.wait.cancel();
    }
};

test "scope children wake independently, bound by slots, and wait drains them" {
    var event: co.Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var slots: [2]Scope(Child).Slot = undefined;
    var scope: Scope(Child) = .init(&ex, &slots);
    defer scope.deinit();

    var polls: [3]usize = @splat(0);
    var other: co.Event = .{};
    defer other.deinit();

    try scope.spawn(.{ .wait = event.wait(), .polls = &polls[0] });
    try scope.spawn(.{ .wait = other.wait(), .polls = &polls[1] });
    try std.testing.expectError(error.Full, scope.spawn(.{ .wait = event.wait(), .polls = &polls[2] }));

    var waiter: co.Frame(Scope(Child).Wait) = .init(scope.wait());

    ex.spawn(&waiter.task);
    ex.run();
    event.set();
    ex.run();

    try std.testing.expectEqual(null, waiter.result);
    try std.testing.expectEqual(1, scope.active);
    try std.testing.expectEqual(.{ 2, 1 }, .{ polls[0], polls[1] });

    other.set();
    ex.run();

    try waiter.getResult();
    try std.testing.expectEqual(0, scope.active);

    // Freed slots are reused.
    try scope.spawn(.{ .wait = event.wait(), .polls = &polls[2] });
    ex.run();
    try std.testing.expectEqual(0, scope.active);
}

test "scope first child error cancels siblings and is returned once drained" {
    var event: co.Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var slots: [3]Scope(Child).Slot = undefined;
    var scope: Scope(Child) = .init(&ex, &slots);
    defer scope.deinit();

    var polls: [3]usize = @splat(0);

    try scope.spawn(.{ .wait = event.wait(), .polls = &polls[0] });
    try scope.spawn(.{ .wait = event.wait(), .polls = &polls[1] });

    var waiter: co.Frame(Scope(Child).Wait) = .init(scope.wait());

    ex.spawn(&waiter.task);
    ex.run();

    try scope.spawn(.{ .wait = event.wait(), .fail = true, .polls = &polls[2] });
    ex.run();

    try std.testing.expectError(error.Failed, waiter.getResult());
    try std.testing.expectEqual(null, event.waiters.first);
    try std.testing.expectError(error.Canceled, scope.spawn(.{ .wait = event.wait(), .polls = &polls[0] }));
}

test "scope reservations are FIFO, win cancellation, and start canceled children" {
    var event: co.Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var slots: [1]Scope(Child).Slot = undefined;
    var scope: Scope(Child) = .init(&ex, &slots);
    defer scope.deinit();

    var polls: [2]usize = @splat(0);

    try scope.spawn(.{ .wait = event.wait(), .polls = &polls[0] });

    var first: co.Frame(Scope(Child).Reserve) = .init(scope.reserve());
    var second: co.Frame(Scope(Child).Reserve) = .init(scope.reserve());

    ex.spawn(&first.task);
    ex.spawn(&second.task);
    ex.run();
    event.set();
    ex.run();

    const slot = try first.getResult();
    try std.testing.expectEqual(null, second.result);
    try std.testing.expectEqual(1, scope.active);

    scope.cancel();
    ex.run();

    try std.testing.expectError(error.Canceled, second.getResult());

    event.reset();
    scope.start(slot, .{ .wait = event.wait(), .polls = &polls[1] });
    ex.run();

    try std.testing.expectEqual(1, polls[1]);
    try std.testing.expectEqual(0, scope.active);

    var waiter: co.Frame(Scope(Child).Wait) = .init(scope.wait());

    ex.spawn(&waiter.task);
    ex.run();

    try std.testing.expectError(error.Canceled, waiter.getResult());
}

test "scope released reservation and canceled wait keep draining" {
    var event: co.Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var slots: [2]Scope(Child).Slot = undefined;
    var scope: Scope(Child) = .init(&ex, &slots);
    defer scope.deinit();

    var polls: usize = 0;
    var reserved: co.Frame(Scope(Child).Reserve) = .init(scope.reserve());

    ex.spawn(&reserved.task);
    ex.run();
    try scope.spawn(.{ .wait = event.wait(), .polls = &polls });

    var waiter: co.Frame(Scope(Child).Wait) = .init(scope.wait());

    ex.spawn(&waiter.task);
    ex.run();
    waiter.cancel();
    ex.run();

    // The canceled child drained; the held reservation still blocks completion.
    try std.testing.expectEqual(null, waiter.result);
    try std.testing.expectEqual(null, event.waiters.first);
    try std.testing.expectEqual(1, waiter.task.outstanding_io);

    scope.release(try reserved.getResult());
    ex.run();

    try std.testing.expectError(error.Canceled, waiter.getResult());
    try std.testing.expectEqual(0, waiter.task.outstanding_io);
}
