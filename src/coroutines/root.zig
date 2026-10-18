//! Stackless state machines: `pub fn poll(self: *Self, task: *Task) Poll(T)`.
//! Keep suspended state/children in fields; arrange `task.wake()` before returning `.pending`.
//! Cancellation is cooperative: `Frame.cancel()` calls optional `cancel()` and wakes;
//! keep polling to ready before freeing borrowed storage.
//! Event/semaphore/channel waits are single-shot. Pin primitives, waiters and channel
//! storage until ready is consumed. Granted permits/values win cancellation; clean them up.
//! Primitives are single-threaded; cross-thread producers use `loop.control.post`.

const std = @import("std");
const assert = std.debug.assert;

pub const Executor = @import("Executor.zig");
pub const Task = Executor.Task;
pub const Event = @import("Event.zig");
pub const Semaphore = @import("Semaphore.zig");
pub const Channel = @import("Channel.zig").Channel;

pub fn Poll(comptime T: type) type {
    return union(enum) { pending, ready: T };
}

fn ResultType(comptime C: type) type {
    const P = @typeInfo(@TypeOf(C.poll)).@"fn".return_type.?;
    const T = @FieldType(P, "ready");

    if (P != Poll(T)) @compileError(@typeName(C) ++ ".poll must return Poll(T)");

    return T;
}

/// Pin from `spawn` until `result` is set: executor and wakers borrow this frame.
pub fn Frame(comptime C: type) type {
    const T = ResultType(C);

    return struct {
        task: Task = .{
            .poll_fn = pollTask,
            .cancel_fn = if (@hasDecl(C, "cancel")) cancelTask else null,
        },
        coroutine: C,
        result: ?T = null,

        const Self = @This();

        pub fn cancel(self: *Self) void {
            self.task.cancel();
        }

        fn cancelTask(task: *Task) void {
            const self: *Self = @alignCast(@fieldParentPtr("task", task));

            self.coroutine.cancel();
        }

        fn pollTask(task: *Task) bool {
            const self: *Self = @alignCast(@fieldParentPtr("task", task));
            assert(self.result == null);

            switch (self.coroutine.poll(task)) {
                .pending => return false,
                .ready => |value| {
                    self.result = value;
                    return true;
                },
            }
        }
    };
}

/// Pin until both finish; errors do not short-circuit, and the caller owns both results.
/// Children require idempotent `pub fn cancel(*Self) void`, valid before polling.
/// Cancellation requests unfinished children; keep polling to ready.
pub fn Join(comptime A: type, comptime B: type) type {
    const AT = ResultType(A);
    const BT = ResultType(B);

    return struct {
        a: A,
        b: B,
        a_result: Poll(AT) = .pending,
        b_result: Poll(BT) = .pending,
        task: ?*Task = null,
        cancel_requested: bool = false,

        const Self = @This();
        pub const Result = struct { a: AT, b: BT };

        pub fn poll(self: *Self, task: *Task) Poll(Result) {
            self.task = task;

            if (self.a_result == .pending) self.a_result = self.a.poll(task);
            if (self.b_result == .pending) self.b_result = self.b.poll(task);
            if (self.a_result == .pending or self.b_result == .pending) return .pending;

            return .{
                .ready = .{
                    .a = self.a_result.ready,
                    .b = self.b_result.ready,
                },
            };
        }

        pub fn cancel(self: *Self) void {
            if (self.cancel_requested) return;

            self.cancel_requested = true;
            if (self.a_result == .pending) self.a.cancel();
            if (self.b_result == .pending) self.b.cancel();

            if (self.a_result == .pending or self.b_result == .pending) {
                if (self.task) |task| task.wake();
            }
        }
    };
}

/// First observed ready wins; same-poll ties go to `a`. Polls both before canceling.
/// Uses `Join`'s pinning/cancellation contract; cancel and drain the unfinished loser.
/// Returns both results unchanged; caller must clean up successful losing resources.
pub fn Race(comptime A: type, comptime B: type) type {
    const AT = ResultType(A);
    const BT = ResultType(B);

    return struct {
        a: A,
        b: B,
        a_result: Poll(AT) = .pending,
        b_result: Poll(BT) = .pending,
        winner: ?Winner = null,
        task: ?*Task = null,
        cancel_requested: bool = false,

        const Self = @This();
        const Winner = enum { a, b };
        pub const Result = struct { winner: Winner, a: AT, b: BT };

        pub fn poll(self: *Self, task: *Task) Poll(Result) {
            self.task = task;

            if (self.a_result == .pending) {
                self.a_result = self.a.poll(task);
                if (self.a_result == .ready and self.winner == null) self.winner = .a;
            }

            if (self.b_result == .pending) {
                self.b_result = self.b.poll(task);
                if (self.b_result == .ready and self.winner == null) self.winner = .b;
            }

            if (self.winner != null) self.cancel();
            if (self.a_result == .pending or self.b_result == .pending) return .pending;

            return .{
                .ready = .{
                    .winner = self.winner.?,
                    .a = self.a_result.ready,
                    .b = self.b_result.ready,
                },
            };
        }

        pub fn cancel(self: *Self) void {
            if (self.cancel_requested) return;

            self.cancel_requested = true;
            if (self.a_result == .pending) self.a.cancel();
            if (self.b_result == .pending) self.b.cancel();

            if (self.a_result == .pending or self.b_result == .pending) {
                if (self.task) |task| task.wake();
            }
        }
    };
}

/// Reusable one-poll suspension, queued behind ready tasks.
/// Cancellation completes normally without suspending again.
pub const Yield = struct {
    yielded: bool = false,

    pub fn poll(self: *Yield, task: *Task) Poll(void) {
        if (self.yielded or task.cancel_requested) {
            self.yielded = false;
            return .{ .ready = {} };
        }

        self.yielded = true;
        task.wake();
        return .pending;
    }

    pub fn cancel(self: *Yield) void {
        self.yielded = true;
    }
};

test {
    std.testing.refAllDecls(@This());
    _ = @import("test.zig");
}
