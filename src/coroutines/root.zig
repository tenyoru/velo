//! Stackless state machines: `pub fn poll(self: *Self, task: *Task) Poll(T)`.
//! Keep suspended state/children in fields; arrange `task.wake()` before returning `.pending`.
//! Polling is single-shot unless explicitly documented reusable; never poll after `.ready`,
//! including an error result. `Frame.getResult()` remains repeatable.
//! `cancel()` must be idempotent and valid before polling. Cancellation is cooperative:
//! `Frame.cancel()` calls optional `cancel()` and wakes; drain to ready before freeing borrows.
//! Custom wake sources must unregister before task completion and count their borrows in
//! `task.outstanding_io` until ready is consumed, including completed, unconsumed results.
//! Pin primitives, waiters and channel storage until ready is consumed.
//! Granted permits/values win cancellation; clean them up.
//! Primitives are single-threaded; cross-thread producers use `loop.control.post`.
//! For children run in sequence, `Machine` (machine.zig) derives `poll` and `cancel`.

const std = @import("std");
const assert = std.debug.assert;

pub const Executor = @import("Executor.zig");
pub const Task = Executor.Task;
pub const Event = @import("Event.zig");
pub const Semaphore = @import("Semaphore.zig");
pub const Channel = @import("Channel.zig").Channel;
pub const Machine = @import("machine.zig").Machine;
pub const Next = @import("machine.zig").Next;

pub fn Poll(comptime T: type) type {
    return union(enum) {
        pending,
        ready: T,

        /// Ready value or null: `const v = child.poll(task).value() orelse return .pending;`.
        pub fn value(self: @This()) ?T {
            return switch (self) {
                .pending => null,
                .ready => |v| v,
            };
        }
    };
}

/// Ready type of a coroutine: `T` in `poll(*C, *Task) Poll(T)`.
pub fn ResultOf(comptime C: type) type {
    const P = @typeInfo(@TypeOf(C.poll)).@"fn".return_type.?;
    const T = @FieldType(P, "ready");

    if (P != Poll(T)) @compileError(@typeName(C) ++ ".poll must return Poll(T)");

    return T;
}

/// Pin from `spawn` until `result` is set: executor and wakers borrow this frame.
pub fn Frame(comptime C: type) type {
    const T = ResultOf(C);

    return struct {
        task: Task = .{
            .poll_fn = pollTask,
            .cancel_fn = if (@hasDecl(C, "cancel")) cancelTask else null,
        },
        coroutine: C,
        result: ?T = null,

        const Self = @This();
        pub const Result = T;

        /// Move freely until spawned; pin afterwards.
        pub fn init(coroutine: C) Self {
            return .{ .coroutine = coroutine };
        }

        pub fn cancel(self: *Self) void {
            self.task.cancel();
        }

        /// Requires completion; does not wait for or drive the coroutine.
        pub fn getResult(self: *const Self) T {
            return self.result.?;
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
    const AT = ResultOf(A);
    const BT = ResultOf(B);

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
            assert(self.a_result == .pending or self.b_result == .pending);
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

/// First observed ready (success or error) wins; same-poll ties go to `a`.
/// Polls both before canceling.
/// Uses `Join`'s pinning/cancellation contract; cancel and drain the unfinished loser.
/// Returns both results unchanged; caller must clean up successful losing resources.
pub fn Race(comptime A: type, comptime B: type) type {
    const AT = ResultOf(A);
    const BT = ResultOf(B);

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
            assert(self.a_result == .pending or self.b_result == .pending);
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
    _ = @import("machine.zig");
}
