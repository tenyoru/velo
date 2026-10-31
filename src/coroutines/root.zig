//! Stackless state machines: `pub fn poll(self: *Self, task: *Task) Poll(T)`.
//! Keep suspended state/children in fields; arrange `task.wake()` before returning `.pending`.
//! Polling is single-shot unless explicitly documented reusable; never poll after `.ready`,
//! including an error result. `Frame.getResult()` remains repeatable.
//! `cancel()` must be idempotent and valid before polling. Cancellation is cooperative:
//! `Frame.cancel()` calls optional `cancel()` and wakes; drain to ready before freeing borrows.
//! Custom wake sources must unregister before task completion and count their borrows in
//! `task.outstanding_io` until ready is consumed, including completed, unconsumed results.
//! Pin primitives, waiters and channel storage until ready is consumed.
//! Do not copy pending children: workers can mutate their storage concurrently. Zig's
//! union tag checks can copy payloads too; use `Machine` for sequential child storage.
//! Granted permits/values win cancellation; clean them up.
//! Primitives are single-threaded; cross-thread producers use `loop.control.post`.
//! For children run in sequence, `Machine` (machine.zig) derives `poll` and `cancel`.

const std = @import("std");
const assert = std.debug.assert;

pub const Executor = @import("Executor.zig");
pub const Task = Executor.Task;
pub const Class = Executor.Class;
pub const Event = @import("Event.zig");
pub const Semaphore = @import("Semaphore.zig");
pub const Channel = @import("Channel.zig").Channel;
pub const Machine = @import("machine.zig").Machine;
pub const Next = @import("machine.zig").Next;
pub const Scope = @import("scope.zig").Scope;

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
            .name = if (Executor.named) @typeName(C) else {},
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

/// Children for `Join`/`Race`: a literal of coroutine types, named `.{ .user = A, .orders = B }`
/// or positional `.{ A, B }`; children, results and winners keep those names.
fn Set(comptime children: anytype) type {
    const info = switch (@typeInfo(@TypeOf(children))) {
        .@"struct" => |info| info,
        else => @compileError("expected coroutine types such as .{ .name = C } or .{ C, D }"),
    };
    const count = info.field_names.len;
    if (count == 0) @compileError("a group needs at least one child");

    var child_types: [count]type = undefined;
    var result_types: [count]type = undefined;
    var poll_types: [count]type = undefined;
    var poll_attrs: [count]std.lang.Type.Struct.FieldAttributes = undefined;

    for (info.field_names, 0..) |name, i| {
        const C = @field(children, name);
        if (!@hasDecl(C, "cancel")) @compileError("child `" ++ name ++ "` needs `cancel`: " ++ @typeName(C));

        child_types[i] = C;
        result_types[i] = ResultOf(C);
        poll_types[i] = Poll(ResultOf(C));
        poll_attrs[i] = .{ .default_value_ptr = &@as(Poll(ResultOf(C)), .pending) };
    }

    const kids = child_types;
    const results = result_types;
    const polls = poll_types;
    const attrs = poll_attrs;

    return struct {
        pub const names = info.field_names;
        pub const is_tuple = info.is_tuple;
        pub const ready_types = results;
        pub const Children = Reify(&kids);
        pub const Results = Reify(&results);
        pub const Polls = @Struct(.auto, null, names, &polls, &attrs);

        fn Reify(comptime types: []const type) type {
            if (is_tuple) return @Tuple(types);
            return @Struct(.auto, null, names, types, &@splat(.{}));
        }

        pub fn pending(states: *const Polls) bool {
            inline for (names) |name| {
                if (@field(states, name) == .pending) return true;
            }
            return false;
        }

        /// Returns whether a canceled child remains pending.
        pub fn cancelPending(children_ptr: *Children, states: *const Polls) bool {
            var any = false;
            inline for (names) |name| {
                if (@field(states, name) == .pending) {
                    @field(children_ptr, name).cancel();
                    any = true;
                }
            }
            return any;
        }

        pub fn collect(states: *const Polls, out: anytype) void {
            inline for (names) |name| @field(out, name) = @field(states, name).ready;
        }
    };
}

/// Pin until all finish; errors do not short-circuit, and the caller owns every result.
/// Children require idempotent `pub fn cancel(*Self) void`, valid before polling.
/// Cancellation requests unfinished children; keep polling to ready.
pub fn Join(comptime children: anytype) type {
    const S = Set(children);

    return struct {
        children: S.Children,
        polls: S.Polls = .{},
        task: ?*Task = null,
        cancel_requested: bool = false,

        const Self = @This();
        pub const Result = S.Results;

        pub fn init(kids: S.Children) Self {
            return .{ .children = kids };
        }

        pub fn poll(self: *Self, task: *Task) Poll(Result) {
            assert(S.pending(&self.polls));
            self.task = task;

            inline for (S.names) |name| {
                const state = &@field(self.polls, name);
                if (state.* == .pending) state.* = @field(self.children, name).poll(task);
            }
            if (S.pending(&self.polls)) return .pending;

            var result: Result = undefined;
            S.collect(&self.polls, &result);
            return .{ .ready = result };
        }

        pub fn cancel(self: *Self) void {
            if (self.cancel_requested) return;

            self.cancel_requested = true;
            if (S.cancelPending(&self.children, &self.polls)) {
                if (self.task) |task| task.wake();
            }
        }
    };
}

/// Named children only. First observed ready (success or error) wins; same-poll ties go
/// to the earlier child. Polls all before canceling.
/// Uses `Join`'s pinning/cancellation contract; cancel and drain unfinished losers.
/// Returns every result unchanged; caller must clean up successful losing resources.
pub fn Race(comptime children: anytype) type {
    const S = Set(children);
    if (S.is_tuple) @compileError("Race children need names, e.g. Race(.{ .recv = io.Recv, .timeout = io.Timeout })");
    if (@hasField(S.Children, "winner")) @compileError("Race reserves the child name `winner`");

    const W = std.meta.FieldEnum(S.Children);

    return struct {
        children: S.Children,
        polls: S.Polls = .{},
        winner: ?Winner = null,
        task: ?*Task = null,
        cancel_requested: bool = false,

        const Self = @This();
        pub const Winner = W;
        pub const Result = @Struct(
            .auto,
            null,
            &[_][]const u8{"winner"} ++ S.names,
            &[_]type{Winner} ++ S.ready_types,
            &@splat(.{}),
        );

        pub fn init(kids: S.Children) Self {
            return .{ .children = kids };
        }

        pub fn poll(self: *Self, task: *Task) Poll(Result) {
            assert(S.pending(&self.polls));
            self.task = task;

            inline for (S.names) |name| {
                const state = &@field(self.polls, name);
                if (state.* == .pending) {
                    state.* = @field(self.children, name).poll(task);
                    if (state.* == .ready and self.winner == null) self.winner = @field(Winner, name);
                }
            }

            if (self.winner != null) self.cancel();
            if (S.pending(&self.polls)) return .pending;

            var result: Result = undefined;
            result.winner = self.winner.?;
            S.collect(&self.polls, &result);
            return .{ .ready = result };
        }

        pub fn cancel(self: *Self) void {
            if (self.cancel_requested) return;

            self.cancel_requested = true;
            if (S.cancelPending(&self.children, &self.polls)) {
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
    _ = @import("scope.zig");
}
