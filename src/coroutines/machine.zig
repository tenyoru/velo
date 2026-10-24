//! `Machine(Ctx)`: a coroutine that drives one child at a time.
//!
//! `Ctx` declares:
//! - `pub const State = union(enum) { tag: Child, ... }`: children follow the `Join` child contract.
//! - `pub const Result`: the machine's ready type.
//! - `pub fn start(*Ctx) Next(Ctx)`: called on the first poll, once pinned, so children
//!   may borrow `Ctx` fields.
//! - `pub fn <tag>(*Ctx, ResultOf(Child)) Next(Ctx)` per state: called with the child's
//!   ready result; `.go` replaces the child, `.done` finishes.
//!
//! A child is replaced only after its ready result is consumed, so handlers own what
//! results carry. Cancellation requests the active child and each later child before its
//! first poll; handlers still choose the result and must reach `.done`. Children that
//! complete normally under cancellation, such as `Yield`, do not end the machine by themselves.

const std = @import("std");
const assert = std.debug.assert;
const co = @import("root.zig");

const Task = co.Task;
const Poll = co.Poll;

pub fn Next(comptime Ctx: type) type {
    return union(enum) {
        go: Ctx.State,
        done: Ctx.Result,
    };
}

/// Pin from the first poll until ready is consumed.
pub fn Machine(comptime Ctx: type) type {
    comptime check(Ctx);

    return struct {
        ctx: Ctx,
        /// Active child; null before `start` and after `.done`.
        state: ?Ctx.State = null,
        started: bool = false,
        canceled: bool = false,
        task: ?*Task = null,

        const Self = @This();

        pub fn init(ctx: Ctx) Self {
            return .{ .ctx = ctx };
        }

        pub fn poll(self: *Self, task: *Task) Poll(Ctx.Result) {
            assert(!self.started or self.state != null);
            self.task = task;

            if (!self.started) {
                self.started = true;
                if (self.apply(Ctx.start(&self.ctx))) |result| return .{ .ready = result };
            }

            while (true) {
                const next = switch (self.state.?) {
                    inline else => |*child, tag| next: {
                        const result = child.poll(task).value() orelse return .pending;
                        break :next @field(Ctx, @tagName(tag))(&self.ctx, result);
                    },
                };

                if (self.apply(next)) |result| return .{ .ready = result };
            }
        }

        pub fn cancel(self: *Self) void {
            if (self.canceled) return;
            self.canceled = true;

            if (self.state) |*child| {
                cancelChild(child);
                if (self.task) |task| task.wake();
            }
        }

        fn apply(self: *Self, next: Next(Ctx)) ?Ctx.Result {
            switch (next) {
                .done => |result| {
                    self.state = null;
                    return result;
                },
                .go => |state| {
                    self.state = state;
                    if (self.canceled) cancelChild(&self.state.?);
                    return null;
                },
            }
        }

        fn cancelChild(state: *Ctx.State) void {
            switch (state.*) {
                inline else => |*child| child.cancel(),
            }
        }
    };
}

fn check(comptime Ctx: type) void {
    const name = @typeName(Ctx);
    const next = "co.Next(" ++ name ++ ")";

    if (!@hasDecl(Ctx, "State")) @compileError(name ++ " needs `pub const State = union(enum) { ... }`");
    if (!@hasDecl(Ctx, "Result")) @compileError(name ++ " needs `pub const Result`");
    if (!@hasDecl(Ctx, "start")) @compileError(name ++ " needs `pub fn start(*" ++ name ++ ") " ++ next ++ "`");

    const info = @typeInfo(Ctx.State);
    if (info != .@"union" or info.@"union".tag_type == null)
        @compileError(name ++ ".State must be a tagged union of coroutines");

    for (info.@"union".field_names, info.@"union".field_types) |tag, Child| {
        const state = name ++ ".State." ++ tag;

        switch (@typeInfo(Child)) {
            .@"struct", .@"union", .@"enum", .@"opaque" => {},
            else => @compileError(state ++ " must be a coroutine, not " ++ @typeName(Child)),
        }
        if (!@hasDecl(Child, "poll") or !@hasDecl(Child, "cancel"))
            @compileError(state ++ " needs `poll` and `cancel`: " ++ @typeName(Child));

        const Result = co.ResultOf(Child);
        const handler = "pub fn " ++ tag ++ "(*" ++ name ++ ", " ++ @typeName(Result) ++ ") " ++ next;

        if (!@hasDecl(Ctx, tag)) @compileError(name ++ " needs `" ++ handler ++ "` for " ++ state);

        const params = @typeInfo(@TypeOf(@field(Ctx, tag))).@"fn".param_types;
        if (params.len != 2 or (params[1] != null and params[1].? != Result))
            @compileError(name ++ "." ++ tag ++ " must be `" ++ handler ++ "`");
    }
}

const Steps = struct {
    event: co.Event = .{},
    log: [3]u8 = undefined,
    len: usize = 0,

    pub const Result = []const u8;
    pub const State = union(enum) { wait: co.Event.Wait, yield: co.Yield };

    fn note(self: *Steps, step: u8) void {
        self.log[self.len] = step;
        self.len += 1;
    }

    pub fn start(self: *Steps) Next(Steps) {
        self.note('s');
        return .{ .go = .{ .wait = self.event.wait() } };
    }

    pub fn wait(self: *Steps, result: error{Canceled}!void) Next(Steps) {
        result catch return .{ .done = "canceled" };

        self.note('w');
        return .{ .go = .{ .yield = .{} } };
    }

    pub fn yield(self: *Steps, _: void) Next(Steps) {
        self.note('y');
        return .{ .done = self.log[0..self.len] };
    }
};

test "machine starts after pinning and transitions through borrowed children" {
    var ex: co.Executor = .{};
    var frame: co.Frame(Machine(Steps)) = .init(.init(.{}));
    defer frame.coroutine.ctx.event.deinit();

    ex.spawn(&frame.task);
    ex.run();

    try std.testing.expectEqual(null, frame.result);
    try std.testing.expectEqual(1, frame.task.outstanding_io);

    frame.coroutine.ctx.event.set();
    ex.run();

    try std.testing.expectEqualStrings("swy", frame.getResult());
    try std.testing.expectEqual(null, frame.coroutine.state);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "machine cancellation before the first poll reaches the first child" {
    var ex: co.Executor = .{};
    var frame: co.Frame(Machine(Steps)) = .init(.init(.{}));
    defer frame.coroutine.ctx.event.deinit();

    frame.cancel();
    frame.cancel();
    ex.spawn(&frame.task);
    ex.run();

    try std.testing.expectEqualStrings("canceled", frame.getResult());
    try std.testing.expectEqual(1, frame.coroutine.ctx.len);
}

test "machine cancellation reaches children armed after a losing race" {
    // Re-arms even after cancellation; only the machine's propagation ends it.
    const Rearm = struct {
        event: *co.Event,
        waits: u32 = 0,

        pub const Result = u32;
        pub const State = union(enum) { wait: co.Event.Wait };

        pub fn start(self: *@This()) Next(@This()) {
            self.waits += 1;
            return .{ .go = .{ .wait = self.event.wait() } };
        }

        pub fn wait(self: *@This(), result: anytype) Next(@This()) {
            result catch if (self.waits == 2) return .{ .done = self.waits };
            return self.start();
        }
    };

    var event: co.Event = .{};
    defer event.deinit();

    var ex: co.Executor = .{};
    var frame: co.Frame(co.Race(Machine(Rearm), co.Yield)) = .init(.{
        .a = .init(.{ .event = &event }),
        .b = .{},
    });

    ex.spawn(&frame.task);
    ex.run();

    const result = frame.getResult();
    try std.testing.expectEqual(.b, result.winner);
    try std.testing.expectEqual(2, result.a);
    try std.testing.expect(!frame.task.cancel_requested);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}
