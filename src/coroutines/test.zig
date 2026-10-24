const std = @import("std");
const assert = std.debug.assert;
const co = @import("root.zig");

const Poll = co.Poll;
const Frame = co.Frame;
const Join = co.Join;
const Race = co.Race;
const Yield = co.Yield;
const Task = co.Task;
const Executor = co.Executor;
const Event = co.Event;
const Semaphore = co.Semaphore;
const Channel = co.Channel;

const Ticker = struct {
    id: u8,
    left: u8,
    log: *std.ArrayList(u8),
    yield: Yield = .{},

    pub fn poll(self: *Ticker, task: *Task) Poll(void) {
        while (self.left > 0) {
            switch (self.yield.poll(task)) {
                .pending => return .pending,
                .ready => {},
            }

            self.log.appendAssumeCapacity(self.id);
            self.left -= 1;
        }

        return .{ .ready = {} };
    }
};

test "yield interleaves tasks" {
    var buffer: [4]u8 = undefined;
    var log: std.ArrayList(u8) = .initBuffer(&buffer);
    var ex: Executor = .{};
    var a: Frame(Ticker) = .{
        .coroutine = .{
            .id = 'a',
            .left = 2,
            .log = &log,
        },
    };
    var b: Frame(Ticker) = .{
        .coroutine = .{
            .id = 'b',
            .left = 2,
            .log = &log,
        },
    };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    ex.run();

    try std.testing.expectEqualStrings("abab", log.items);
}

/// Stand-in for an I/O completion: the producer stores a value and wakes the waiter.
const Slot = struct {
    value: ?u32 = null,
    waiter: ?*Task = null,

    fn send(slot: *Slot, value: u32) void {
        assert(slot.value == null);

        slot.value = value;

        if (slot.waiter) |task| {
            slot.waiter = null;
            task.wake();
        }
    }
};

const Recv = struct {
    slot: *Slot,

    pub fn poll(self: *Recv, task: *Task) Poll(u32) {
        if (self.slot.value) |value| {
            self.slot.value = null;
            return .{ .ready = value };
        }

        self.slot.waiter = task;
        return .pending;
    }
};

const Sum = struct {
    recv: Recv,
    yield: Yield = .{},
    total: u64 = 0,
    state: enum { receive, yield } = .receive,

    pub fn poll(self: *Sum, task: *Task) Poll(u64) {
        sw: switch (self.state) {
            .receive => {
                const n = switch (self.recv.poll(task)) {
                    .pending => return .pending,
                    .ready => |n| n,
                };

                if (n == 0) return .{ .ready = self.total };

                self.total += n;
                self.state = .yield;
                continue :sw .yield;
            },
            .yield => {
                switch (self.yield.poll(task)) {
                    .pending => return .pending,
                    .ready => {},
                }

                self.state = .receive;
                continue :sw .receive;
            },
        }
    }
};

test "await a child woken from outside" {
    var slot: Slot = .{};
    var ex: Executor = .{};
    var sum: Frame(Sum) = .{
        .coroutine = .{
            .recv = .{ .slot = &slot },
        },
    };

    ex.spawn(&sum.task);

    for ([_]u32{ 3, 4, 0 }) |n| {
        ex.run();
        try std.testing.expectEqual(null, sum.result);

        slot.send(n);
    }

    ex.run();

    try std.testing.expectEqual(7, sum.getResult());
}

test "Poll.value keeps ready optionals distinct from pending" {
    const pending: Poll(u32) = .pending;
    const ready: Poll(u32) = .{ .ready = 7 };
    const empty: Poll(?u32) = .{ .ready = null };

    try std.testing.expectEqual(null, pending.value());
    try std.testing.expectEqual(7, ready.value().?);
    try std.testing.expectEqual(null, empty.value().?);
}

test "Frame.init runs a coroutine that unwraps its child with value" {
    const Twice = struct {
        yield: Yield = .{},
        polls: u32 = 0,

        pub fn poll(self: *@This(), task: *Task) Poll(u32) {
            self.polls += 1;
            _ = self.yield.poll(task).value() orelse return .pending;

            return .{ .ready = self.polls };
        }
    };

    comptime assert(Frame(Twice).Result == u32);

    var ex: Executor = .{};
    var frame: Frame(Twice) = .init(.{});

    ex.spawn(&frame.task);
    ex.run();

    try std.testing.expectEqual(2, frame.getResult());
}

fn GroupProbe(comptime T: type) type {
    return struct {
        value: T,
        canceled_value: T = undefined,
        left: u8 = 0,
        drain_left: u8 = 0,
        wait_for_cancel: bool = false,
        wake_pending: bool = true,
        polls: usize = 0,
        cancel_calls: usize = 0,
        canceled: bool = false,
        done: bool = false,
        task: ?*Task = null,

        pub fn poll(self: *@This(), task: *Task) Poll(T) {
            assert(!self.done);
            self.polls += 1;

            if (self.task) |previous| {
                assert(previous == task);
            } else {
                self.task = task;
                task.outstanding_io += 1;
            }

            if (self.canceled) {
                if (self.drain_left > 0) {
                    self.drain_left -= 1;
                    task.wake();
                    return .pending;
                }
            } else if (self.left > 0 or self.wait_for_cancel) {
                self.left -|= 1;
                if (self.wake_pending) task.wake();
                return .pending;
            }

            self.done = true;
            task.outstanding_io -= 1;
            return .{ .ready = if (self.canceled) self.canceled_value else self.value };
        }

        pub fn cancel(self: *@This()) void {
            assert(!self.done);
            self.cancel_calls += 1;
            self.canceled = true;
        }
    };
}

test "join retains values and never repolls completed children" {
    const A = GroupProbe(u32);
    const B = GroupProbe(u8);
    var frame: Frame(Join(.{ .a = A, .b = B })) = .{
        .coroutine = .init(.{
            .a = .{ .value = 7 },
            .b = .{ .value = 11, .left = 2 },
        }),
    };
    var ex: Executor = .{};

    ex.spawn(&frame.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(null, frame.result);
    try std.testing.expectEqual(1, frame.task.outstanding_io);

    ex.run();

    try std.testing.expectEqual(7, frame.getResult().a);
    try std.testing.expectEqual(11, frame.getResult().b);
    try std.testing.expectEqual(1, frame.coroutine.children.a.polls);
    try std.testing.expectEqual(3, frame.coroutine.children.b.polls);
    try std.testing.expectEqual(&frame.task, frame.coroutine.children.a.task.?);
    try std.testing.expectEqual(&frame.task, frame.coroutine.children.b.task.?);
    try std.testing.expectEqual(0, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(0, frame.coroutine.children.b.cancel_calls);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "nested joins retain nullable error union and void results" {
    const A = GroupProbe(?u8);
    const B = GroupProbe(error{Failed}!u16);
    const C = GroupProbe(void);

    inline for (.{ false, true }) |error_first| {
        var frame: Frame(Join(.{ .a = Join(.{ .a = A, .b = B }), .b = C })) = .{
            .coroutine = .init(.{
                .a = .init(.{
                    .a = .{
                        .value = null,
                        .left = if (error_first) 2 else 0,
                    },
                    .b = .{
                        .value = error.Failed,
                        .left = if (error_first) 0 else 2,
                    },
                }),
                .b = .{ .value = {}, .left = 1 },
            }),
        };
        var ex: Executor = .{};

        ex.spawn(&frame.task);
        try std.testing.expect(ex.runOne());

        try std.testing.expectEqual(null, frame.result);

        ex.run();

        try std.testing.expectEqual(null, frame.getResult().a.a);
        try std.testing.expectError(error.Failed, frame.getResult().a.b);
        try std.testing.expectEqual({}, frame.getResult().b);
        try std.testing.expectEqual(if (error_first) 3 else 1, frame.coroutine.children.a.children.a.polls);
        try std.testing.expectEqual(if (error_first) 1 else 3, frame.coroutine.children.a.children.b.polls);
        try std.testing.expectEqual(2, frame.coroutine.children.b.polls);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}

test "race keeps b winner while canceled a drains and preserves its result" {
    const A = GroupProbe(error{Canceled}!u32);
    const B = GroupProbe(u8);
    var frame: Frame(Race(.{ .a = A, .b = B })) = .{
        .coroutine = .init(.{
            .a = .{
                .value = 9,
                .canceled_value = error.Canceled,
                .wait_for_cancel = true,
                .drain_left = 2,
            },
            .b = .{ .value = 42, .left = 1 },
        }),
    };
    var ex: Executor = .{};

    ex.spawn(&frame.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(2, frame.task.outstanding_io);

    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(null, frame.result);
    try std.testing.expectEqual(1, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(0, frame.coroutine.children.b.cancel_calls);
    try std.testing.expectEqual(1, frame.task.outstanding_io);

    frame.coroutine.cancel();

    for (0..2) |_| {
        try std.testing.expect(ex.runOne());
        try std.testing.expectEqual(null, frame.result);
        try std.testing.expectEqual(1, frame.task.outstanding_io);
    }

    ex.run();

    try std.testing.expectEqual(.b, frame.getResult().winner);
    try std.testing.expectError(error.Canceled, frame.getResult().a);
    try std.testing.expectEqual(42, frame.getResult().b);
    try std.testing.expectEqual(5, frame.coroutine.children.a.polls);
    try std.testing.expectEqual(2, frame.coroutine.children.b.polls);
    try std.testing.expectEqual(1, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "race breaks same poll ties in favor of a and returns both resources" {
    const Resource = struct {
        closed: *bool,

        fn close(self: @This()) void {
            assert(!self.closed.*);
            self.closed.* = true;
        }
    };
    const Child = GroupProbe(Resource);
    var a_closed = false;
    var b_closed = false;
    var frame: Frame(Race(.{ .a = Child, .b = Child })) = .{
        .coroutine = .init(.{
            .a = .{
                .value = .{ .closed = &a_closed },
            },
            .b = .{
                .value = .{ .closed = &b_closed },
            },
        }),
    };
    var ex: Executor = .{};

    ex.spawn(&frame.task);
    ex.run();

    const result = frame.getResult();

    try std.testing.expectEqual(.a, result.winner);
    try std.testing.expectEqual(&a_closed, result.a.closed);
    try std.testing.expectEqual(&b_closed, result.b.closed);
    try std.testing.expectEqual(0, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(0, frame.coroutine.children.b.cancel_calls);

    result.a.close();
    result.b.close();

    try std.testing.expect(a_closed and b_closed);
}

test "race cancellation before polling retains child results and first observed winner" {
    const A = GroupProbe(error{Canceled}!void);
    const B = GroupProbe(?u16);
    var frame: Frame(Race(.{ .a = A, .b = B })) = .{
        .coroutine = .init(.{
            .a = .{
                .value = {},
                .canceled_value = error.Canceled,
                .drain_left = 1,
            },
            .b = .{ .value = 3, .canceled_value = null },
        }),
    };

    frame.coroutine.cancel();
    frame.coroutine.cancel();

    var ex: Executor = .{};

    ex.spawn(&frame.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(null, frame.result);
    try std.testing.expectEqual(1, frame.task.outstanding_io);

    ex.run();

    try std.testing.expectEqual(.b, frame.getResult().winner);
    try std.testing.expectError(error.Canceled, frame.getResult().a);
    try std.testing.expectEqual(null, frame.getResult().b);
    try std.testing.expectEqual(1, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(1, frame.coroutine.children.b.cancel_calls);
    try std.testing.expectEqual(1, frame.coroutine.children.b.polls);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "group cancellation wakes parked children and drains both exactly once" {
    const Child = GroupProbe(error{Canceled}!void);

    inline for (.{ false, true }) |is_race| {
        const Group = if (is_race) Race(.{ .a = Child, .b = Child }) else Join(.{ .a = Child, .b = Child });
        var frame: Frame(Group) = .{
            .coroutine = .init(.{
                .a = .{
                    .value = {},
                    .canceled_value = error.Canceled,
                    .wait_for_cancel = true,
                    .wake_pending = false,
                    .drain_left = 1,
                },
                .b = .{
                    .value = {},
                    .canceled_value = error.Canceled,
                    .wait_for_cancel = true,
                    .wake_pending = false,
                    .drain_left = 1,
                },
            }),
        };
        var ex: Executor = .{};

        ex.spawn(&frame.task);
        ex.run();

        try std.testing.expectEqual(null, frame.result);
        try std.testing.expectEqual(.idle, frame.task.state);
        try std.testing.expectEqual(2, frame.task.outstanding_io);

        frame.coroutine.cancel();
        frame.coroutine.cancel();
        try std.testing.expect(ex.runOne());

        try std.testing.expectEqual(null, frame.result);
        try std.testing.expectEqual(2, frame.task.outstanding_io);

        ex.run();

        try std.testing.expectError(error.Canceled, frame.getResult().a);
        try std.testing.expectError(error.Canceled, frame.getResult().b);
        if (is_race) try std.testing.expectEqual(.a, frame.getResult().winner);
        try std.testing.expectEqual(1, frame.coroutine.children.a.cancel_calls);
        try std.testing.expectEqual(1, frame.coroutine.children.b.cancel_calls);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}

test "join cancellation skips completed children" {
    const A = GroupProbe(void);
    const B = GroupProbe(error{Canceled}!void);
    var frame: Frame(Join(.{ .a = A, .b = B })) = .{
        .coroutine = .init(.{
            .a = .{ .value = {} },
            .b = .{
                .value = {},
                .canceled_value = error.Canceled,
                .wait_for_cancel = true,
                .wake_pending = false,
                .drain_left = 1,
            },
        }),
    };
    var ex: Executor = .{};

    ex.spawn(&frame.task);
    ex.run();

    frame.coroutine.cancel();
    frame.coroutine.cancel();
    ex.run();

    try std.testing.expectEqual({}, frame.getResult().a);
    try std.testing.expectError(error.Canceled, frame.getResult().b);
    try std.testing.expectEqual(1, frame.coroutine.children.a.polls);
    try std.testing.expectEqual(0, frame.coroutine.children.a.cancel_calls);
    try std.testing.expectEqual(1, frame.coroutine.children.b.cancel_calls);
    try std.testing.expectEqual(0, frame.task.outstanding_io);
}

test "join and race accept three positional or named children" {
    const P = GroupProbe(u8);

    comptime assert(Join(.{ .a = P, .b = P }) == Join(.{ .a = P, .b = P }));
    comptime assert(@typeInfo(Join(.{ P, P, P }).Result).@"struct".is_tuple);

    var ex: Executor = .{};
    var joined: Frame(Join(.{ P, P, P })) = .init(.init(.{
        .{ .value = 1, .left = 2 },
        .{ .value = 2 },
        .{ .value = 3, .left = 1 },
    }));
    var raced: Frame(Race(.{ .slow = P, .tie = P, .late = P })) = .init(.init(.{
        .slow = .{ .value = 1, .canceled_value = 9, .left = 1 },
        .tie = .{ .value = 2 },
        .late = .{ .value = 3 },
    }));

    ex.spawn(&joined.task);
    ex.spawn(&raced.task);
    ex.run();

    const all = joined.getResult();
    try std.testing.expectEqual(.{ 1, 2, 3 }, .{ all[0], all[1], all[2] });
    try std.testing.expectEqual(1, joined.coroutine.children[1].polls);
    try std.testing.expectEqual(3, joined.coroutine.children[0].polls);

    const first = raced.getResult();
    try std.testing.expectEqual(.tie, first.winner);
    try std.testing.expectEqual(.{ 9, 2, 3 }, .{ first.slow, first.tie, first.late });
    try std.testing.expectEqual(1, raced.coroutine.children.slow.cancel_calls);
    try std.testing.expectEqual(0, raced.coroutine.children.late.cancel_calls);
    try std.testing.expectEqual(0, raced.task.outstanding_io);
}

test "frame cancellation supports children aligned above Task" {
    const Child = struct {
        value: u128 = 42,
        canceled: bool = false,

        pub fn poll(self: *@This(), _: *Task) Poll(error{Canceled}!u128) {
            return if (self.canceled) .{ .ready = error.Canceled } else .{ .ready = self.value };
        }

        pub fn cancel(self: *@This()) void {
            self.canceled = true;
        }
    };

    try std.testing.expect(@alignOf(Frame(Child)) > @alignOf(Task));

    var ex: Executor = .{};
    var success: Frame(Child) = .{ .coroutine = .{} };
    var canceled: Frame(Child) = .{ .coroutine = .{} };

    canceled.cancel();
    canceled.cancel();
    ex.spawn(&success.task);
    ex.spawn(&canceled.task);
    ex.run();

    try std.testing.expectEqual(42, try success.getResult());
    try std.testing.expectError(error.Canceled, canceled.getResult());

    success.cancel();

    try std.testing.expect(!success.task.cancel_requested);
}

test "yield can be canceled and composed in join and race" {
    inline for (.{ false, true }) |is_race| {
        const Group = if (is_race) Race(.{ .a = Yield, .b = Yield }) else Join(.{ .a = Yield, .b = Yield });
        var ex: Executor = .{};
        var frame: Frame(Group) = .{
            .coroutine = .init(.{
                .a = .{},
                .b = .{},
            }),
        };

        ex.spawn(&frame.task);
        try std.testing.expect(ex.runOne());

        frame.cancel();
        ex.run();

        try std.testing.expect(frame.result != null);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
        try std.testing.expectEqual(null, ex.ready.first);
    }

    var ex: Executor = .{};
    var frame: Frame(Yield) = .{ .coroutine = .{} };

    frame.cancel();
    ex.spawn(&frame.task);
    try std.testing.expect(ex.runOne());

    try std.testing.expect(frame.task.state == .done);
    try std.testing.expect(!ex.runOne());
}

test "task cancellation drains joins and races of borrowed primitive waits" {
    inline for (.{ false, true }) |is_race| {
        var event: Event = .{};
        defer event.deinit();

        var sem = Semaphore.init(0);
        defer sem.deinit();

        const Group = if (is_race)
            Race(.{ .a = Event.Wait, .b = Semaphore.Acquire })
        else
            Join(.{ .a = Event.Wait, .b = Semaphore.Acquire });
        var frame: Frame(Group) = .{
            .coroutine = .init(.{
                .a = event.wait(),
                .b = sem.acquire(),
            }),
        };
        var ex: Executor = .{};

        ex.spawn(&frame.task);
        ex.run();

        try std.testing.expectEqual(2, frame.task.outstanding_io);

        ex.cancelAll();

        try std.testing.expectEqual(null, event.waiters.first);
        try std.testing.expectEqual(null, sem.waiters.first);
        try std.testing.expectEqual(2, frame.task.outstanding_io);

        ex.run();

        try std.testing.expectError(error.Canceled, frame.getResult().a);
        try std.testing.expectError(error.Canceled, frame.getResult().b);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
        try std.testing.expectEqual(null, ex.all_tasks.first);
    }
}

test "race preserves both channel resources granted before its next poll" {
    const Ch = Channel(*bool);
    var channel = Ch.init(&.{});
    defer channel.deinit();

    var ex: Executor = .{};
    var race: Frame(Race(.{ .a = Ch.Recv, .b = Ch.Recv })) = .{
        .coroutine = .init(.{
            .a = channel.recv(),
            .b = channel.recv(),
        }),
    };

    ex.spawn(&race.task);
    ex.run();

    var a_owned = true;
    var b_owned = true;
    var a: Frame(Ch.Send) = .{ .coroutine = channel.send(&a_owned) };
    var b: Frame(Ch.Send) = .{ .coroutine = channel.send(&b_owned) };

    ex.spawn(&a.task);
    ex.spawn(&b.task);
    try std.testing.expect(ex.runOne());
    try std.testing.expect(ex.runOne());

    try std.testing.expectEqual(2, race.task.outstanding_io);

    race.cancel();
    ex.run();

    try a.getResult();
    try b.getResult();
    try std.testing.expectEqual(.a, race.getResult().winner);
    try std.testing.expectEqual(&a_owned, try race.getResult().a);
    try std.testing.expectEqual(&b_owned, try race.getResult().b);

    (try race.getResult().a).* = false;
    (try race.getResult().b).* = false;

    try std.testing.expect(!a_owned and !b_owned);
    try std.testing.expectEqual(0, race.task.outstanding_io);
}
