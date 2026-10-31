const std = @import("std");
const linux = std.os.linux;
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const support = @import("support.zig");
const testing = std.testing;
const run = support.run;

fn uring(entries: u16) !io.Loop {
    return io.Loop.initWithBackend(.{ .entries = entries }, .uring) catch |err| switch (err) {
        error.PermissionDenied, error.SystemOutdated => error.SkipZigTest,
        else => err,
    };
}

fn registeredLoop(buffers: []const []u8) !io.Loop {
    // Skip only setup denial; explicit registration errors must fail the test.
    var available = try uring(8);
    available.deinit();
    return io.Loop.initWithBackend(.{ .entries = 8, .buffers = buffers }, .uring);
}

fn drained(loop: *io.Loop) !void {
    try testing.expectEqual(0, loop.active);
    try testing.expectEqual(0, loop.outstanding_cqes);
    try testing.expectEqual(null, loop.pending.first);
    try testing.expectEqual(null, loop.executor.ready.first);
    try testing.expect(!loop.running);
    try testing.expectEqual(0, loop.ring.sq_ready());
    try testing.expectEqual(0, loop.ring.cq_ready());
}

fn Await(comptime Op: type) type {
    return struct {
        op: Op,

        pub fn poll(
            self: *@This(),
            task: *co.Task,
        ) @typeInfo(@TypeOf(Op.poll)).@"fn".return_type.? {
            return self.op.poll(task);
        }
    };
}

fn syscall(rc: usize) !usize {
    if (@as(isize, @bitCast(rc)) < 0) return error.TestSyscallFailed;

    return rc;
}

fn pair() ![2]linux.fd_t {
    var fds: [2]linux.fd_t = undefined;

    _ = try syscall(linux.socketpair(
        linux.AF.UNIX,
        linux.SOCK.STREAM | linux.SOCK.CLOEXEC,
        0,
        &fds,
    ));

    return fds;
}

fn deadline(ms: i64) io.Timespec {
    return support.deadline(ms) catch @panic("clock_gettime");
}

fn negative(e: linux.E) i32 {
    return -@as(i32, @intCast(@backingInt(e)));
}

test "io empty loop and minimum ring size" {
    try testing.expectError(error.RingTooSmall, io.Loop.init(.{ .entries = 0 }));
    try testing.expectError(error.RingTooSmall, io.Loop.init(.{ .entries = 1 }));

    var loop = try uring(2);
    defer loop.deinit();

    run(&loop);
    run(&loop);

    try drained(&loop);
}

test "io timer wakes once and operation can be reused after ready" {
    var loop = try uring(2);
    defer loop.deinit();
    var frame: co.Frame(Await(io.Timeout)) = .{
        .coroutine = .{
            .op = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 2_000_000 },
            }),
        },
    };
    const before = deadline(0);

    loop.executor.spawn(&frame.task);
    run(&loop);

    try frame.getResult();
    const after = deadline(0);
    try testing.expect(after.sec > before.sec or after.nsec >= before.nsec + 2_000_000);
    try testing.expectEqual(linux.E.TIME, frame.coroutine.op.primaryError().?);
    try testing.expectEqual(null, frame.coroutine.op.deadlineError());
    try testing.expectEqual(0, frame.task.outstanding_io);

    frame.coroutine.op.deinit();

    try drained(&loop);

    frame = .{
        .coroutine = .{
            .op = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 1 },
            }),
        },
    };

    loop.executor.spawn(&frame.task);
    run(&loop);

    try frame.getResult();

    frame.coroutine.op.deinit();

    try drained(&loop);
}

fn epollLoop() !io.Loop {
    return io.Loop.initWithBackend(.{ .entries = 8 }, .epoll);
}

fn sendLater(fd: io.Socket) void {
    std.Io.sleep(testing.io, .fromMilliseconds(2), .awake) catch {};
    _ = linux.write(fd, "x", 1);
}

test "epoll leaves a fired fd registered but disabled" {
    var loop = try epollLoop();
    defer loop.deinit();
    const fds = try support.pair(&loop, .ipv4);
    defer for (fds) |fd| support.close(fd);

    var buffer: [1]u8 = undefined;
    var recv: co.Frame(Await(io.Recv)) = .init(.{ .op = io.Recv.init(&loop, .{
        .fd = fds[0],
        .buffer = &buffer,
        .deadline = try support.deadline(5000),
    }) });
    loop.spawn(&recv);
    const writer = try std.Thread.spawn(.{}, sendLater, .{fds[1]});
    run(&loop);
    writer.join();
    try testing.expectEqual(1, try recv.getResult());

    // Unread data must not raise events: level-triggered interest would spin the loop here.
    try testing.expectEqual(1, linux.write(fds[1], "y", 1));
    const state = &loop.epoll.?;
    var batch: [1]linux.epoll_event = undefined;
    try testing.expectEqual(0, linux.epoll_wait(state.fd, &batch, batch.len, 1));

    var event: linux.epoll_event = .{ .events = linux.EPOLL.IN, .data = .{ .fd = fds[0] } };
    try testing.expectEqual(.SUCCESS, linux.errno(linux.epoll_ctl(state.fd, linux.EPOLL.CTL_DEL, fds[0], &event)));
}

test "epoll rearms a descriptor number reused after close" {
    var loop = try epollLoop();
    defer loop.deinit();

    const first = try support.pair(&loop, .ipv4);
    var buffer: [1]u8 = undefined;
    var recv: co.Frame(Await(io.Recv)) = .init(.{ .op = io.Recv.init(&loop, .{
        .fd = first[0],
        .buffer = &buffer,
        .deadline = try support.deadline(5000),
    }) });
    loop.spawn(&recv);
    var writer = try std.Thread.spawn(.{}, sendLater, .{first[1]});
    run(&loop);
    writer.join();
    try testing.expectEqual(1, try recv.getResult());

    // The closed fd left a disabled registration behind; its number now names a new socket.
    for (first) |fd| support.close(fd);
    const second = try support.pair(&loop, .ipv4);
    defer for (second) |fd| support.close(fd);
    const reused = second[0] == first[0] or second[1] == first[0];
    try testing.expect(reused);

    const target = if (second[0] == first[0]) second[0] else second[1];
    const peer = if (second[0] == first[0]) second[1] else second[0];
    var again: co.Frame(Await(io.Recv)) = .init(.{ .op = io.Recv.init(&loop, .{
        .fd = target,
        .buffer = &buffer,
        .deadline = try support.deadline(5000),
    }) });
    loop.spawn(&again);
    writer = try std.Thread.spawn(.{}, sendLater, .{peer});
    run(&loop);
    writer.join();
    try testing.expectEqual(1, try again.getResult());
    try support.drained(&loop);
}

test "epoll rearms the direction an event did not finish" {
    const Drain = struct {
        fn run(fd: io.Socket, sink: *[4096]u8, left: *std.atomic.Value(bool)) void {
            std.Io.sleep(testing.io, .fromMilliseconds(2), .awake) catch {};
            _ = linux.write(fd, "x", 1);
            // Let the read event fire alone before the write side has room.
            std.Io.sleep(testing.io, .fromMilliseconds(5), .awake) catch {};
            while (left.load(.acquire)) _ = linux.read(fd, sink, sink.len);
        }
    };
    const SendThenFlag = struct {
        op: io.Send,
        left: *std.atomic.Value(bool),

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!usize) {
            switch (self.op.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    self.left.store(false, .release);
                    return .{ .ready = result };
                },
            }
        }
    };

    var loop = try epollLoop();
    defer loop.deinit();
    const fds = try support.pair(&loop, .ipv4);
    defer for (fds) |fd| support.close(fd);
    // Nonblocking reads let the drain thread observe completion even when the peer is empty.
    _ = linux.fcntl(fds[1], linux.F.SETFL, @as(usize, 1) << 11);

    var chunk: [65536]u8 = @splat(7);
    while (true) {
        const rc = linux.sendto(fds[0], &chunk, chunk.len, linux.MSG.DONTWAIT, null, 0);
        if (linux.errno(rc) == .AGAIN) break;
    }

    var buffer: [1]u8 = undefined;
    var left: std.atomic.Value(bool) = .init(true);
    var sink: [4096]u8 = undefined;
    var recv: co.Frame(Await(io.Recv)) = .init(.{ .op = io.Recv.init(&loop, .{
        .fd = fds[0],
        .buffer = &buffer,
        .deadline = try support.deadline(5000),
    }) });
    var send: co.Frame(SendThenFlag) = .init(.{
        .op = io.Send.init(&loop, .{ .fd = fds[0], .buffer = chunk[0..1024], .deadline = try support.deadline(5000) }),
        .left = &left,
    });
    loop.spawn(&recv);
    loop.spawn(&send);
    const drain = try std.Thread.spawn(.{}, Drain.run, .{ fds[1], &sink, &left });
    run(&loop);
    drain.join();

    try testing.expectEqual(1, try recv.getResult());
    try testing.expect(try send.getResult() > 0);
    try support.drained(&loop);
}

const Pressure = enum { yields, long_polls, posts };

test "epoll checks readiness under busy tasks and post floods" {
    const Spinner = struct {
        done: *std.atomic.Value(bool),
        long: bool,
        target: *const io.Operation,
        long_polls: usize = 0,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            if (self.done.load(.acquire)) return .{ .ready = {} };
            // Only once armed: a slice that outlasts the writer would find data in the first recv.
            if (self.long and self.target.native.epoll.waiting) {
                self.long_polls += 1;
                const start = io.now() catch @panic("clock failed");
                while (true) {
                    const now = io.now() catch @panic("clock failed");
                    if ((now.sec - start.sec) * 1_000_000_000 + now.nsec - start.nsec >= 1_000_000) break;
                }
            }
            task.wake();
            return .pending;
        }
    };
    const Reader = struct {
        op: io.Recv,
        done: *std.atomic.Value(bool),

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!usize) {
            switch (self.op.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    self.done.store(true, .release);
                    return .{ .ready = result };
                },
            }
        }
    };
    // Reposting from the callback keeps the stack non-empty at every `enterPark`, so the
    // loop can never reach a blocking check by luck, as it could against another thread.
    const Flood = struct {
        loop: *io.Loop,
        done: *std.atomic.Value(bool),
        post: io.Control.Post = .{ .callback = callback },

        fn callback(post: *io.Control.Post) void {
            const self: *@This() = @fieldParentPtr("post", post);
            if (!self.done.load(.acquire)) self.loop.post(&self.post);
        }
    };

    for (std.enums.values(Pressure)) |pressure| {
        var loop = try epollLoop();
        defer loop.deinit();
        const fds = try support.pair(&loop, .ipv4);
        defer for (fds) |fd| support.close(fd);

        var done: std.atomic.Value(bool) = .init(false);
        var buffer: [1]u8 = undefined;
        // The deadline turns starvation into a failure instead of a hung test.
        var reader: co.Frame(Reader) = .init(.{
            .op = io.Recv.init(&loop, .{ .fd = fds[0], .buffer = &buffer, .deadline = try support.deadline(5000) }),
            .done = &done,
        });
        var spinner: co.Frame(Spinner) = .init(.{
            .done = &done,
            .long = pressure == .long_polls,
            .target = &reader.coroutine.op.operation,
        });
        var flood: Flood = .{ .loop = &loop, .done = &done };

        loop.spawn(&reader);
        if (pressure == .posts) loop.post(&flood.post) else loop.spawn(&spinner);
        const writer = try std.Thread.spawn(.{}, sendLater, .{fds[1]});
        run(&loop);
        writer.join();

        try testing.expectEqual(1, try reader.getResult());
        // One slice of 32 long polls is the worst case; 64 would mean the quota was ignored.
        if (pressure == .long_polls) try testing.expect(spinner.coroutine.long_polls < 48);
        try support.drained(&loop);
    }
}

test "io timers take no ring capacity and leave nothing in the kernel to cancel" {
    const Settle = struct {
        loop: *io.Loop,
        expected: usize,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            if (self.loop.active == self.expected and self.loop.pending.first == null) {
                self.loop.stop();
                return .{ .ready = {} };
            }
            task.wake();
            return .pending;
        }
    };

    // Far more timers than the two SQ slots could hold at once.
    var loop = try uring(2);
    defer loop.deinit();

    var timers: [256]co.Frame(io.Timeout) = undefined;
    for (&timers) |*timer| {
        timer.* = .{ .coroutine = io.Timeout.init(&loop, .{ .duration = io.seconds(3600) }) };
        loop.spawn(timer);
    }
    var settle: co.Frame(Settle) = .init(.{ .loop = &loop, .expected = timers.len });
    loop.spawn(&settle);
    run(&loop);

    try testing.expectEqual(timers.len, loop.active);
    try testing.expectEqual(0, loop.outstanding_cqes);

    loop.shutdown();
    run(&loop);
    for (&timers) |*timer| try testing.expectError(error.OperationCanceled, timer.getResult());
    try drained(&loop);
}

test "io timers bound waits with I/O in flight and expire in deadline order" {
    const Sleeper = struct {
        timer: io.Timeout,
        clock: *usize,
        finished: usize = 0,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            switch (self.timer.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    result catch @panic("timer failed");
                    self.clock.* += 1;
                    self.finished = self.clock.*;
                    return .{ .ready = {} };
                },
            }
        }
    };

    var loop = try uring(8);
    defer loop.deinit();
    const fds = try support.pair(&loop, .ipv4);
    defer for (fds) |fd| support.close(fd);

    var clock: usize = 0;
    var buffer: [1]u8 = undefined;
    var recv: co.Frame(Await(io.Recv)) = .init(.{ .op = io.Recv.init(&loop, .{
        .fd = fds[0],
        .buffer = &buffer,
        .deadline = try support.deadline(60),
    }) });
    var sleepers: [3]co.Frame(Sleeper) = undefined;
    for (&sleepers, [_]u32{ 9, 3, 6 }) |*sleeper, ms| {
        sleeper.* = .init(.{ .timer = io.Timeout.init(&loop, .{ .duration = io.milliseconds(ms) }), .clock = &clock });
    }

    loop.spawn(&recv);
    for (&sleepers) |*sleeper| loop.spawn(sleeper);
    run(&loop);

    try testing.expectError(error.Timeout, recv.getResult());
    for (&sleepers, [_]usize{ 3, 1, 2 }) |*sleeper, finished|
        try testing.expectEqual(finished, sleeper.coroutine.finished);
    try drained(&loop);
}

test "io timers reject invalid durations and fire zero durations on both backends" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(8, mode);
        defer loop.deinit();

        for ([_]io.Timespec{ .{ .sec = -1, .nsec = 0 }, .{ .sec = 0, .nsec = -1 }, .{ .sec = 0, .nsec = 1_000_000_000 } }) |duration| {
            var timer: co.Frame(io.Timeout) = .{ .coroutine = io.Timeout.init(&loop, .{ .duration = duration }) };
            loop.spawn(&timer);
            run(&loop);
            try testing.expectError(error.InvalidArgument, timer.getResult());
        }

        var zero: co.Frame(io.Timeout) = .{ .coroutine = io.Timeout.init(&loop, .{ .duration = .{ .sec = 0, .nsec = 0 } }) };
        loop.spawn(&zero);
        run(&loop);
        try zero.getResult();
        try support.drained(&loop);
    }
}

test "io linked recv expires and drains both CQEs; ready recv cancels long timer" {
    var loop = try uring(2);
    defer loop.deinit();

    const fds = try pair();
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var buffer: [4]u8 = undefined;
    var frame: co.Frame(Await(io.Recv)) = .{
        .coroutine = .{
            .op = io.Recv.init(&loop, .{
                .fd = fds[0],
                .buffer = &buffer,
                .deadline = deadline(5),
            }),
        },
    };

    loop.executor.spawn(&frame.task);
    run(&loop);

    try testing.expectError(error.Timeout, frame.getResult());
    try testing.expectEqual(linux.E.CANCELED, frame.coroutine.op.primaryError().?);
    try testing.expectEqual(linux.E.TIME, frame.coroutine.op.deadlineError().?);
    try testing.expectEqual(0, frame.task.outstanding_io);

    frame.coroutine.op.deinit();

    try drained(&loop);

    try testing.expectEqual(
        4,
        try syscall(linux.sendto(fds[1], "ping", 4, linux.MSG.NOSIGNAL, null, 0)),
    );

    frame = .{
        .coroutine = .{
            .op = io.Recv.init(&loop, .{
                .fd = fds[0],
                .buffer = &buffer,
                .deadline = deadline(30_000),
            }),
        },
    };

    loop.executor.spawn(&frame.task);
    run(&loop);

    try testing.expectEqual(4, try frame.getResult());
    try testing.expectEqualStrings("ping", &buffer);
    try testing.expectEqual(null, frame.coroutine.op.primaryError());
    try testing.expectEqual(linux.E.CANCELED, frame.coroutine.op.deadlineError().?);

    frame.coroutine.op.deinit();

    try drained(&loop);
}

test "io invalid descriptor preserves kernel errno" {
    var loop = try uring(2);
    defer loop.deinit();
    var buffer: [1]u8 = undefined;
    var frame: co.Frame(Await(io.Recv)) = .{
        .coroutine = .{
            .op = io.Recv.init(&loop, .{
                .fd = -1,
                .buffer = &buffer,
            }),
        },
    };

    loop.executor.spawn(&frame.task);
    run(&loop);

    try testing.expectError(error.BadFileDescriptor, frame.getResult());
    try testing.expectEqual(linux.E.BADF, frame.coroutine.op.primaryError().?);

    frame.coroutine.op.deinit();

    try drained(&loop);
}

test "io tiny ring drains queued timers and linked receives without losing work" {
    var loop = try uring(2);
    defer loop.deinit();

    const fds = try pair();
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var timers: [20]co.Frame(Await(io.Timeout)) = undefined;
    var recvs: [20]co.Frame(Await(io.Recv)) = undefined;
    var buffers: [20][1]u8 = undefined;

    for (&timers, &recvs, &buffers) |*timer, *recv, *buffer| {
        timer.* = .{
            .coroutine = .{
                .op = io.Timeout.init(&loop, .{
                    .duration = .{ .sec = 0, .nsec = 100_000 },
                }),
            },
        };
        recv.* = .{
            .coroutine = .{
                .op = io.Recv.init(&loop, .{
                    .fd = fds[0],
                    .buffer = buffer,
                    .deadline = deadline(10),
                }),
            },
        };

        loop.executor.spawn(&timer.task);
        loop.executor.spawn(&recv.task);
    }

    run(&loop);

    for (&timers, &recvs) |*timer, *recv| {
        try timer.getResult();
        try testing.expectError(error.Timeout, recv.getResult());
        try testing.expectEqual(linux.E.TIME, recv.coroutine.op.deadlineError().?);
        try testing.expectEqual(linux.E.CANCELED, recv.coroutine.op.primaryError().?);
        try testing.expectEqual(0, timer.task.outstanding_io);
        try testing.expectEqual(0, recv.task.outstanding_io);

        timer.coroutine.op.deinit();
        recv.coroutine.op.deinit();
    }

    try drained(&loop);
}

test "io linked completion orders never wake early and preserve result precedence" {
    const Case = struct {
        primary: i32,
        timer: i32,
        result: io.Error!usize,
    };
    const cases = [_]Case{
        .{ .primary = 3, .timer = negative(.CANCELED), .result = 3 },
        .{ .primary = 3, .timer = negative(.TIME), .result = 3 },
        .{ .primary = 3, .timer = negative(.INVAL), .result = 3 },
        .{ .primary = 0, .timer = negative(.CANCELED), .result = 0 },
        .{
            .primary = negative(.CANCELED),
            .timer = negative(.TIME),
            .result = error.Timeout,
        },
        .{
            .primary = negative(.BADF),
            .timer = negative(.CANCELED),
            .result = error.BadFileDescriptor,
        },
        .{
            .primary = negative(.CANCELED),
            .timer = negative(.INVAL),
            .result = error.InvalidArgument,
        },
        .{
            .primary = negative(.CANCELED),
            .timer = negative(.ALREADY),
            .result = error.OperationCanceled,
        },
        .{
            .primary = negative(.ACCES),
            .timer = negative(.CANCELED),
            .result = error.OsError,
        },
    };

    for (cases) |case| for ([_]bool{ false, true }) |timer_first| {
        // Fabricated CQEs isolate ordering from the kernel's scheduling choices.
        var loop: io.Loop = .{ .ring = undefined };
        var buffer: [3]u8 = undefined;
        var op = io.Recv.init(&loop, .{
            .fd = -1,
            .buffer = &buffer,
            .deadline = .{ .sec = 1, .nsec = 0 },
        });
        var task: co.Task = .{
            .executor = &loop.executor,
            .poll_fn = struct {
                fn poll(_: *co.Task) bool {
                    return true;
                }
            }.poll,
        };

        try testing.expect(op.poll(&task) == .pending);

        loop.pending.remove(&op.operation.node);
        var sqe: linux.io_uring_sqe = undefined;
        var timer: linux.io_uring_sqe = undefined;
        op.operation.prepare(&sqe, &timer);

        try testing.expectEqual(@intFromPtr(&op.operation), sqe.user_data);
        try testing.expectEqual(sqe.user_data | 1, timer.user_data);

        op.operation.complete(timer_first, if (timer_first) case.timer else case.primary);

        try testing.expectEqual(null, loop.executor.ready.first);
        try testing.expectEqual(1, loop.active);
        try testing.expectEqual(1, task.outstanding_io);
        try testing.expect(op.poll(&task) == .pending);

        op.operation.complete(!timer_first, if (timer_first) case.primary else case.timer);

        try testing.expect(loop.executor.ready.first == &task.node);
        const result = op.poll(&task).ready;
        if (case.result) |expected| {
            try testing.expectEqual(expected, try result);
        } else |err| try testing.expectError(err, result);
        try testing.expectEqual(case.primary, op.operation.primary.?);
        try testing.expectEqual(case.timer, op.operation.timer.?);
        try testing.expectEqual(0, loop.active);
        try testing.expectEqual(0, task.outstanding_io);

        _ = loop.executor.runOne();
        op.deinit();
    };
}

test "io yielding peer cannot starve a timer" {
    const Spinner = struct {
        stop: *bool,
        polls: usize = 0,
        yield: co.Yield = .{},

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
            self.polls += 1;
            if (self.stop.*) return .{ .ready = {} };

            while (true) switch (self.yield.poll(task)) {
                .pending => return .pending,
                .ready => {},
            };
        }
    };

    const Stopper = struct {
        stop: *bool,
        op: io.Timeout,

        pub fn poll(self: *@This(), task: *co.Task) co.Poll(io.Error!void) {
            switch (self.op.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    self.stop.* = true;
                    return .{ .ready = result };
                },
            }
        }
    };

    var loop = try uring(2);
    defer loop.deinit();
    var stop = false;
    var spinner: co.Frame(Spinner) = .{ .coroutine = .{ .stop = &stop } };
    var stopper: co.Frame(Stopper) = .{
        .coroutine = .{
            .stop = &stop,
            .op = io.Timeout.init(&loop, .{
                .duration = .{ .sec = 0, .nsec = 2_000_000 },
            }),
        },
    };

    loop.executor.spawn(&spinner.task);
    loop.executor.spawn(&stopper.task);
    run(&loop);

    try stopper.getResult();
    try testing.expect(spinner.result != null);
    try testing.expect(spinner.coroutine.polls > 1);

    stopper.coroutine.op.deinit();

    try drained(&loop);
}

test "io loopback TCP connect echo partial transfers and async close" {
    var loop = try uring(8);
    defer loop.deinit();

    const listener: linux.fd_t = @intCast(try syscall(linux.socket(
        linux.AF.INET,
        linux.SOCK.STREAM | linux.SOCK.CLOEXEC | linux.SOCK.NONBLOCK,
        0,
    )));
    defer _ = linux.close(listener);
    var address: linux.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };

    _ = try syscall(linux.bind(listener, @ptrCast(&address), @sizeOf(@TypeOf(address))));
    _ = try syscall(linux.listen(listener, 1));
    var len: linux.socklen_t = @sizeOf(@TypeOf(address));
    _ = try syscall(linux.getsockname(listener, @ptrCast(&address), &len));

    try testing.expect(address.port != 0);

    const client: linux.fd_t = @intCast(try syscall(linux.socket(
        linux.AF.INET,
        linux.SOCK.STREAM | linux.SOCK.CLOEXEC,
        0,
    )));
    var client_open = true;
    defer if (client_open) {
        _ = linux.close(client);
    };
    var connect: co.Frame(Await(io.Connect)) = .{
        .coroutine = .{
            .op = io.Connect.init(&loop, .{
                .fd = client,
                .address = @ptrCast(&address),
                .address_len = len,
                .deadline = deadline(2_000),
            }),
        },
    };

    loop.executor.spawn(&connect.task);
    run(&loop);

    try connect.getResult();

    connect.coroutine.op.deinit();

    // Connect completion guarantees a queued connection; NONBLOCK makes setup fail rather than hang.
    const server: linux.fd_t = @intCast(try syscall(linux.accept4(
        listener,
        null,
        null,
        linux.SOCK.CLOEXEC,
    )));
    var server_open = true;
    defer if (server_open) {
        _ = linux.close(server);
    };

    var sent: [2048]u8 = undefined;
    for (&sent, 0..) |*byte, i| byte.* = @truncate(i * 37);
    const expected = sent;
    var echoed: [sent.len]u8 = undefined;
    var a: co.Frame(support.Echo) = .{
        .coroutine = .{
            .loop = &loop,
            .fd = client,
            .buffer = &sent,
            .sending = true,
            .until = deadline(2_000),
        },
    };
    var b: co.Frame(support.Echo) = .{
        .coroutine = .{
            .loop = &loop,
            .fd = server,
            .buffer = &echoed,
            .sending = false,
            .until = deadline(2_000),
        },
    };

    loop.executor.spawn(&a.task);
    loop.executor.spawn(&b.task);
    run(&loop);

    try a.getResult();
    try b.getResult();
    try testing.expectEqualSlices(u8, &expected, &sent);
    try testing.expectEqualSlices(u8, &expected, &echoed);
    try drained(&loop);

    var closes: [2]co.Frame(Await(io.Close)) = undefined;

    for (&closes, [_]linux.fd_t{ client, server }) |*frame, fd| {
        frame.* = .{ .coroutine = .{ .op = io.Close.init(&loop, .{ .fd = fd }) } };
        loop.executor.spawn(&frame.task);
    }

    run(&loop);

    closes[0].getResult() catch @panic("async client close failed");
    client_open = false;
    closes[1].getResult() catch @panic("async server close failed");
    server_open = false;

    for (&closes) |*frame| frame.coroutine.op.deinit();

    try drained(&loop);
}

test "io pending sends unblock submitted receives with tiny ring" {
    var loop = try uring(2);
    defer loop.deinit();

    const fds = try pair();
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var bytes: [8][1]u8 = undefined;
    var recvs: [8]co.Frame(Await(io.Recv)) = undefined;
    var sends: [8]co.Frame(Await(io.Send)) = undefined;

    for (&recvs, &bytes) |*recv, *buffer| {
        recv.* = .{
            .coroutine = .{
                .op = io.Recv.init(&loop, .{
                    .fd = fds[0],
                    .buffer = buffer,
                }),
            },
        };

        loop.executor.spawn(&recv.task);
    }

    for (&sends) |*send| {
        send.* = .{
            .coroutine = .{
                .op = io.Send.init(&loop, .{
                    .fd = fds[1],
                    .buffer = "x",
                }),
            },
        };

        loop.executor.spawn(&send.task);
    }

    run(&loop);

    for (&recvs, &sends, &bytes) |*recv, *send, *buffer| {
        try testing.expectEqual(1, try recv.getResult());
        try testing.expectEqual(1, try send.getResult());
        try testing.expectEqualStrings("x", buffer);
        try testing.expectEqual(0, recv.task.outstanding_io);
        try testing.expectEqual(0, send.task.outstanding_io);

        recv.coroutine.op.deinit();
        send.coroutine.op.deinit();
    }

    try drained(&loop);
}

test "io cancellation retains operation through all three CQE orders" {
    const orders = [_][3]u2{
        .{ 0, 1, 2 },
        .{ 0, 2, 1 },
        .{ 1, 0, 2 },
        .{ 1, 2, 0 },
        .{ 2, 0, 1 },
        .{ 2, 1, 0 },
    };

    for ([_]bool{ false, true }) |succeeded| for (orders) |order| {
        var loop: io.Loop = .{};
        var buffer: [1]u8 = undefined;
        var op = io.Recv.init(&loop, .{
            .fd = -1,
            .buffer = &buffer,
            .deadline = .{ .sec = 1, .nsec = 0 },
        });
        var task: co.Task = .{
            .executor = &loop.executor,
            .poll_fn = struct {
                fn poll(_: *co.Task) bool {
                    return true;
                }
            }.poll,
        };

        try testing.expect(op.poll(&task) == .pending);

        loop.pending.remove(&op.operation.node);
        var primary: linux.io_uring_sqe = undefined;
        var timer: linux.io_uring_sqe = undefined;
        op.operation.prepare(&primary, &timer);
        op.operation.native.uring.cancel = .submitted;

        for (order, 0..) |tag, index| {
            switch (tag) {
                0 => op.operation.complete(false, if (succeeded) 1 else negative(.CANCELED)),
                1 => op.operation.complete(true, negative(.CANCELED)),
                2 => @import("../backends/Uring.zig").cancelComplete(&op.operation),
                else => unreachable,
            }

            if (index < 2) {
                try testing.expect(op.poll(&task) == .pending);
                try testing.expectEqual(null, loop.executor.ready.first);
                try testing.expectEqual(1, task.outstanding_io);
            }
        }

        const result = op.poll(&task).ready;
        if (succeeded)
            try testing.expectEqual(1, try result)
        else
            try testing.expectError(error.OperationCanceled, result);
        try testing.expectEqual(0, task.outstanding_io);
        try testing.expectEqual(0, loop.active);

        _ = loop.executor.runOne();
        op.deinit();
    };
}

test "io fixed buffers select SQE opcode and index using the clamped contained range" {
    // Preparation never dereferences these ranges, including the oversized and wrapping ones.
    const Case = struct {
        address: usize,
        len: usize,
        region_len: usize = 16,
        index: ?u16 = null,
    };
    const limit = std.math.maxInt(i32);
    const cases = [_]Case{
        .{ .address = 0x10000, .len = 16, .index = 0 },
        .{ .address = 0x10003, .len = 5, .index = 0 },
        .{ .address = 0x20000, .len = 16, .index = 1 },
        .{ .address = 0x2000f, .len = 1, .index = 1 },
        .{ .address = 0xffff, .len = 2 },
        .{ .address = 0x1000f, .len = 2 },
        .{ .address = 0x10010, .len = 1 },
        .{ .address = 0x1ffff, .len = 2 },
        .{ .address = 0x2000f, .len = 2 },
        .{ .address = 0xffff, .len = 0x10012 },
        .{ .address = 0x10008, .len = 0x10000 },
        .{ .address = 0x30000, .len = 4 },
        .{ .address = std.math.maxInt(usize) - 7, .len = 16 },
        .{ .address = 0x10000, .len = limit + 17, .region_len = limit, .index = 0 },
        .{ .address = 0x10000, .len = limit + 17, .region_len = limit - 1 },
        .{ .address = 0x10001, .len = limit + 17, .region_len = limit },
    };

    inline for (.{ io.Kind.read_at, .write_at }) |kind| {
        for (cases) |case| {
            const first: [*]u8 = @ptrFromInt(0x10000);
            const second: [*]u8 = @ptrFromInt(0x20000);
            const regions = [_][]u8{ first[0..case.region_len], second[0..16] };
            var loop: io.Loop = .{ .buffers = &regions };
            const pointer: [*]u8 = @ptrFromInt(case.address);
            var op = io.Operation.Coroutine(kind).init(&loop, .{
                .file = .stdout(),
                .buffer = pointer[0..case.len],
                .offset = 17,
            });
            var sqe: linux.io_uring_sqe = undefined;
            op.operation.prepare(&sqe, null);

            const expected: linux.IORING_OP = if (case.index != null)
                (if (kind == .read_at) linux.IORING_OP.READ_FIXED else .WRITE_FIXED)
            else
                (if (kind == .read_at) linux.IORING_OP.READ else .WRITE);
            try testing.expectEqual(expected, sqe.opcode);
            try testing.expectEqual(case.index orelse 0, sqe.buf_index);
            try testing.expectEqual(case.address, sqe.addr);
            try testing.expectEqual(@min(case.len, limit), sqe.len);
            try testing.expectEqual(std.Io.File.stdout().handle, sqe.fd);
            try testing.expectEqual(17, sqe.off);
            try testing.expectEqual(0, sqe.flags);
            try testing.expectEqual(0, sqe.rw_flags);
            try testing.expectEqual(@intFromPtr(&op.operation), sqe.user_data);
        }
    }
}

test "io fixed buffers register once and mix kernel file transfers with ordinary memory" {
    var memory: [2][64]u8 = undefined;
    const regions = [_][]u8{ &memory[0], &memory[1] };
    var loop = try registeredLoop(&regions);
    defer loop.deinit();
    if (!loop.file_ops.read or !loop.file_ops.write) return error.SkipZigTest;
    try testing.expectEqual(regions[0..].ptr, loop.buffers.ptr);
    try testing.expectEqual(regions[0].ptr, loop.buffers[0].ptr);
    try testing.expectEqual(regions[1].ptr, loop.buffers[1].ptr);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(testing.io, "fixed", .{ .read = true });
    defer file.close(testing.io);
    @memcpy(memory[0][5..9], "ping");
    var ordinary: [4]u8 = undefined;

    for ([_][]const u8{ memory[0][5..9], "pong" }, [_][]u8{ memory[1][7..11], &ordinary }) |input, output| {
        var write: co.Frame(io.Operation.Coroutine(.write_at)) = .{
            .coroutine = .init(&loop, .{ .file = file, .buffer = input, .offset = 3 }),
        };
        loop.executor.spawn(&write.task);
        run(&loop);
        try testing.expectEqual(4, try write.getResult());
        write.coroutine.deinit();
        try drained(&loop);

        var read: co.Frame(io.Operation.Coroutine(.read_at)) = .{
            .coroutine = .init(&loop, .{ .file = file, .buffer = output, .offset = 3 }),
        };
        loop.executor.spawn(&read.task);
        run(&loop);
        try testing.expectEqual(4, try read.getResult());
        try testing.expectEqualStrings(input, output);
        read.coroutine.deinit();
        try drained(&loop);
    }

    var short: co.Frame(io.Operation.Coroutine(.read_at)) = .{
        .coroutine = .init(&loop, .{ .file = file, .buffer = memory[1][0..16], .offset = 5 }),
    };
    loop.executor.spawn(&short.task);
    run(&loop);
    try testing.expectEqual(2, try short.getResult());
    try testing.expectEqualStrings("ng", memory[1][0..2]);
    short.coroutine.deinit();
    try drained(&loop);
}

test "io fixed buffers retain registered memory through kernel cancellation and drain" {
    var memory: [64]u8 = undefined;
    const regions = [_][]u8{&memory};
    var loop = try registeredLoop(&regions);
    defer loop.deinit();
    if (!loop.file_ops.read) return error.SkipZigTest;
    const fds = try pair();
    defer for (fds) |fd| {
        _ = linux.close(fd);
    };

    var read: co.Frame(io.Operation.Coroutine(.read_at)) = .{
        .coroutine = .init(&loop, .{
            .file = .{ .handle = fds[0], .flags = .{ .nonblocking = false } },
            .buffer = memory[3..7],
        }),
    };
    loop.executor.spawn(&read.task);
    loop.executor.run();
    const op = &read.coroutine.operation;
    loop.pending.remove(&op.node);
    const sqe = try loop.ring.get_sqe();
    op.prepare(sqe, null);
    try testing.expectEqual(linux.IORING_OP.READ_FIXED, sqe.opcode);
    loop.outstanding_cqes += 1;
    try testing.expectEqual(1, try loop.ring.submit());

    read.coroutine.cancel();
    run(&loop);
    try testing.expectError(error.OperationCanceled, read.getResult());
    try testing.expectEqual(.done, op.native.uring.cancel);
    try testing.expectEqual(0, read.task.outstanding_io);
    read.coroutine.deinit();
    try drained(&loop);
}

test "io fixed buffers reject excess and invalid registration without automatic fallback" {
    var available = try uring(2);
    available.deinit();
    var byte: [1]u8 = undefined;
    const regions: [65][]u8 = @splat(&byte);
    const inaccessible: [*]u8 = @ptrFromInt(1);
    const invalid = [_][]u8{inaccessible[0..1]};
    for ([_]io.Loop.Backend{ .uring, .auto }) |mode| {
        try testing.expectError(error.TooManyBuffers, io.Loop.initWithBackend(.{ .buffers = &regions }, mode));
        try testing.expectError(error.InvalidArgument, io.Loop.initWithBackend(.{ .buffers = &invalid }, mode));
    }

    var maximum = try registeredLoop(regions[0..64]);
    defer maximum.deinit();
    try testing.expectEqual(64, maximum.buffers.len);
    run(&maximum);
    try drained(&maximum);
}

test "io epoll ignores fixed buffer descriptors and registration limits" {
    const inaccessible: [*]u8 = @ptrFromInt(1);
    const regions: [65][]u8 = @splat(inaccessible[0..1]);
    var loop = try io.Loop.initWithBackend(.{ .buffers = &regions }, .epoll);
    defer loop.deinit();
    try testing.expect(loop.epoll != null);
    try testing.expectEqual(0, loop.buffers.len);
    run(&loop);
    try support.drained(&loop);
}
