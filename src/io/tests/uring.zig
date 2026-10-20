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

test "io kernel timeout wakes once and operation can be reused after ready" {
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

test "io yielding peer cannot starve kernel timeout" {
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
