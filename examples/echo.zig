const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.io;

const Handler = struct {
    loop: *io.Loop,
    listener: io.Socket,
    fd: ?io.Socket = null,
    buffer: [4096]u8 = undefined,
    accept: io.Accept,
    recv: io.Recv = undefined,
    send: io.SendAll = undefined,
    state: enum { accept, recv, send } = .accept,

    pub fn cancel(self: *@This()) void {
        switch (self.state) {
            .accept => self.accept.cancel(),
            .recv => self.recv.cancel(),
            .send => self.send.cancel(),
        }
    }

    fn cleanup(self: *@This()) void {
        if (self.fd) |fd| close(fd);
        self.fd = null;
    }

    fn disconnected(self: *@This(), task: *co.Task, result: anyerror!void) co.Poll(anyerror!void) {
        // The active child has returned ready, so nothing still borrows this socket.
        self.cleanup();

        result catch |err| {
            if (err == error.OperationCanceled) return .{ .ready = {} };

            std.debug.print("connection: {s}\n", .{@errorName(err)});
        };

        if (task.cancel_requested or self.loop.control.isShuttingDown()) return .{ .ready = {} };

        self.accept = io.Accept.init(self.loop, .{ .fd = self.listener });
        self.state = .accept;
        task.wake();
        return .pending;
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
        sw: switch (self.state) {
            .accept => {
                self.fd = switch (self.accept.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result catch |err| return .{
                        .ready = if (err == error.OperationCanceled) {} else err,
                    },
                };

                self.recv = io.Recv.init(self.loop, .{
                    .fd = self.fd.?,
                    .buffer = &self.buffer,
                });
                self.state = .recv;
                continue :sw .recv;
            },
            .recv => {
                const n = switch (self.recv.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result catch |err| return self.disconnected(task, err),
                };

                if (n == 0) return self.disconnected(task, {});

                self.send = io.SendAll.init(self.loop, .{
                    .fd = self.fd.?,
                    .buffer = self.buffer[0..n],
                });
                self.state = .send;
                continue :sw .send;
            },
            .send => {
                switch (self.send.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result catch |err| return self.disconnected(task, err),
                }

                self.recv = io.Recv.init(self.loop, .{
                    .fd = self.fd.?,
                    .buffer = &self.buffer,
                });
                self.state = .recv;
                continue :sw .recv;
            },
        }
    }
};

const Input = struct {
    pub fn run(_: *@This(), std_io: std.Io) !void {
        var byte: [1]u8 = undefined;

        while (true) {
            const n = std.Io.File.stdin().readStreaming(
                std_io,
                &.{&byte},
            ) catch |err| switch (err) {
                error.EndOfStream => return,
                else => return err,
            };

            if (n != 0 and byte[0] == '\n') return;
        }
    }
};

const Stopper = struct {
    loop: *io.Loop,
    input: io.WorkerPool.Work(Input),

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
        const result = switch (self.input.poll(task)) {
            .pending => return .pending,
            .ready => |result| result,
        };

        // Shutdown cannot interrupt a running blocking read; finish it before joining.
        self.loop.shutdown();
        return .{ .ready = result };
    }
};

fn close(fd: io.Socket) void {
    io.closeSocket(fd) catch |err| std.debug.panic("closeSocket: {s}", .{@errorName(err)});
}

pub fn main(init: std.process.Init) !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    const listener = try io.tcpSocket(.ipv4);
    defer close(listener);

    var address: io.SockAddr.in = .{
        .port = 0,
        .addr = std.mem.nativeToBig(u32, 0x7f000001),
    };
    var len: io.SockLen = @sizeOf(@TypeOf(address));

    try io.bind(listener, @ptrCast(&address), len);
    try io.listen(listener, 16);
    try io.localAddress(listener, @ptrCast(&address), &len);

    var pool: io.WorkerPool = undefined;
    try pool.init(init.gpa, &loop, 1);
    defer pool.deinit();

    var handlers: [4]co.Frame(Handler) = undefined;

    for (&handlers) |*frame| {
        frame.* = .{
            .coroutine = .{
                .loop = &loop,
                .listener = listener,
                .accept = io.Accept.init(&loop, .{ .fd = listener }),
            },
        };

        loop.executor.spawn(&frame.task);
    }
    defer for (&handlers) |*frame| frame.coroutine.cleanup();

    var stopper: co.Frame(Stopper) = .{
        .coroutine = .{
            .loop = &loop,
            .input = .init(&pool, .{}),
        },
    };

    loop.executor.spawn(&stopper.task);

    std.debug.print(
        "Listening on 127.0.0.1:{d}; press Enter or send stdin EOF to stop.\n",
        .{std.mem.bigToNative(u16, address.port)},
    );

    // A fatal run error leaves live frames borrowed by the kernel; never unwind them.
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    try stopper.result.?;
    for (&handlers) |*frame| try frame.result.?;
}

test "handler cancellation consumes queued children before closing sockets" {
    inline for (.{ false, true }) |receiving| {
        var loop = try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();

        const fd = try io.tcpSocket(.ipv4);
        defer if (!receiving) close(fd);

        var frame: co.Frame(Handler) = .{
            .coroutine = .{
                .loop = &loop,
                .listener = fd,
                .accept = io.Accept.init(&loop, .{ .fd = fd }),
            },
        };

        if (receiving) {
            frame.coroutine.fd = fd;
            frame.coroutine.state = .recv;
            frame.coroutine.recv = io.Recv.init(&loop, .{
                .fd = fd,
                .buffer = &frame.coroutine.buffer,
            });
        }

        loop.executor.spawn(&frame.task);
        try std.testing.expect(loop.executor.runOne());

        try std.testing.expectEqual(1, frame.task.outstanding_io);

        frame.cancel();
        loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

        try frame.result.?;
        try std.testing.expectEqual(null, frame.coroutine.fd);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}
