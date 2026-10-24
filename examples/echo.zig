const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;

// One connection slot: accept, echo until the peer leaves, then accept again.
const Connection = struct {
    loop: *io.Loop,
    listener: io.Socket,
    /// Set before spawning to start echoing on an already accepted socket.
    fd: ?io.Socket = null,
    buffer: [4096]u8 = undefined,

    pub const Result = anyerror!void;
    pub const State = union(enum) { accept: io.Accept, recv: io.Recv, send: io.SendAll };
    const Next = co.Next(@This());

    pub fn start(self: *@This()) Next {
        if (self.fd != null) return self.receive();

        return .{ .go = .{ .accept = io.Accept.init(self.loop, .{ .fd = self.listener }) } };
    }

    pub fn accept(self: *@This(), result: io.Error!io.Socket) Next {
        // Cancellation and shutdown end the slot; reaching here means no socket to close.
        self.fd = result catch |err| return .{
            .done = if (err == error.OperationCanceled) {} else err,
        };

        return self.receive();
    }

    pub fn recv(self: *@This(), result: io.Error!usize) Next {
        const n = result catch |err| return self.disconnected(err);
        if (n == 0) return self.disconnected({});

        return .{ .go = .{ .send = io.SendAll.init(self.loop, .{
            .fd = self.fd.?,
            .buffer = self.buffer[0..n],
        }) } };
    }

    pub fn send(self: *@This(), result: co.ResultOf(io.SendAll)) Next {
        result catch |err| return self.disconnected(err);

        return self.receive();
    }

    fn receive(self: *@This()) Next {
        return .{ .go = .{ .recv = io.Recv.init(self.loop, .{
            .fd = self.fd.?,
            .buffer = &self.buffer,
        }) } };
    }

    fn disconnected(self: *@This(), result: anyerror!void) Next {
        // The finished child no longer borrows this socket.
        self.cleanup();

        result catch |err| if (err != error.OperationCanceled)
            std.debug.print("connection: {s}\n", .{@errorName(err)});

        // After cancellation or shutdown, the next accept fails fast and ends the slot.
        return self.start();
    }

    fn cleanup(self: *@This()) void {
        if (self.fd) |fd| close(fd);
        self.fd = null;
    }
};

const Handler = co.Machine(Connection);

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
    input: velo.workers.Pool.Work(Input),

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
        const result = self.input.poll(task).value() orelse return .pending;

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

    var pool: velo.workers.Pool = undefined;
    try pool.init(init.gpa, &loop, 1);
    defer pool.deinit();

    var handlers: [4]co.Frame(Handler) = undefined;

    for (&handlers) |*frame| {
        frame.* = .init(.init(.{ .loop = &loop, .listener = listener }));
        loop.spawn(frame);
    }
    defer for (&handlers) |*frame| frame.coroutine.ctx.cleanup();

    var stopper: co.Frame(Stopper) = .init(.{ .loop = &loop, .input = .init(&pool, .{}) });

    std.debug.print(
        "Listening on 127.0.0.1:{d}; press Enter or send stdin EOF to stop.\n",
        .{std.mem.bigToNative(u16, address.port)},
    );

    // Runs until Enter or EOF shuts down the loop and every handler drains.
    try loop.block(&stopper);
    for (&handlers) |*frame| try frame.getResult();
}

test "handler cancellation consumes queued children before closing sockets" {
    inline for (.{ false, true }) |receiving| {
        var loop = try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();

        const fd = try io.tcpSocket(.ipv4);
        defer if (!receiving) close(fd);

        var frame: co.Frame(Handler) = .init(.init(.{
            .loop = &loop,
            .listener = fd,
            .fd = if (receiving) fd else null,
        }));

        loop.spawn(&frame);
        try std.testing.expect(loop.executor.runOne());

        try std.testing.expectEqual(1, frame.task.outstanding_io);

        frame.cancel();
        loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

        try frame.getResult();
        try std.testing.expectEqual(null, frame.coroutine.ctx.fd);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}
