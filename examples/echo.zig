const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;

// One connection slot: accept, echo until the peer leaves, then accept again.
const Connection = struct {
    listener: *const io.Listener,
    /// Set before spawning to start echoing on an already accepted stream.
    stream: ?io.Stream = null,
    buffer: [4096]u8 = undefined,

    pub const Result = anyerror!void;
    pub const State = union(enum) { accept: io.Accept, recv: io.Recv, send: io.SendAll };
    const Next = co.Next(@This());

    pub fn start(self: *@This()) Next {
        if (self.stream != null) return self.receive();

        return .{ .go = .{ .accept = self.listener.accept(.{}) } };
    }

    pub fn accept(self: *@This(), result: io.Error!io.Socket) Next {
        // Cancellation and shutdown end the slot; reaching here means no socket to close.
        const fd = result catch |err| return .{
            .done = if (err == error.OperationCanceled) {} else err,
        };

        self.stream = .init(self.listener.loop, fd);
        return self.receive();
    }

    pub fn recv(self: *@This(), result: io.Error!usize) Next {
        const n = result catch |err| return self.disconnected(err);
        if (n == 0) return self.disconnected({});

        return .{ .go = .{ .send = self.stream.?.sendAll(self.buffer[0..n]) } };
    }

    pub fn send(self: *@This(), result: co.ResultOf(io.SendAll)) Next {
        result catch |err| return self.disconnected(err);

        return self.receive();
    }

    fn receive(self: *@This()) Next {
        return .{ .go = .{ .recv = self.stream.?.recv(&self.buffer) } };
    }

    fn disconnected(self: *@This(), result: anyerror!void) Next {
        // The finished child no longer borrows this stream.
        self.cleanup();

        result catch |err| if (err != error.OperationCanceled)
            std.debug.print("connection: {s}\n", .{@errorName(err)});

        // After cancellation or shutdown, the next accept fails fast and ends the slot.
        return self.start();
    }

    fn cleanup(self: *@This()) void {
        if (self.stream) |stream| stream.close();
        self.stream = null;
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

pub fn main(init: std.process.Init) !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    // Port 0 lets the kernel choose; `listener.address` reports the bound port.
    var listener: io.Listener = try .init(&loop, try .parseLiteral("127.0.0.1:0"), .{ .backlog = 16 });
    defer listener.deinit();

    var pool: velo.workers.Pool = undefined;
    try pool.init(init.gpa, &loop, 1);
    defer pool.deinit();

    var handlers: [4]co.Frame(Handler) = undefined;

    for (&handlers) |*frame| {
        frame.* = .init(.init(.{ .listener = &listener }));
        loop.spawn(frame);
    }
    defer for (&handlers) |*frame| frame.coroutine.ctx.cleanup();

    var stopper: co.Frame(Stopper) = .init(.{ .loop = &loop, .input = .init(&pool, .{}) });

    std.debug.print(
        "Listening on {f}; press Enter or send stdin EOF to stop.\n",
        .{listener.address},
    );

    // Runs until Enter or EOF shuts down the loop and every handler drains.
    try loop.block(&stopper);
    for (&handlers) |*frame| try frame.getResult();
}

test "handler cancellation consumes queued children before closing sockets" {
    inline for (.{ false, true }) |receiving| {
        var loop = try io.Loop.init(.{ .entries = 8 });
        defer loop.deinit();

        // An unbound socket parks accepts and receives until canceled.
        var listener: io.Listener = .{ .loop = &loop, .fd = try io.tcpSocket(.ipv4), .address = undefined };
        defer if (!receiving) listener.deinit();

        var frame: co.Frame(Handler) = .init(.init(.{
            .listener = &listener,
            .stream = if (receiving) .init(&loop, listener.fd) else null,
        }));

        loop.spawn(&frame);
        try std.testing.expect(loop.executor.runOne());

        try std.testing.expectEqual(1, frame.task.outstanding_io);

        frame.cancel();
        loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

        try frame.getResult();
        try std.testing.expectEqual(null, frame.coroutine.ctx.stream);
        try std.testing.expectEqual(0, frame.task.outstanding_io);
    }
}
