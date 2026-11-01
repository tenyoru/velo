const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const net = velo.net;

// One connection: echo until the peer closes. The server accepts, owns and closes the stream.
const Echo = struct {
    stream: net.Stream,
    buffer: [4096]u8 = undefined,

    pub const Result = anyerror!void;
    pub const State = union(enum) { recv: io.Recv, send: net.SendAll };
    const Next = co.Next(@This());

    pub fn start(self: *Echo) Next {
        return .{ .go = .{ .recv = self.stream.recv(&self.buffer) } };
    }

    pub fn recv(self: *Echo, result: io.Error!usize) Next {
        const n = result catch |err| return finish(err);
        if (n == 0) return .{ .done = {} };

        return .{ .go = .{ .send = self.stream.sendAll(self.buffer[0..n]) } };
    }

    pub fn send(self: *Echo, result: co.ResultOf(net.SendAll)) Next {
        result catch |err| return finish(err);
        return self.start();
    }

    // Shutdown cancels live connections; that is a normal end, not a failure.
    fn finish(err: anyerror) Next {
        return .{ .done = if (err == error.OperationCanceled) {} else err };
    }
};

fn connect(_: void, stream: net.Stream) co.Machine(Echo) {
    return .init(.{ .stream = stream });
}

const Server = net.Server(connect);

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

    var pool: velo.workers.Pool = undefined;
    try pool.init(init.gpa, &loop, 1);
    defer pool.deinit();

    // Each slot holds one connection's handler and buffer; a full server stops accepting.
    var slots: [16]Server.Slot = undefined;
    var server: co.Frame(Server) = .init(try .init(&loop, try .parseLiteral("127.0.0.1:0"), {}, &slots, .{}));
    defer server.coroutine.deinit();

    var stopper: co.Frame(Stopper) = .init(.{ .loop = &loop, .input = .init(&pool, .{}) });

    loop.spawn(&server);
    std.debug.print(
        "Listening on {f}; press Enter or send stdin EOF to stop.\n",
        .{server.coroutine.listener.address},
    );

    // Runs until Enter or EOF shuts down the loop and every connection drains.
    try loop.block(&stopper);
    try server.getResult();

    const stats = server.coroutine.stats;
    std.debug.print("Served {d} connections, {d} failed.\n", .{ stats.accepted, stats.failed });
}

test "echo server answers a client and drains on loop shutdown" {
    const Client = struct {
        loop: *io.Loop,
        address: net.Address,
        stream: ?net.Stream = null,
        reply: [5]u8 = undefined,

        pub const Result = anyerror!void;
        pub const State = union(enum) {
            dial: net.Dial,
            ping: co.Join(.{ .send = net.SendAll, .read = net.ReadExact }),
        };
        const Next = co.Next(@This());

        pub fn start(self: *@This()) Next {
            const until = io.deadlineIn(io.seconds(5)) catch |err| return self.done(err);
            return .{ .go = .{ .dial = .init(self.loop, self.address, .{ .deadline = until }) } };
        }

        pub fn dial(self: *@This(), result: io.Error!net.Stream) Next {
            self.stream = result catch |err| return self.done(err);
            self.stream.?.deadline = io.deadlineIn(io.seconds(5)) catch |err| return self.done(err);
            const stream = self.stream.?;

            return .{ .go = .{ .ping = .init(.{
                .send = stream.sendAll("hello"),
                .read = stream.readExact(&self.reply),
            }) } };
        }

        pub fn ping(self: *@This(), result: co.ResultOf(co.Join(.{ .send = net.SendAll, .read = net.ReadExact }))) Next {
            result.send catch |err| return self.done(err);
            result.read catch |err| return self.done(err);

            return self.done(if (std.mem.eql(u8, &self.reply, "hello")) {} else error.WrongEcho);
        }

        // Shutdown is what ends the server, so every outcome triggers it.
        fn done(self: *@This(), result: anyerror!void) Next {
            if (self.stream) |stream| stream.close();
            self.loop.shutdown();
            return .{ .done = result };
        }
    };

    var loop = try io.Loop.init(.{ .entries = 16 });
    defer loop.deinit();

    var slots: [2]Server.Slot = undefined;
    var server: co.Frame(Server) = .init(try .init(&loop, try .parseLiteral("127.0.0.1:0"), {}, &slots, .{}));
    defer server.coroutine.deinit();

    var client: co.Frame(co.Machine(Client)) = .init(.init(.{
        .loop = &loop,
        .address = server.coroutine.listener.address,
    }));

    loop.spawn(&client);
    try loop.block(&server);
    try client.getResult();

    try std.testing.expectEqual(1, server.coroutine.stats.accepted);
    try std.testing.expectEqual(0, server.coroutine.stats.failed);
}
