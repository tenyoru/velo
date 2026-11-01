const std = @import("std");
const io = @import("../../event/root.zig");
const co = @import("../../coroutines/root.zig");
const net = @import("../root.zig");
const support = @import("../../event/tests/support.zig");
const testing = std.testing;

const Echo = struct {
    stream: net.Stream,
    buffer: [16]u8 = undefined,

    pub const Result = anyerror!void;
    pub const State = union(enum) { recv: io.Recv, send: net.SendAll };
    const Next = co.Next(@This());

    pub fn start(self: *Echo) Next {
        return .{ .go = .{ .recv = self.stream.recv(&self.buffer) } };
    }

    pub fn recv(self: *Echo, result: io.Error!usize) Next {
        const n = result catch |err| return .{ .done = if (err == error.OperationCanceled) {} else err };
        if (n == 0) return .{ .done = {} };
        if (self.buffer[0] == '!') return .{ .done = error.Rejected };

        return .{ .go = .{ .send = self.stream.sendAll(self.buffer[0..n]) } };
    }

    pub fn send(self: *Echo, result: co.ResultOf(net.SendAll)) Next {
        result catch |err| return .{ .done = err };
        return self.start();
    }
};

fn session(sessions: *usize, stream: net.Stream) co.Machine(Echo) {
    sessions.* += 1;
    return .init(.{ .stream = stream });
}

const App = net.Server(session);
const Ping = co.Join(.{ .send = net.SendAll, .read = net.ReadExact });

/// Client script shared by the tests: every step has a deadline, and finishing for any
/// reason closes its streams and cancels the server so `block` cannot hang.
const Script = struct {
    loop: *io.Loop,
    server: *co.Frame(App),
    until: io.Timespec,
    streams: [2]?net.Stream = @splat(null),
    byte: [1]u8 = undefined,

    const Self = @This();

    fn dial(self: *Self) net.Dial {
        return .init(self.loop, self.server.coroutine.listener.address, .{ .deadline = self.until });
    }

    fn connected(self: *Self, index: usize, result: io.Error!net.Stream) !net.Stream {
        var stream = try result;
        stream.deadline = self.until;
        self.streams[index] = stream;
        return stream;
    }

    fn closeStream(self: *Self, index: usize) void {
        if (self.streams[index]) |stream| stream.close();
        self.streams[index] = null;
    }

    fn ping(self: *Self, index: usize, byte: []const u8) Ping {
        const stream = self.streams[index].?;
        return .init(.{ .send = stream.sendAll(byte), .read = stream.readExact(&self.byte) });
    }

    fn finish(self: *Self, result: anyerror!void) anyerror!void {
        for (0..self.streams.len) |i| self.closeStream(i);
        self.server.cancel();
        return result;
    }
};

fn expectEcho(result: Ping.Result, script: anytype, byte: u8) !void {
    try result.send;
    try result.read;
    if (script.byte[0] != byte) return error.WrongEcho;
}

const Backpressure = struct {
    script: Script,

    pub const Result = anyerror!void;
    pub const State = union(enum) {
        dial_a: net.Dial,
        dial_b: net.Dial,
        ping_a: Ping,
        send_b: net.SendAll,
        idle_b: io.Recv,
        read_b: net.ReadExact,
        reject_b: net.SendAll,
        eof_b: io.Recv,
    };
    const Next = co.Next(@This());

    fn fail(self: *Backpressure, err: anyerror) Next {
        return .{ .done = self.script.finish(err) };
    }

    pub fn start(self: *Backpressure) Next {
        return .{ .go = .{ .dial_a = self.script.dial() } };
    }

    pub fn dial_a(self: *Backpressure, result: io.Error!net.Stream) Next {
        _ = self.script.connected(0, result) catch |err| return self.fail(err);
        return .{ .go = .{ .dial_b = self.script.dial() } };
    }

    pub fn dial_b(self: *Backpressure, result: io.Error!net.Stream) Next {
        _ = self.script.connected(1, result) catch |err| return self.fail(err);
        return .{ .go = .{ .ping_a = self.script.ping(0, "a") } };
    }

    pub fn ping_a(self: *Backpressure, result: Ping.Result) Next {
        expectEcho(result, &self.script, 'a') catch |err| return self.fail(err);
        return .{ .go = .{ .send_b = self.script.streams[1].?.sendAll("b") } };
    }

    pub fn send_b(self: *Backpressure, result: co.ResultOf(net.SendAll)) Next {
        result catch |err| return self.fail(err);

        // The only slot serves `a`, so `b` stays queued in the listen backlog.
        var queued = self.script.streams[1].?;
        queued.deadline = io.deadlineIn(io.milliseconds(50)) catch |err| return self.fail(err);
        return .{ .go = .{ .idle_b = queued.recv(&self.script.byte) } };
    }

    pub fn idle_b(self: *Backpressure, result: io.Error!usize) Next {
        if (result) |_| return self.fail(error.ServedPastCapacity) else |err| {
            if (err != error.Timeout) return self.fail(err);
        }

        self.script.closeStream(0);
        return .{ .go = .{ .read_b = self.script.streams[1].?.readExact(&self.script.byte) } };
    }

    pub fn read_b(self: *Backpressure, result: co.ResultOf(net.ReadExact)) Next {
        result catch |err| return self.fail(err);
        if (self.script.byte[0] != 'b') return self.fail(error.WrongEcho);

        return .{ .go = .{ .reject_b = self.script.streams[1].?.sendAll("!") } };
    }

    pub fn reject_b(self: *Backpressure, result: co.ResultOf(net.SendAll)) Next {
        result catch |err| return self.fail(err);
        return .{ .go = .{ .eof_b = self.script.streams[1].?.recv(&self.script.byte) } };
    }

    pub fn eof_b(self: *Backpressure, result: io.Error!usize) Next {
        const n = result catch |err| return self.fail(err);
        if (n != 0) return self.fail(error.ExpectedEof);

        return .{ .done = self.script.finish({}) };
    }
};

const CancelLive = struct {
    script: Script,

    pub const Result = anyerror!void;
    pub const State = union(enum) { dial: net.Dial, ping: Ping, eof: io.Recv };
    const Next = co.Next(@This());

    fn fail(self: *CancelLive, err: anyerror) Next {
        return .{ .done = self.script.finish(err) };
    }

    pub fn start(self: *CancelLive) Next {
        return .{ .go = .{ .dial = self.script.dial() } };
    }

    pub fn dial(self: *CancelLive, result: io.Error!net.Stream) Next {
        _ = self.script.connected(0, result) catch |err| return self.fail(err);
        return .{ .go = .{ .ping = self.script.ping(0, "x") } };
    }

    pub fn ping(self: *CancelLive, result: Ping.Result) Next {
        expectEcho(result, &self.script, 'x') catch |err| return self.fail(err);

        // Canceling the server must cancel the live handler and close its stream.
        self.script.server.cancel();
        return .{ .go = .{ .eof = self.script.streams[0].?.recv(&self.script.byte) } };
    }

    pub fn eof(self: *CancelLive, result: io.Error!usize) Next {
        const n = result catch |err| return self.fail(err);
        if (n != 0) return self.fail(error.ExpectedEof);

        return .{ .done = self.script.finish({}) };
    }
};

fn serve(comptime Client: type, slot_count: comptime_int, sessions_expected: usize, expected: App.Stats) !void {
    for (support.modes) |mode| {
        var loop = try support.initLoop(16, mode);
        defer loop.deinit();

        var sessions: usize = 0;
        var slots: [slot_count]App.Slot = undefined;
        var server: co.Frame(App) = .init(try .init(&loop, try .parseLiteral("127.0.0.1:0"), &sessions, &slots, .{}));
        defer server.coroutine.deinit();

        var client: co.Frame(co.Machine(Client)) = .init(.init(.{ .script = .{
            .loop = &loop,
            .server = &server,
            .until = try io.deadlineIn(io.seconds(5)),
        } }));

        loop.spawn(&client);
        try loop.block(&server);
        try client.getResult();

        try testing.expectEqual(sessions_expected, sessions);
        try testing.expectEqual(expected, server.coroutine.stats);
        try testing.expectEqual(0, server.coroutine.connections.active);
        try support.drained(&loop);
    }
}

test "server queues clients behind full slots, counts handler errors and closes streams" {
    try serve(Backpressure, 1, 2, .{ .accepted = 2, .failed = 1 });
}

test "server cancellation cancels live connections and drains" {
    try serve(CancelLive, 2, 1, .{ .accepted = 1 });
}

test "server rejects empty slots and cancellation before polling opens no sessions" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(4, mode);
        defer loop.deinit();
        var sessions: usize = 0;
        const address: net.Address = try .parseLiteral("127.0.0.1:0");
        try testing.expectError(error.InvalidArgument, App.init(&loop, address, &sessions, &.{}, .{}));

        var slots: [1]App.Slot = undefined;
        var server: co.Frame(App) = .init(try .init(&loop, address, &sessions, &slots, .{}));
        defer server.coroutine.deinit();
        server.cancel();
        try loop.block(&server);
        try testing.expectEqual(0, sessions);
        try testing.expectEqual(0, server.coroutine.connections.active);
        try support.drained(&loop);
    }
}

test "server backoff errors propagate after draining rather than reporting success" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(4, mode);
        defer loop.deinit();
        var sessions: usize = 0;
        var slots: [1]App.Slot = undefined;
        var server: co.Frame(App) = .init(try .init(&loop, try .parseLiteral("127.0.0.1:0"), &sessions, &slots, .{}));
        defer server.coroutine.deinit();

        // Inject a timer failure into the acceptor's result handler.
        const acceptor = &server.coroutine.acceptor;
        acceptor.ctx.server = &server.coroutine;
        acceptor.state = acceptor.ctx.backoff(error.SystemResources).go;
        acceptor.tag = .drain;
        acceptor.started = true;
        try testing.expectError(error.SystemResources, loop.block(&server));
        try testing.expectEqual(0, sessions);
        try testing.expectEqual(0, server.coroutine.connections.active);
        try support.drained(&loop);
    }
}
