const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const net = velo.net;

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
        const n = result catch |err| return self.done(err);
        if (n == 0) return self.done({});
        return .{ .go = .{ .send = self.stream.sendAll(self.buffer[0..n]) } };
    }

    pub fn send(self: *Echo, result: co.ResultOf(net.SendAll)) Next {
        result catch |err| return self.done(err);
        return self.start();
    }

    fn done(self: *Echo, result: Result) Next {
        self.stream.close();
        return .{ .done = result };
    }
};

const Acceptor = struct {
    listener: *net.Listener,
    frames: []co.Frame(co.Machine(Echo)),
    accepted: usize = 0,

    pub const Result = anyerror!void;
    pub const State = union(enum) { accept: io.Accept };
    const Next = co.Next(@This());

    pub fn start(self: *Acceptor) Next {
        return .{ .go = .{ .accept = self.listener.accept(.{}) } };
    }

    pub fn accept(self: *Acceptor, result: io.Error!io.Socket) Next {
        const fd = result catch |err| return self.fail(err);
        const stream = net.Stream.init(self.listener.loop, fd);
        const enabled: c_int = 1;
        if (std.os.linux.errno(std.os.linux.setsockopt(fd, std.os.linux.IPPROTO.TCP, std.os.linux.TCP.NODELAY, std.mem.asBytes(&enabled), @sizeOf(c_int))) != .SUCCESS) {
            stream.close();
            return self.fail(error.SetNoDelay);
        }
        const frame = &self.frames[self.accepted];
        frame.* = .init(.init(.{ .stream = stream }));
        self.listener.loop.spawn(frame);
        self.accepted += 1;
        return if (self.accepted == self.frames.len) .{ .done = {} } else self.start();
    }

    fn fail(self: *Acceptor, err: anyerror) Next {
        self.listener.loop.shutdown();
        return .{ .done = err };
    }
};

pub fn main(init: std.process.Init) !void {
    if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len != 3) return error.Usage;
    const count = try std.fmt.parseInt(usize, args[1], 10);
    if (count == 0 or count > 10000) return error.InvalidConnections;
    const backend = std.meta.stringToEnum(io.Loop.Backend, args[2]) orelse return error.InvalidBackend;
    var loop = try io.Loop.initWithBackend(.{ .entries = 256 }, backend);
    defer loop.deinit();
    var listener = try net.Listener.init(&loop, try .parseLiteral("127.0.0.1:0"), .{ .backlog = 1024 });
    defer listener.deinit();
    const frames = try init.gpa.alloc(co.Frame(co.Machine(Echo)), count);
    defer init.gpa.free(frames);

    var output_buffer: [256]u8 = undefined;
    var output = std.Io.File.stdout().writerStreaming(init.io, &output_buffer);
    try std.json.Stringify.value(.{
        .port = listener.address.ip4.port,
        .backend = if (loop.epoll != null) "epoll" else "uring",
        .version = builtin.zig_version_string,
    }, .{}, &output.interface);
    try output.interface.writeByte('\n');
    try output.interface.flush();

    var acceptor: co.Frame(co.Machine(Acceptor)) = .init(.init(.{ .listener = &listener, .frames = frames }));
    try loop.block(&acceptor);
    for (frames) |*frame| try frame.getResult();
}
