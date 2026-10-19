const std = @import("std");
const builtin = @import("builtin");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const testing = std.testing;

const Mode = enum { auto, epoll };
pub const modes = if (builtin.os.tag == .linux) [_]Mode{ .auto, .epoll } else [_]Mode{.auto};

pub fn initLoop(entries: u16, mode: Mode) !io.Loop {
    if (builtin.os.tag == .linux) return io.Loop.initWithBackend(.{ .entries = entries }, switch (mode) {
        .auto => .auto,
        .epoll => .epoll,
    });

    return io.Loop.init(.{ .entries = entries });
}

pub fn run(loop: *io.Loop) void {
    // A run error retains pinned frames; unwinding would destroy live storage.
    loop.run() catch |err| std.debug.panic("loop.run: {s}", .{@errorName(err)});
}

pub fn drained(loop: *io.Loop) !void {
    try testing.expectEqual(0, loop.active);
    try testing.expectEqual(null, loop.pending.first);
    try testing.expectEqual(null, loop.executor.ready.first);

    if (builtin.os.tag == .linux) {
        try testing.expectEqual(0, loop.outstanding_cqes);
        try testing.expectEqual(null, loop.cancellations.first);

        if (loop.epoll) |*state| try testing.expectEqual(0, state.live);
    } else {
        try testing.expectEqual(null, loop.live.first);
    }
}

pub fn deadline(ms: i64) !io.Timespec {
    const time = try io.now();
    const ns = time.nsec + @mod(ms, 1000) * 1_000_000;

    return .{
        .sec = time.sec + @divTrunc(ms, 1000) + @divTrunc(ns, 1_000_000_000),
        .nsec = @mod(ns, 1_000_000_000),
    };
}

pub fn close(fd: io.Socket) void {
    io.closeSocket(fd) catch |err| std.debug.panic("closeSocket: {s}", .{@errorName(err)});
}

pub fn pair(loop: *io.Loop, comptime family: io.AddressFamily) ![2]io.Socket {
    const listener = try io.tcpSocket(family);
    defer close(listener);

    var address: if (family == .ipv4) io.SockAddr.in else io.SockAddr.in6 = if (family == .ipv4)
        .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) }
    else
        .{
            .port = 0,
            .flowinfo = 0,
            .addr = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
            .scope_id = 0,
        };
    var len: io.SockLen = @sizeOf(@TypeOf(address));

    try io.bind(listener, @ptrCast(&address), len);
    try io.listen(listener, 1);
    try io.localAddress(listener, @ptrCast(&address), &len);

    try testing.expect(address.port != 0);

    const client = try io.tcpSocket(family);
    errdefer close(client);
    const until = try deadline(3_000);
    var accept: co.Frame(io.Accept) = .{
        .coroutine = io.Accept.init(loop, .{
            .fd = listener,
            .deadline = until,
        }),
    };
    var connect: co.Frame(io.Connect) = .{
        .coroutine = io.Connect.init(loop, .{
            .fd = client,
            .address = @ptrCast(&address),
            .address_len = len,
            .deadline = until,
        }),
    };

    loop.executor.spawn(&accept.task);
    loop.executor.spawn(&connect.task);
    run(loop);
    defer accept.coroutine.deinit();
    defer connect.coroutine.deinit();

    const server = try accept.result.?;
    errdefer close(server);
    try connect.result.?;

    accept.coroutine.cancel();
    accept.coroutine.cancel();

    try testing.expectEqual(server, try accept.coroutine.poll(&accept.task).ready);
    try testing.expect(!accept.coroutine.operation.cancel_requested);
    try testing.expect(server != client and server != listener);
    try testing.expectEqual(0, accept.task.outstanding_io);
    try testing.expectEqual(0, connect.task.outstanding_io);
    try drained(loop);

    return .{ client, server };
}

pub const Echo = struct {
    loop: *io.Loop,
    fd: io.Socket,
    buffer: []u8,
    sending: bool,
    second: bool = false,
    offset: usize = 0,
    pending: bool = false,
    send: io.Send = undefined,
    recv: io.Recv = undefined,
    until: io.Timespec,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
        while (true) {
            if (self.offset == self.buffer.len) {
                if (self.second) return .{ .ready = {} };
                self.second = true;
                self.sending = !self.sending;
                self.offset = 0;
            }

            if (!self.pending) {
                if (self.sending) {
                    self.send = io.Send.init(self.loop, .{
                        .fd = self.fd,
                        .buffer = self.buffer[self.offset..@min(
                            self.offset + 127,
                            self.buffer.len,
                        )],
                        .deadline = self.until,
                    });
                } else {
                    self.recv = io.Recv.init(self.loop, .{
                        .fd = self.fd,
                        .buffer = self.buffer[self.offset..@min(
                            self.offset + 31,
                            self.buffer.len,
                        )],
                        .deadline = self.until,
                    });
                }

                self.pending = true;
            }

            const n = switch (if (self.sending) self.send.poll(task) else self.recv.poll(task)) {
                .pending => return .pending,
                .ready => |value| value catch |err| return .{ .ready = err },
            };
            self.pending = false;

            if (n == 0) return .{ .ready = error.UnexpectedZeroTransfer };

            self.offset += n;
        }
    }
};
