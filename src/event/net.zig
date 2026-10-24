//! TCP conveniences over the socket helpers and operations: parsed addresses, listeners,
//! streams with a shared deadline, and connects that own their socket until success.
//! Initialize the loop first; close sockets after their operations and before loop deinit.

const std = @import("std");
const io = @import("root.zig");
const co = @import("../coroutines/root.zig");

/// IPv4 or IPv6 address and port: `try .parseLiteral("127.0.0.1:8080")`, `"[::1]:8080"`;
/// format with `{f}`.
pub const Address = std.Io.net.IpAddress;

/// Native socket address for an `Address`; pin it while an operation borrows it.
pub const NativeAddress = struct {
    storage: extern union { any: io.SockAddr, in: io.SockAddr.in, in6: io.SockAddr.in6 },
    family: io.AddressFamily,

    pub fn init(value: Address) NativeAddress {
        return switch (value) {
            .ip4 => |ip| .{ .family = .ipv4, .storage = .{ .in = .{
                .port = std.mem.nativeToBig(u16, ip.port),
                .addr = @bitCast(ip.bytes),
            } } },
            .ip6 => |ip| .{ .family = .ipv6, .storage = .{ .in6 = .{
                .port = std.mem.nativeToBig(u16, ip.port),
                .flowinfo = ip.flow,
                .addr = ip.bytes,
                .scope_id = ip.interface.index,
            } } },
        };
    }

    pub fn address(self: *const NativeAddress) Address {
        return switch (self.family) {
            .ipv4 => .{ .ip4 = .{
                .bytes = @bitCast(self.storage.in.addr),
                .port = std.mem.bigToNative(u16, self.storage.in.port),
            } },
            .ipv6 => .{ .ip6 = .{
                .bytes = self.storage.in6.addr,
                .port = std.mem.bigToNative(u16, self.storage.in6.port),
                .flow = self.storage.in6.flowinfo,
                .interface = .{ .index = self.storage.in6.scope_id },
            } },
        };
    }

    pub fn ptr(self: *NativeAddress) *io.SockAddr {
        return &self.storage.any;
    }

    pub fn len(self: *const NativeAddress) io.SockLen {
        return switch (self.family) {
            .ipv4 => @sizeOf(io.SockAddr.in),
            .ipv6 => @sizeOf(io.SockAddr.in6),
        };
    }
};

fn familyOf(address: Address) io.AddressFamily {
    return switch (address) {
        .ip4 => .ipv4,
        .ip6 => .ipv6,
    };
}

// Close errors other than a bad descriptor still release it.
fn closeSocket(fd: io.Socket) void {
    io.closeSocket(fd) catch |err| switch (err) {
        error.BadFileDescriptor => unreachable, // double close or never opened
        else => {},
    };
}

pub const Listener = struct {
    loop: *io.Loop,
    fd: io.Socket,
    /// Bound local address, including the kernel-chosen port for port 0.
    address: Address,

    pub const Options = struct { backlog: u31 = 128 };

    pub fn init(loop: *io.Loop, address: Address, options: Options) io.Error!Listener {
        const fd = try io.tcpSocket(familyOf(address));
        errdefer closeSocket(fd);

        var native: NativeAddress = .init(address);
        var length = native.len();

        try io.bind(fd, native.ptr(), length);
        try io.listen(fd, options.backlog);
        try io.localAddress(fd, native.ptr(), &length);

        return .{ .loop = loop, .fd = fd, .address = native.address() };
    }

    /// Requires every accept on this listener to be consumed.
    pub fn deinit(self: *Listener) void {
        closeSocket(self.fd);
    }

    /// The accepted socket belongs to the caller even if cancellation arrives late.
    pub fn accept(self: *const Listener, options: struct { deadline: ?io.Timespec = null }) io.Accept {
        return .init(self.loop, .{ .fd = self.fd, .deadline = options.deadline });
    }
};

/// A connected socket. Operations borrow their buffers until ready is consumed.
pub const Stream = struct {
    loop: *io.Loop,
    fd: io.Socket,
    /// Absolute deadline for operations created afterwards, e.g. `io.deadlineIn(...)`.
    deadline: ?io.Timespec = null,

    pub fn init(loop: *io.Loop, fd: io.Socket) Stream {
        return .{ .loop = loop, .fd = fd };
    }

    /// Zero means EOF; may transfer fewer bytes than the buffer holds.
    pub fn recv(self: Stream, buffer: []u8) io.Recv {
        return .init(self.loop, .{ .fd = self.fd, .buffer = buffer, .deadline = self.deadline });
    }

    /// May transfer fewer bytes than the buffer holds.
    pub fn send(self: Stream, buffer: []const u8) io.Send {
        return .init(self.loop, .{ .fd = self.fd, .buffer = buffer, .deadline = self.deadline });
    }

    /// Fills the buffer or fails with EndOfStream.
    pub fn readExact(self: Stream, buffer: []u8) io.ReadExact {
        return .init(self.loop, .{ .fd = self.fd, .buffer = buffer, .deadline = self.deadline });
    }

    pub fn sendAll(self: Stream, buffer: []const u8) io.SendAll {
        return .init(self.loop, .{ .fd = self.fd, .buffer = buffer, .deadline = self.deadline });
    }

    pub fn shutdown(self: Stream, how: io.ShutdownHow) io.Error!void {
        return io.shutdownSocket(self.fd, how);
    }

    /// Requires every operation on this stream to be consumed.
    pub fn close(self: Stream) void {
        closeSocket(self.fd);
    }
};

/// Connects a new TCP socket. The returned `Stream` belongs to the caller; failure, timeout
/// and cancellation close the socket, since readiness backends cannot undo a started connect.
pub const Dial = struct {
    loop: *io.Loop,
    native: NativeAddress,
    deadline: ?io.Timespec,
    fd: ?io.Socket = null,
    connect: io.Connect = undefined,
    canceled: bool = false,

    pub fn init(loop: *io.Loop, address: Address, options: struct { deadline: ?io.Timespec = null }) Dial {
        return .{ .loop = loop, .native = .init(address), .deadline = options.deadline };
    }

    pub fn poll(self: *Dial, task: *co.Task) co.Poll(io.Error!Stream) {
        if (self.fd == null) {
            if (self.canceled or task.cancel_requested or self.loop.control.isShuttingDown())
                return .{ .ready = error.OperationCanceled };

            self.fd = io.tcpSocket(self.native.family) catch |err| return .{ .ready = err };
            self.connect = .init(self.loop, .{
                .fd = self.fd.?,
                .address = self.native.ptr(),
                .address_len = self.native.len(),
                .deadline = self.deadline,
            });
        }

        const result = self.connect.poll(task).value() orelse return .pending;
        result catch |err| {
            closeSocket(self.fd.?);
            return .{ .ready = err };
        };

        return .{ .ready = Stream.init(self.loop, self.fd.?) };
    }

    pub fn cancel(self: *Dial) void {
        self.canceled = true;
        if (self.fd != null) self.connect.cancel();
    }
};
