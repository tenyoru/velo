//! Overlapped sockets stay on one loop; never skip success packets or suppress OVERLAPPED events.
//! Initialize the loop before socket helpers; close sockets before deinit.

const std = @import("std");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const Control = @import("../Control.zig");
const ThreadGuard = @import("../../ThreadGuard.zig");
const w = @import("win32.zig");
const ws = std.os.windows.ws2_32;
const assert = std.debug.assert;

pub const NativeError = u32;

pub const OperationState = struct {
    overlapped: w.OVERLAPPED = .{},
    buffer: w.WSABUF = .{ .len = 0, .buf = undefined },
    flags: u32 = 0,
    address_len: i32 = 0,
    deadline: ?i128 align(8) = null,
    expired: bool = false,
    cancel_requested: bool = false,
    accept_socket: io.Socket = w.INVALID_SOCKET,
    accept_buffer: [2 * (@sizeOf(ws.sockaddr.storage) + 16)]u8 = undefined,
};

pub fn errno(result: ?i32) ?u32 {
    const value = result orelse return null;
    return if (value == 0) null else @bitCast(value);
}

pub fn now() io.Error!io.Timespec {
    var ticks: i64 = undefined;
    var frequency: i64 = undefined;
    if (w.QueryPerformanceFrequency(&frequency) == 0 or frequency <= 0 or
        w.QueryPerformanceCounter(&ticks) == 0) return error.OsError;

    return .{
        .sec = @divFloor(ticks, frequency),
        .nsec = @intCast(@divFloor(@as(i128, @mod(ticks, frequency)) * 1_000_000_000, frequency)),
    };
}

fn ns(time: io.Timespec) i128 {
    return @as(i128, time.sec) * 1_000_000_000 + time.nsec;
}

fn mapped(code: u32) io.Error {
    return switch (code) {
        w.OPERATION_ABORTED => error.OperationCanceled,
        121, 10060, w.WAIT_TIMEOUT => error.Timeout,
        1225, 10061 => error.ConnectionRefused,
        64, 1236, 10052, 10053, 10054 => error.ConnectionResetByPeer,
        109, 10058 => error.BrokenPipe,
        234, 10040 => error.MessageTooBig,
        10035 => error.WouldBlock,
        6, 10009, 10038 => error.BadFileDescriptor,
        87, 10014, 10022, 10047 => error.InvalidArgument,
        8, 14, 10024, 10055 => error.SystemResources,
        else => error.OsError,
    };
}

pub fn tcpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const fd = w.WSASocketW(
        switch (family) {
            .ipv4 => ws.AF.INET,
            .ipv6 => ws.AF.INET6,
        },
        ws.SOCK.STREAM,
        ws.IPPROTO.TCP,
        null,
        0,
        w.WSA_FLAG_OVERLAPPED | w.WSA_FLAG_NO_HANDLE_INHERIT,
    );
    if (fd == w.INVALID_SOCKET) return mapped(@bitCast(w.WSAGetLastError()));

    return fd;
}

pub fn udpSocket(family: io.AddressFamily) io.Error!io.Socket {
    const fd = w.WSASocketW(
        switch (family) {
            .ipv4 => ws.AF.INET,
            .ipv6 => ws.AF.INET6,
        },
        ws.SOCK.DGRAM,
        ws.IPPROTO.UDP,
        null,
        0,
        w.WSA_FLAG_OVERLAPPED | w.WSA_FLAG_NO_HANDLE_INHERIT,
    );
    if (fd == w.INVALID_SOCKET) return mapped(@bitCast(w.WSAGetLastError()));

    return fd;
}

pub fn shutdownSocket(fd: io.Socket, how: io.ShutdownHow) io.Error!void {
    if (w.shutdown(fd, switch (how) {
        .read => 0,
        .write => 1,
        .both => 2,
    }) != 0) return mapped(@bitCast(w.WSAGetLastError()));
}

pub fn bind(fd: io.Socket, address: *const io.SockAddr, len: io.SockLen) io.Error!void {
    if (len > std.math.maxInt(i32)) return error.InvalidArgument;
    if (w.bind(fd, address, @intCast(len)) != 0) return mapped(@bitCast(w.WSAGetLastError()));
}

pub fn listen(fd: io.Socket, backlog: u31) io.Error!void {
    if (w.listen(fd, backlog) != 0) return mapped(@bitCast(w.WSAGetLastError()));
}

pub fn localAddress(fd: io.Socket, address: *io.SockAddr, len: *io.SockLen) io.Error!void {
    if (len.* > std.math.maxInt(i32)) return error.InvalidArgument;

    var native_len: i32 = @intCast(len.*);
    if (w.getsockname(fd, address, &native_len) != 0) return mapped(@bitCast(w.WSAGetLastError()));
    len.* = @intCast(native_len);
}

pub fn closeSocket(fd: io.Socket) io.Error!void {
    if (w.closesocket(fd) != 0) return mapped(@bitCast(w.WSAGetLastError()));
}

fn socketOf(op: *io.Operation) usize {
    return switch (op.data) {
        inline .connect, .accept, .send, .recv, .sendto, .recvfrom, .close => |v| v.fd,
        .timeout => unreachable,
    };
}

pub const Loop = struct {
    pub const Options = io.LoopOptions;

    port: w.HANDLE,
    executor: co.Executor = .{},
    control: Control = .{ .signal = signalLoop },
    pending: std.DoublyLinkedList = .{},
    active: usize = 0,
    running: bool = false,
    live: std.DoublyLinkedList = .{},

    const wake_key: usize = 1;

    fn signalLoop(control: *Control) void {
        const loop: *Loop = @fieldParentPtr("control", control);
        if (w.PostQueuedCompletionStatus(loop.port, 0, wake_key, null) == 0)
            @panic("IOCP wake failed");
    }

    pub fn post(loop: *Loop, command: *Control.Post) void {
        loop.control.post(command);
    }

    pub fn retain(loop: *Loop) void {
        loop.control.retain();
    }

    pub fn release(loop: *Loop) void {
        loop.control.release();
    }

    pub fn stop(loop: *Loop) void {
        loop.control.stop();
    }

    pub fn shutdown(loop: *Loop) void {
        loop.control.shutdown();
    }

    pub fn init(options: Options) !Loop {
        if (options.entries == 0) return error.InvalidArgument;

        var data: w.WSAData = undefined;
        if (w.WSAStartup(0x0202, &data) != 0) return error.WinsockStartupFailed;
        errdefer _ = w.WSACleanup();

        const port = w.CreateIoCompletionPort(
            @ptrFromInt(std.math.maxInt(usize)),
            null,
            0,
            1,
        ) orelse
            return error.CompletionPortCreationFailed;

        return .{ .port = port };
    }

    pub fn deinit(loop: *Loop) void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(!loop.running and loop.active == 0);
        assert(loop.pending.first == null and loop.live.first == null and
            loop.executor.ready.first == null);
        assert(loop.executor.all_tasks.first == null);

        loop.control.deinit();

        const closed = w.CloseHandle(loop.port);
        assert(closed != 0);

        const cleanup = w.WSACleanup();
        assert(cleanup == 0);
    }

    pub fn cancel(loop: *Loop, op: *io.Operation) void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(op.loop == loop and op.state == .inflight);
        op.cancel_requested = true;
        if (op.data == .timeout) loop.finish(op, w.OPERATION_ABORTED, 0);
    }

    pub fn run(loop: *Loop) !void {
        const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

        assert(!loop.running);
        loop.running = true;
        defer loop.running = false;

        while (true) {
            if (loop.control.service(&loop.executor)) return;

            var budget: usize = 64;
            while (budget > 0 and loop.executor.runOne()) : (budget -= 1) {}

            var completions: usize = 0;
            while (completions < 64 and try loop.completion(0)) : (completions += 1) {}
            if (loop.control.service(&loop.executor)) return;

            while (loop.pending.first) |node| {
                const op: *io.Operation = @fieldParentPtr("node", node);
                const current = ns(try now());

                loop.pending.remove(node);
                loop.live.append(node);
                op.state = .inflight;
                loop.submit(op, current);
            }

            const wait = try loop.deadlines(ns(try now()));
            if (loop.executor.ready.first != null) continue;

            if (loop.live.first == null and !loop.control.hasWork()) {
                if (!loop.control.idle(&loop.executor)) continue;
                assert(loop.active == 0);
                return;
            }

            _ = try loop.completion(wait);
        }
    }

    fn finish(loop: *Loop, op: *io.Operation, code: u32, bytes: usize) void {
        loop.live.remove(&op.node);

        var result = bytes;
        if (op.data == .accept) {
            const accepted = op.native.accept_socket;
            if (code == 0) {
                assert(accepted != w.INVALID_SOCKET);
                result = accepted;
            } else if (accepted != w.INVALID_SOCKET) {
                _ = w.closesocket(accepted);
            }
            op.native.accept_socket = w.INVALID_SOCKET;
        }

        op.primary = @bitCast(code);
        if (op.linked and op.timer == null) op.timer = w.OPERATION_ABORTED;
        op.finish(if (code == 0) result else if (code == w.OPERATION_ABORTED and op.native.expired)
            error.Timeout
        else
            mapped(code));
    }

    fn submit(loop: *Loop, op: *io.Operation, current: i128) void {
        if (op.linked or op.data == .timeout) {
            if (op.time.nsec < 0 or op.time.nsec >= 1_000_000_000 or
                (op.data == .timeout and op.time.sec < 0))
            {
                loop.finish(op, 10022, 0);
                return;
            }
            op.native.deadline = ns(op.time) + (if (op.data == .timeout) current else @as(i128, 0));
        }
        if (op.data == .timeout) return;

        const fd = socketOf(op);
        // INVALID_SOCKET is also IOCP's sentinel for creating a new port.
        if (fd == w.INVALID_SOCKET) {
            loop.finish(op, 10038, 0);
            return;
        }

        if (op.data == .close) {
            const code: u32 = if (w.closesocket(fd) == 0) 0 else @bitCast(w.WSAGetLastError());
            loop.finish(op, code, 0);
            return;
        }

        // Sockets carry no loop, so their first operation associates them. Windows allows
        // one association per handle and rejects a repeat with ERROR_INVALID_PARAMETER.
        if (w.CreateIoCompletionPort(@ptrFromInt(fd), loop.port, 0, 0) == null) {
            const code = w.GetLastError();
            if (code != w.INVALID_PARAMETER) {
                loop.finish(op, code, 0);
                return;
            }
        }

        const result = switch (op.data) {
            .send => |v| blk: {
                op.native.buffer = .{
                    .buf = @constCast(v.buffer.ptr),
                    .len = @intCast(@min(v.buffer.len, std.math.maxInt(i32))),
                };

                break :blk w.WSASend(
                    fd,
                    @ptrCast(&op.native.buffer),
                    1,
                    null,
                    0,
                    &op.native.overlapped,
                    null,
                );
            },
            .recv => |v| blk: {
                op.native.buffer = .{
                    .buf = v.buffer.ptr,
                    .len = @intCast(@min(v.buffer.len, std.math.maxInt(i32))),
                };

                break :blk w.WSARecv(
                    fd,
                    @ptrCast(&op.native.buffer),
                    1,
                    null,
                    &op.native.flags,
                    &op.native.overlapped,
                    null,
                );
            },
            .sendto => |v| blk: {
                if (v.address_len > std.math.maxInt(i32)) {
                    loop.finish(op, 10022, 0);
                    return;
                }

                op.native.buffer = .{
                    .buf = @constCast(v.buffer.ptr),
                    .len = @intCast(@min(v.buffer.len, std.math.maxInt(i32))),
                };

                break :blk w.WSASendTo(
                    fd,
                    @ptrCast(&op.native.buffer),
                    1,
                    null,
                    0,
                    v.address,
                    @intCast(v.address_len),
                    &op.native.overlapped,
                    null,
                );
            },
            .recvfrom => |v| blk: {
                if (v.address_len.* > std.math.maxInt(i32)) {
                    loop.finish(op, 10022, 0);
                    return;
                }

                op.native.buffer = .{
                    .buf = v.buffer.ptr,
                    .len = @intCast(@min(v.buffer.len, std.math.maxInt(i32))),
                };
                op.native.address_len = @intCast(v.address_len.*);

                break :blk w.WSARecvFrom(
                    fd,
                    @ptrCast(&op.native.buffer),
                    1,
                    null,
                    &op.native.flags,
                    v.address,
                    &op.native.address_len,
                    &op.native.overlapped,
                    null,
                );
            },
            .connect => return loop.connect(op),
            .accept => return loop.accept(op),
            else => unreachable,
        };

        if (result != 0) {
            const code: u32 = @bitCast(w.WSAGetLastError());
            // A truncated receive still queues its STATUS_BUFFER_OVERFLOW completion.
            if (code != w.IO_PENDING and
                !((op.data == .recv or op.data == .recvfrom) and code == 10040))
                loop.finish(op, code, 0);
        }

        // Synchronous success still owns OVERLAPPED until its IOCP packet is dequeued.
    }

    fn connect(loop: *Loop, op: *io.Operation) void {
        const v = op.data.connect;
        if (v.address_len < @sizeOf(u16) or v.address_len > std.math.maxInt(i32)) {
            loop.finish(op, 10022, 0);
            return;
        }

        const length: i32 = switch (v.address.family) {
            ws.AF.INET => @sizeOf(ws.sockaddr.in),
            ws.AF.INET6 => @sizeOf(ws.sockaddr.in6),
            else => {
                loop.finish(op, 10047, 0);
                return;
            },
        };
        if (v.address_len < length) {
            loop.finish(op, 10022, 0);
            return;
        }

        var address = std.mem.zeroes(ws.sockaddr.storage);
        var local_length: i32 = @sizeOf(ws.sockaddr.storage);
        if (w.getsockname(v.fd, @ptrCast(&address), &local_length) != 0) {
            const code: u32 = @bitCast(w.WSAGetLastError());
            if (code != 10022) {
                loop.finish(op, code, 0);
                return;
            }

            address = std.mem.zeroes(ws.sockaddr.storage);
            address.family = v.address.family;
            if (w.bind(v.fd, @ptrCast(&address), length) != 0) {
                loop.finish(op, @bitCast(w.WSAGetLastError()), 0);
                return;
            }
        }

        const guid = std.os.windows.GUID{
            .Data1 = 0x25a207b9,
            .Data2 = 0xddf3,
            .Data3 = 0x4660,
            .Data4 = .{ 0x8e, 0xe9, 0x76, 0xe5, 0x8c, 0x74, 0x06, 0x3e },
        };
        var connect_ex: w.ConnectEx = undefined;
        var bytes: u32 = 0;

        if (w.WSAIoctl(
            v.fd,
            0xc8000006,
            &guid,
            @sizeOf(@TypeOf(guid)),
            @ptrCast(&connect_ex),
            @sizeOf(w.ConnectEx),
            &bytes,
            null,
            null,
        ) != 0) {
            loop.finish(op, @bitCast(w.WSAGetLastError()), 0);
            return;
        }
        if (bytes != @sizeOf(w.ConnectEx)) {
            loop.finish(op, 10022, 0);
            return;
        }

        if (connect_ex(
            v.fd,
            v.address,
            @intCast(v.address_len),
            null,
            0,
            null,
            &op.native.overlapped,
        ) == 0) {
            const code: u32 = @bitCast(w.WSAGetLastError());
            if (code != w.IO_PENDING) loop.finish(op, code, 0);
        }
    }

    fn accept(loop: *Loop, op: *io.Operation) void {
        const fd = op.data.accept.fd;
        var address: ws.sockaddr.storage = undefined;
        var length: i32 = @sizeOf(ws.sockaddr.storage);
        if (w.getsockname(fd, @ptrCast(&address), &length) != 0) {
            loop.finish(op, @bitCast(w.WSAGetLastError()), 0);
            return;
        }
        if (address.family != ws.AF.INET and address.family != ws.AF.INET6) {
            loop.finish(op, 10047, 0);
            return;
        }

        const guid = std.os.windows.GUID{
            .Data1 = 0xb5367df1,
            .Data2 = 0xcbac,
            .Data3 = 0x11cf,
            .Data4 = .{ 0x95, 0xca, 0x00, 0x80, 0x5f, 0x48, 0xa1, 0x92 },
        };
        var accept_ex: w.AcceptEx = undefined;
        var bytes: u32 = 0;

        if (w.WSAIoctl(
            fd,
            0xc8000006,
            &guid,
            @sizeOf(@TypeOf(guid)),
            @ptrCast(&accept_ex),
            @sizeOf(w.AcceptEx),
            &bytes,
            null,
            null,
        ) != 0) {
            loop.finish(op, @bitCast(w.WSAGetLastError()), 0);
            return;
        }
        if (bytes != @sizeOf(w.AcceptEx)) {
            loop.finish(op, 10022, 0);
            return;
        }

        op.native.accept_socket = w.WSASocketW(
            address.family,
            ws.SOCK.STREAM,
            ws.IPPROTO.TCP,
            null,
            0,
            w.WSA_FLAG_OVERLAPPED | w.WSA_FLAG_NO_HANDLE_INHERIT,
        );
        if (op.native.accept_socket == w.INVALID_SOCKET) {
            loop.finish(op, @bitCast(w.WSAGetLastError()), 0);
            return;
        }

        const address_size = @sizeOf(ws.sockaddr.storage) + 16;
        if (accept_ex(
            fd,
            op.native.accept_socket,
            &op.native.accept_buffer,
            0,
            address_size,
            address_size,
            &bytes,
            &op.native.overlapped,
        ) == 0) {
            const code: u32 = @bitCast(w.WSAGetLastError());
            if (code != w.IO_PENDING) loop.finish(op, code, 0);
        }
    }

    fn deadlines(loop: *Loop, current: i128) !u32 {
        var wait: u32 = std.math.maxInt(u32);

        // O(n) deadline scans; use a timer heap at scale.
        var cursor = loop.live.first;

        while (cursor) |node| {
            cursor = node.next;
            const op: *io.Operation = @fieldParentPtr("node", node);
            if (op.native.cancel_requested) continue;
            if (!op.cancel_requested) {
                const deadline = op.native.deadline orelse continue;
                const remaining = deadline - current;
                if (remaining > 0) {
                    wait = @min(wait, @as(u32, @intCast(@min(
                        @divTrunc(remaining + 999_999, 1_000_000),
                        std.math.maxInt(u32) - 1,
                    ))));
                    continue;
                }
            }

            if (op.data == .timeout) {
                loop.finish(op, if (op.cancel_requested) w.OPERATION_ABORTED else 0, 0);
                continue;
            }

            op.native.expired = !op.cancel_requested;
            if (w.CancelIoEx(@ptrFromInt(socketOf(op)), &op.native.overlapped) == 0) {
                const code = w.GetLastError();
                if (code != w.NOT_FOUND) {
                    op.timer = @bitCast(code);
                    return error.IoCancellationFailed;
                }
            }
            if (op.linked)
                op.timer = if (op.native.expired) w.WAIT_TIMEOUT else w.OPERATION_ABORTED;
            op.native.cancel_requested = true;
        }

        return wait;
    }

    fn completion(loop: *Loop, wait: u32) !bool {
        var bytes: u32 = undefined;
        var key: usize = undefined;
        var overlapped: ?*w.OVERLAPPED = null;

        const success = w.GetQueuedCompletionStatus(
            loop.port,
            &bytes,
            &key,
            &overlapped,
            wait,
        ) != 0;
        var code: u32 = if (success) 0 else w.GetLastError();
        const pointer = overlapped orelse {
            if (success and key == wake_key) return true;
            if (!success and code == w.WAIT_TIMEOUT) return false;
            return error.CompletionPortWaitFailed;
        };

        const native: *OperationState = @fieldParentPtr("overlapped", pointer);
        const op: *io.Operation = @fieldParentPtr("native", native);
        assert(op.loop == loop and op.state == .inflight);

        if (code == 0 and op.data == .connect) {
            if (w.setsockopt(
                socketOf(op),
                ws.SOL.SOCKET,
                ws.SO.UPDATE_CONNECT_CONTEXT,
                null,
                0,
            ) != 0)
                code = @bitCast(w.WSAGetLastError());
        }
        if (code == 0 and op.data == .accept) {
            const fd = socketOf(op);
            if (w.setsockopt(
                op.native.accept_socket,
                ws.SOL.SOCKET,
                ws.SO.UPDATE_ACCEPT_CONTEXT,
                @ptrCast(&fd),
                @sizeOf(io.Socket),
            ) != 0)
                code = @bitCast(w.WSAGetLastError());
        }
        if (op.data == .recvfrom and (code == 0 or code == 10040 or code == 234)) {
            if (op.native.address_len < 0) {
                code = 10022;
            } else {
                op.data.recvfrom.address_len.* = @intCast(op.native.address_len);
            }
        }

        loop.finish(op, code, bytes);
        return true;
    }
};

test "iocp wake packets dispatch cross-thread posts only on the loop thread" {
    const Probe = struct {
        command: Control.Post = .{ .callback = complete },
        loop: *Loop,
        thread_id: std.Thread.Id,
        callbacks: usize = 0,

        fn complete(command: *Control.Post) void {
            const probe: *@This() = @fieldParentPtr("command", command);
            assert(std.Thread.getCurrentId() == probe.thread_id);
            probe.callbacks += 1;
        }

        fn produce(probe: *@This()) void {
            probe.loop.post(&probe.command);
            probe.loop.release();
        }
    };

    var loop = try Loop.init(.{ .entries = 4 });
    defer loop.deinit();

    var probe: Probe = .{ .loop = &loop, .thread_id = std.Thread.getCurrentId() };
    loop.post(&probe.command);
    try std.testing.expect(try loop.completion(0));
    try std.testing.expectEqual(0, probe.callbacks);
    try std.testing.expectEqual(0, loop.active);
    try std.testing.expect(!loop.control.service(&loop.executor));
    try std.testing.expectEqual(1, probe.callbacks);

    loop.retain();
    const thread = try std.Thread.spawn(.{}, Probe.produce, .{&probe});
    defer thread.join();
    try loop.run();
    try std.testing.expectEqual(2, probe.callbacks);
    try std.testing.expectEqual(0, loop.active);

    loop.stop();
    try loop.run();
    try loop.run();

    loop.shutdown();
    try loop.run();
    try std.testing.expect(loop.control.isShuttingDown());
}

test "iocp queued datagrams preserve zero length and drain truncation packets" {
    const support = @import("../tests/support.zig");

    inline for (.{ io.AddressFamily.ipv4, io.AddressFamily.ipv6 }) |family| {
        var loop = try Loop.init(.{ .entries = 4 });
        defer loop.deinit();

        const sender = try udpSocket(family);
        defer support.close(sender);
        const receiver = try udpSocket(family);
        defer support.close(receiver);

        var address: if (family == .ipv4) io.SockAddr.in else io.SockAddr.in6 = if (family == .ipv4)
            .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) }
        else
            .{
                .port = 0,
                .flowinfo = 0,
                .addr = .{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 },
                .scope_id = 0,
            };
        var address_len: io.SockLen = @sizeOf(@TypeOf(address));
        try bind(receiver, @ptrCast(&address), address_len);
        try localAddress(receiver, @ptrCast(&address), &address_len);

        const until = try support.deadline(3_000);

        for ([_][]const u8{ "oversized", "" }) |payload| {
            var send: co.Frame(io.SendTo) = .{
                .coroutine = io.SendTo.init(&loop, .{
                    .fd = sender,
                    .buffer = payload,
                    .address = @ptrCast(&address),
                    .address_len = address_len,
                    .deadline = until,
                }),
            };

            loop.executor.spawn(&send.task);
            support.run(&loop);
            try std.testing.expectEqual(payload.len, try send.result.?);
            send.coroutine.deinit();

            var buffer: [1]u8 = undefined;
            var source: io.SockAddr.storage = undefined;
            var source_len: io.SockLen = @sizeOf(@TypeOf(source));
            var recv: co.Frame(io.RecvFrom) = .{
                .coroutine = io.RecvFrom.init(&loop, .{
                    .fd = receiver,
                    .buffer = &buffer,
                    .address = @ptrCast(&source),
                    .address_len = &source_len,
                    .deadline = until,
                }),
            };

            loop.executor.spawn(&recv.task);
            support.run(&loop);

            if (payload.len != 0) {
                try std.testing.expectError(error.MessageTooBig, recv.result.?);
            } else {
                try std.testing.expectEqual(0, try recv.result.?);
            }
            try std.testing.expectEqual(address_len, source_len);
            try std.testing.expectEqual(address.family, source.family);

            recv.coroutine.deinit();
            try support.drained(&loop);
        }
    }
}

test "iocp write half-close preserves the reverse stream" {
    const support = @import("../tests/support.zig");

    var loop = try Loop.init(.{ .entries = 4 });
    defer loop.deinit();

    const sockets = try support.pair(&loop, .ipv4);
    defer for (sockets) |fd| support.close(fd);
    try shutdownSocket(sockets[0], .write);

    var buffer: [1]u8 = undefined;
    var recv: co.Frame(io.Recv) = .{
        .coroutine = io.Recv.init(&loop, .{
            .fd = sockets[1],
            .buffer = &buffer,
            .deadline = try support.deadline(3_000),
        }),
    };

    loop.executor.spawn(&recv.task);
    support.run(&loop);
    try std.testing.expectEqual(0, try recv.result.?);
    recv.coroutine.deinit();

    recv = .{
        .coroutine = io.Recv.init(&loop, .{
            .fd = sockets[0],
            .buffer = &buffer,
            .deadline = try support.deadline(3_000),
        }),
    };
    var send: co.Frame(io.Send) = .{
        .coroutine = io.Send.init(&loop, .{ .fd = sockets[1], .buffer = "x" }),
    };

    loop.executor.spawn(&recv.task);
    loop.executor.spawn(&send.task);
    support.run(&loop);

    try std.testing.expectEqual(1, try send.result.?);
    try std.testing.expectEqual(1, try recv.result.?);
    try std.testing.expectEqualStrings("x", &buffer);

    send.coroutine.deinit();
    recv.coroutine.deinit();

    try shutdownSocket(sockets[0], .read);
    try shutdownSocket(sockets[1], .both);
    try support.drained(&loop);
}
