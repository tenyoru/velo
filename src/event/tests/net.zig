const std = @import("std");
const builtin = @import("builtin");
const io = @import("../root.zig");
const co = @import("../../coroutines/root.zig");
const support = @import("support.zig");
const testing = std.testing;

test "net native addresses round-trip and format IPv4 and IPv6 literals" {
    for ([_][]const u8{ "127.0.0.1:8080", "[::1]:9", "[fe80::1]:443" }) |text| {
        const address: io.Address = try .parseLiteral(text);
        const native: io.NativeAddress = .init(address);
        var buffer: [64]u8 = undefined;

        try testing.expect(native.address().eql(&address));
        try testing.expectEqualStrings(text, try std.fmt.bufPrint(&buffer, "{f}", .{native.address()}));
    }
}

test "net deadlines and durations normalize nanoseconds" {
    try testing.expectEqual(io.Timespec{ .sec = 1, .nsec = 500_000_000 }, io.milliseconds(1_500));
    try testing.expectEqual(io.Timespec{ .sec = 7, .nsec = 0 }, io.seconds(7));

    const start = try io.now();
    const deadline = try io.deadlineIn(.{ .sec = 0, .nsec = 999_999_999 });

    try testing.expect(deadline.nsec >= 0 and deadline.nsec < std.time.ns_per_s);
    try testing.expect(deadline.sec >= start.sec);
}

test "net listener dial and stream exchange data, half-close and drain" {
    for (support.modes) |mode| {
        inline for (.{ "127.0.0.1:0", "[::1]:0" }) |text| {
            var loop = try support.initLoop(8, mode);
            defer loop.deinit();

            var listener: io.Listener = try .init(&loop, try .parseLiteral(text), .{ .backlog = 4 });
            defer listener.deinit();
            try testing.expect(listener.address.getPort() != 0);

            const until = try io.deadlineIn(io.seconds(3));
            var connect: co.Frame(co.Join(.{ .accept = io.Accept, .dial = io.Dial })) = .init(.init(.{
                .accept = listener.accept(.{ .deadline = until }),
                .dial = .init(&loop, listener.address, .{ .deadline = until }),
            }));
            const connected = loop.block(&connect);
            // Both results own resources independently, even if the other child failed.
            defer if (connected.accept) |fd| support.close(fd) else |_| {};
            defer if (connected.dial) |stream| stream.close() else |_| {};

            var server: io.Stream = .init(&loop, try connected.accept);
            var client = try connected.dial;

            server.deadline = until;
            client.deadline = until;

            var reply: [5]u8 = undefined;
            var exchange: co.Frame(co.Join(.{ .send = io.SendAll, .read = io.ReadExact })) = .init(.init(.{
                .send = client.sendAll("hello"),
                .read = server.readExact(&reply),
            }));
            const exchanged = loop.block(&exchange);

            try exchanged.send;
            try exchanged.read;
            try testing.expectEqualStrings("hello", &reply);

            try client.shutdown(.write);
            var eof: co.Frame(io.Recv) = .init(server.recv(&reply));
            try testing.expectEqual(0, try loop.block(&eof));
            try support.drained(&loop);
        }
    }
}

test "net failed dial closes its socket and pre-canceled dial opens none" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(4, mode);
        defer loop.deinit();

        // Keep the port bound but not listening, so another process cannot claim it.
        const fd = try io.tcpSocket(.ipv4);
        defer support.close(fd);
        var native: io.NativeAddress = .init(try .parseLiteral("127.0.0.1:0"));
        var length = native.len();
        try io.bind(fd, native.ptr(), length);
        try io.localAddress(fd, native.ptr(), &length);
        const address = native.address();

        // XNU drops packets for a bound TCPS_CLOSED socket without sending a reset.
        // Linux and Windows refuse; Darwin exercises the same cleanup via a deadline.
        const darwin = builtin.os.tag == .macos;
        var failed: co.Frame(io.Dial) = .init(.init(&loop, address, .{
            .deadline = try io.deadlineIn(if (darwin) io.milliseconds(100) else io.seconds(5)),
        }));

        try testing.expectError(if (darwin) error.Timeout else error.ConnectionRefused, loop.block(&failed));
        try testing.expect(failed.coroutine.fd != null);
        try testing.expectError(error.BadFileDescriptor, io.localAddress(failed.coroutine.fd.?, native.ptr(), &length));

        var canceled: co.Frame(io.Dial) = .init(.init(&loop, address, .{}));

        canceled.cancel();
        try testing.expectError(error.OperationCanceled, loop.block(&canceled));
        try testing.expectEqual(null, canceled.coroutine.fd);
        try support.drained(&loop);
    }
}
