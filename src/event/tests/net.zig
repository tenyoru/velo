const std = @import("std");
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

            var server: io.Stream = .init(&loop, try connected.accept);
            defer server.close();
            var client = try connected.dial;
            defer client.close();

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

test "net dial closes its socket on refusal and opens none when canceled first" {
    for (support.modes) |mode| {
        var loop = try support.initLoop(4, mode);
        defer loop.deinit();

        // A just-closed listener's port refuses connections.
        var listener: io.Listener = try .init(&loop, try .parseLiteral("127.0.0.1:0"), .{});
        const address = listener.address;
        listener.deinit();

        var refused: co.Frame(io.Dial) = .init(.init(&loop, address, .{
            .deadline = try io.deadlineIn(io.seconds(5)),
        }));

        try testing.expectError(error.ConnectionRefused, loop.block(&refused));
        try testing.expect(refused.coroutine.fd != null);

        var canceled: co.Frame(io.Dial) = .init(.init(&loop, address, .{}));

        canceled.cancel();
        try testing.expectError(error.OperationCanceled, loop.block(&canceled));
        try testing.expectEqual(null, canceled.coroutine.fd);
        try support.drained(&loop);
    }
}
