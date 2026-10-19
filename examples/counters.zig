const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.io;

const Counter = struct {
    id: u8,
    count: u8 = 0,

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(u8) {
        self.count += 1;

        if (!@import("builtin").is_test)
            std.debug.print("counter {d}: {d}\n", .{ self.id, self.count });

        if (self.count == 5) return .{ .ready = self.count };

        task.wake();
        return .pending;
    }
};

pub fn main() !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    var counters = [_]co.Frame(Counter){
        .{
            .coroutine = .{ .id = 1 },
        },
        .{
            .coroutine = .{ .id = 2 },
        },
    };

    for (&counters) |*counter| loop.executor.spawn(&counter.task);
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    for (&counters) |*counter| {
        if (counter.getResult() != 5) return error.IncorrectCount;
    }

    if (!@import("builtin").is_test) std.debug.print("PASS: both counters reached 5\n", .{});
}

test "two independent counters each reach five" {
    try main();
}
