const std = @import("std");
const builtin = @import("builtin");

const ThreadGuard = @This();
pub const enabled = builtin.is_test and std.debug.runtime_safety and !builtin.single_threaded;

const Owner = if (enabled) std.atomic.Value(std.Thread.Id) else void;

owner: Owner = if (enabled) Owner.init(0),

comptime {
    if (!enabled and @sizeOf(ThreadGuard) != 0)
        @compileError("disabled ownership guards must occupy zero bytes");
}

pub inline fn enter(guard: *ThreadGuard) bool {
    return guard.tryEnter() orelse @panic("velo: concurrent or wrong-thread executor access");
}

// Relaxed atomics are diagnostic only and must not synchronize caller data.
inline fn tryEnter(guard: *ThreadGuard) ?bool {
    if (enabled) {
        const current = std.Thread.getCurrentId();
        if (guard.owner.load(.monotonic) == current) return false;
        if (guard.owner.cmpxchgStrong(0, current, .monotonic, .monotonic) != null) return null;

        return true;
    }

    return false;
}

pub inline fn leave(guard: *ThreadGuard, entered: bool) void {
    if (enabled) {
        if (!entered) return;

        std.debug.assert(guard.owner.load(.monotonic) == std.Thread.getCurrentId());
        guard.owner.store(0, .monotonic);
    }
}

test "nested entry preserves ownership and outer leave permits reentry" {
    var guard: ThreadGuard = .{};

    const entered = guard.enter();
    try std.testing.expectEqual(enabled, entered);
    const nested = guard.enter();
    try std.testing.expect(!nested);
    guard.leave(nested);
    try std.testing.expect(!guard.enter());
    guard.leave(entered);

    const reentered = guard.enter();
    try std.testing.expectEqual(enabled, reentered);
    guard.leave(reentered);

    if (!enabled) try std.testing.expectEqual(0, @sizeOf(ThreadGuard));
}

test "safe test builds reject a different thread while ownership is held" {
    if (builtin.is_test and std.debug.runtime_safety and !builtin.single_threaded) {
        const Probe = struct {
            fn run(guard: *ThreadGuard, result: *?bool) void {
                result.* = guard.tryEnter();
                if (result.*) |entered| guard.leave(entered);
            }
        };

        var guard: ThreadGuard = .{};
        const entered = guard.enter();
        defer guard.leave(entered);

        var result: ?bool = undefined;
        const thread = try std.Thread.spawn(.{}, Probe.run, .{ &guard, &result });
        thread.join();

        try std.testing.expectEqual(null, result);
    }
}
