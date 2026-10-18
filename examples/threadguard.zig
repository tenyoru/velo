//! Deliberately panics; run separately from the passing example suite:
//! zig test --dep velo -Mroot=examples/threadguard.zig -Mvelo=src/root.zig

const std = @import("std");
const builtin = @import("builtin");
const co = @import("velo").coroutines;

const WrongThreadWake = struct {
    pub fn poll(_: *@This(), task: *co.Task) co.Poll(std.Thread.SpawnError!void) {
        const thread = std.Thread.spawn(.{}, co.Task.wake, .{task}) catch |err|
            return .{ .ready = err };

        // Keep the executor inside poll until the foreign thread attempts its wake.
        thread.join();
        return .{ .ready = {} };
    }
};

test "ThreadGuard panics on a wake from another thread" {
    if (!std.debug.runtime_safety or builtin.single_threaded) return error.SkipZigTest;

    var executor: co.Executor = .{};
    var frame: co.Frame(WrongThreadWake) = .{
        .coroutine = .{},
    };

    executor.spawn(&frame.task);
    executor.run();

    try frame.result.?;
    return error.ThreadGuardDidNotTrigger;
}
