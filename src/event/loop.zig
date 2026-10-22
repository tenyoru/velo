//! Backend-independent loop API. Each backend aliases `Shared(Loop, hooks)` with private
//! hooks taking `*Loop` (`*const Loop` for `inflight`):
//! - `turn() !bool`: advance I/O; true to repeat before the idle/park checks.
//! - `inflight() bool`: native work can still complete.
//! - `park() !void`: block for a completion, deadline, or wake; may return early.
//! - `close() void`: release native resources after the shared drain checks.

const std = @import("std");
const assert = std.debug.assert;
const Control = @import("Control.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

pub fn Shared(comptime Loop: type, comptime hooks: anytype) type {
    comptime {
        for ([_][]const u8{ "executor", "control", "pending", "active", "running" }) |name| {
            if (!@hasField(Loop, name))
                @compileError(@typeName(Loop) ++ " lacks field `" ++ name ++ "`");
        }
        for ([_][]const u8{ "turn", "inflight", "park", "close" }) |name| {
            if (!@hasField(@TypeOf(hooks), name))
                @compileError(@typeName(Loop) ++ " lacks hook `" ++ name ++ "`");
        }
    }

    return struct {
        pub fn run(loop: *Loop) !void {
            // Keep guard address calculations out of normal builds.
            const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
            defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

            assert(!loop.running);
            loop.running = true;
            defer loop.running = false;

            while (true) {
                if (loop.control.service(&loop.executor)) return;

                var budget: usize = 64;
                while (budget > 0 and loop.executor.runOne()) : (budget -= 1) {}

                if (try hooks.turn(loop)) continue;
                // A queued send may be what completes an already-submitted receive.
                if (loop.executor.ready.first != null or loop.pending.first != null) continue;

                if (!hooks.inflight(loop) and !loop.control.hasWork()) {
                    if (!loop.control.idle(&loop.executor)) continue;
                    assert(loop.active == 0);
                    return;
                }

                try hooks.park(loop);
            }
        }

        pub fn deinit(loop: *Loop) void {
            const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
            defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

            assert(!loop.running);
            loop.control.deinit();
            assert(loop.active == 0 and loop.pending.first == null);
            assert(loop.executor.ready.first == null and loop.executor.all_tasks.first == null);

            hooks.close(loop);
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
    };
}
