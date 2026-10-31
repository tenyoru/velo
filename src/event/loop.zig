//! Backend-independent loop API. Each backend aliases `Shared(Loop, hooks)` with private
//! hooks taking `*Loop` (`*const Loop` for `inflight`):
//! - `turn() !bool`: advance I/O; true to repeat before the idle/park checks.
//! - `inflight() bool`: native work can still complete.
//! - `park() !void`: block for a completion, deadline, or wake; may return early.
//!   Runs inside `Control.enterPark`, so a post that skipped its wake cannot strand it.
//! - `spin() !bool` (optional): poll before parking; true if work may be ready.
//! - `close() void`: release native resources after the shared drain checks.

const std = @import("std");
const assert = std.debug.assert;
const event = @import("root.zig");
const Control = @import("Control.zig");
const Executor = @import("../coroutines/Executor.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

/// Polls per I/O turn: turns are cheap, so tasks that keep yielding must not delay I/O.
const max_polls = 64;
/// Upper bound for long polls, like Seastar's default task quota.
const quota_ns = 500 * std.time.ns_per_us;
/// A clock read costs several trivial polls, so short polls share one.
const max_stride = 16;
/// Safe builds report polls this long, like Seastar's stall detector.
const stall_ns = 25 * std.time.ns_per_ms;

fn nanoseconds(time: event.Timespec) u64 {
    return @as(u64, @intCast(time.sec)) * std.time.ns_per_s + @as(u64, @intCast(time.nsec));
}

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
                loop.executor.stats.turns += 1;
                if (loop.control.service(&loop.executor)) return;

                try runSlice(loop);

                if (try hooks.turn(loop)) continue;
                // A queued send may be what completes an already-submitted receive.
                if (loop.executor.hasReady() or loop.pending.first != null) continue;

                if (!hooks.inflight(loop) and !loop.control.hasWork()) {
                    if (!loop.control.idle(&loop.executor)) continue;
                    assert(loop.active == 0);
                    return;
                }

                if (@hasField(@TypeOf(hooks), "spin")) {
                    if (try hooks.spin(loop)) continue;
                }
                try park(loop);
            }
        }

        fn runSlice(loop: *Loop) !void {
            const executor = &loop.executor;
            const slice = &executor.slice;
            const weighted = executor.classes.first != null and executor.hasReady();
            slice.* = .{ .stride = slice.stride, .class = if (weighted) executor.pickClass() else null };
            const started = if (weighted) nanoseconds(try event.now()) else 0;
            defer slice.class = null;

            var polls: usize = 0;
            while (polls < max_polls and !slice.over and try poll(loop)) : (polls += 1) {
                slice.polls += 1;
                if (slice.polls >= slice.stride) try checkSlice(loop);
            }

            if (weighted) executor.charge(slice.class, nanoseconds(try event.now()) - started);
        }

        fn checkSlice(loop: *Loop) !void {
            const slice = &loop.executor.slice;
            const current = nanoseconds(try event.now());
            defer {
                slice.checked = current;
                slice.polls = 0;
            }

            const started = slice.started orelse {
                slice.started = current;
                return;
            };

            // Aim for a few reads per quota: long polls are caught one at a time.
            const elapsed = @max(current - slice.checked, 1);
            const stride = quota_ns / 4 * @as(u64, @max(slice.polls, 1)) / elapsed;
            slice.stride = @intCast(std.math.clamp(stride, 1, max_stride));
            slice.over = current - started >= quota_ns;
        }

        fn poll(loop: *Loop) !bool {
            if (!Executor.named) return loop.executor.runOne();

            const task = loop.executor.peek() orelse return false;
            const name = task.getName();
            const started = nanoseconds(try event.now());
            _ = loop.executor.runOne();
            const elapsed = nanoseconds(try event.now()) - started;

            const counters = &loop.executor.stats;
            if (elapsed <= counters.longest_poll_ns) return true;
            counters.longest_poll_ns = elapsed;
            if (elapsed >= stall_ns)
                std.debug.print("velo: task {s} held its loop for {d} ms\n", .{ name, elapsed / std.time.ns_per_ms });
            return true;
        }

        fn park(loop: *Loop) !void {
            if (!loop.control.enterPark()) return;
            defer loop.control.leavePark();

            loop.executor.stats.parks += 1;
            try hooks.park(loop);
        }

        pub fn deinit(loop: *Loop) void {
            const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
            defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

            assert(!loop.running);
            if (std.debug.runtime_safety) checkDrained(loop);
            loop.control.deinit();
            assert(loop.active == 0 and loop.pending.first == null);
            assert(!loop.executor.hasReady() and loop.executor.all_tasks.first == null);

            hooks.close(loop);
        }

        /// Schedules a `*Frame(C)`; pin it until its result is consumed.
        pub fn spawn(loop: *Loop, frame: anytype) void {
            loop.executor.spawn(&frame.task);
        }

        /// Spawns `frame`, runs until the loop drains (including other work), and returns
        /// its result. Panics on run errors, since unwinding would free borrowed storage,
        /// and if run returns with any live work (stop, or a task parked without a waker).
        /// Use `spawn` and `run` to stop/resume or retry run errors.
        pub fn block(loop: *Loop, frame: anytype) std.meta.Child(@TypeOf(frame)).Result {
            spawn(loop, frame);
            run(loop) catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

            if (frame.result == null or !drained(loop)) {
                if (std.debug.runtime_safety) {
                    std.debug.print("velo: loop stopped or stalled before all work drained; live tasks:\n", .{});
                    loop.executor.dumpTasks();
                }
                @panic("velo: block returned with live work; use spawn/run for stop and resume");
            }
            return frame.getResult();
        }

        fn drained(loop: *const Loop) bool {
            return loop.executor.all_tasks.first == null and !loop.executor.hasReady() and
                loop.active == 0 and loop.pending.first == null and
                loop.control.registrations.first == null and !loop.control.hasWork();
        }

        // Reports what is still live before the drain assertions fire.
        fn checkDrained(loop: *Loop) void {
            if (drained(loop)) return;

            std.debug.print(
                "velo: loop deinit before draining (active operations: {d}, external work: {}); live tasks:\n",
                .{ loop.active, loop.control.hasWork() },
            );
            loop.executor.dumpTasks();
            @panic("velo: loop deinit with live work; cancel or shut down, then run until drained");
        }

        /// For a long poll, between chunks of work: true once its slice has used the quota.
        /// Return pending through `Yield` then; the next turn goes to I/O. Reads the clock.
        pub fn shouldYield(loop: *Loop) bool {
            if (!loop.executor.slice.over) checkSlice(loop) catch return true;
            return loop.executor.slice.over;
        }

        /// Read on the loop thread, or after `run` returned and every producer finished.
        pub fn stats(loop: *const Loop) Executor.Stats {
            var snapshot = loop.executor.stats;
            snapshot.wakes = loop.control.wakes.load(.monotonic);
            return snapshot;
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
