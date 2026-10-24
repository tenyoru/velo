//! `WithTimeout(C)` bounds a cancelable coroutine with an error-union result by a relative timer.
//!
//! The timer starts only if the child's first poll is pending. A child success always wins,
//! even after expiry, so nothing it owns is dropped; a child error after the timer expired
//! first becomes `error.Timeout`. Any other timer completion also cancels the child. Pin
//! until ready; cancellation requests both and drains them before returning.

const std = @import("std");
const assert = std.debug.assert;
const io = @import("root.zig");
const co = @import("../coroutines/root.zig");

pub fn WithTimeout(comptime C: type) type {
    const R = co.ResultOf(C);
    const info = switch (@typeInfo(R)) {
        .error_union => |info| info,
        else => @compileError("WithTimeout needs an error-union result to report cancellation: " ++ @typeName(C)),
    };
    if (!@hasDecl(C, "cancel")) @compileError("WithTimeout needs `cancel`: " ++ @typeName(C));

    return struct {
        child: C,
        timer: io.Timeout,
        child_result: ?R = null,
        timer_result: ?co.ResultOf(io.Timeout) = null,
        timer_started: bool = false,
        expired: bool = false,
        done: bool = false,
        cancel_requested: bool = false,
        task: ?*co.Task = null,

        const Self = @This();
        pub const Result = (info.error_set || error{Timeout})!info.payload;

        pub fn init(loop: *io.Loop, child: C, options: struct { duration: io.Timespec }) Self {
            return .{
                .child = child,
                .timer = io.Timeout.init(loop, .{ .duration = options.duration }),
            };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(Result) {
            assert(!self.done);
            self.task = task;

            if (self.child_result == null) {
                self.child_result = self.child.poll(task).value();

                // An unpolled timer holds no registration, so an immediate result skips it.
                if (!self.timer_started) {
                    if (self.child_result) |result| return self.finish(result);
                }
            }

            if (self.timer_result == null) {
                self.timer_started = true;
                self.timer_result = self.timer.poll(task).value();

                if (self.timer_result) |timer| {
                    if (self.child_result == null) {
                        self.expired = if (timer) |_| true else |_| false;
                        self.child.cancel();
                        // Like Frame/Race, cancellation must schedule another poll even
                        // when the child's cancel callback only sets a flag.
                        task.wake();
                    }
                }
            }

            if (self.child_result != null and self.timer_result == null) self.timer.cancel();

            const result = self.child_result orelse return .pending;
            if (self.timer_result == null) return .pending;

            return self.finish(result catch |err| if (self.expired) error.Timeout else err);
        }

        pub fn cancel(self: *Self) void {
            if (self.cancel_requested or self.done) return;

            self.cancel_requested = true;
            if (self.child_result == null) self.child.cancel();
            if (self.timer_result == null) self.timer.cancel();
            if (self.task) |task| task.wake();
        }

        fn finish(self: *Self, result: Result) co.Poll(Result) {
            self.done = true;
            return .{ .ready = result };
        }
    };
}
