//! Stream transfers preserve partial progress in `offset` on error. Cancellation
//! drains the active child before returning; referenced buffers must stay pinned.

const io = @import("root.zig");
const co = @import("../coroutines/root.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const assert = @import("std").debug.assert;

pub const Error = io.Error || error{ EndOfStream, WriteZero };
pub const SendAll = Transfer(true);
pub const ReadExact = Transfer(false);

fn Transfer(comptime sending: bool) type {
    const Child = if (sending) io.Send else io.Recv;
    const Buffer = if (sending) []const u8 else []u8;

    return struct {
        loop: *io.Loop,
        fd: io.Socket,
        buffer: Buffer,
        deadline: ?io.Timespec,
        offset: usize = 0,
        child: Child = undefined,
        pending: bool = false,
        canceled: bool = false,
        result: ?(Error!void) = null,

        const Self = @This();

        pub fn init(
            loop: *io.Loop,
            options: struct {
                fd: io.Socket,
                buffer: Buffer,
                deadline: ?io.Timespec = null,
            },
        ) Self {
            return .{
                .loop = loop,
                .fd = options.fd,
                .buffer = options.buffer,
                .deadline = options.deadline,
            };
        }

        pub fn cancel(self: *Self) void {
            if (self.result != null) return;

            const entered = if (ThreadGuard.enabled)
                self.loop.executor.thread_guard.enter()
            else
                false;
            defer if (ThreadGuard.enabled) self.loop.executor.thread_guard.leave(entered);

            self.canceled = true;
            if (self.pending) self.child.cancel();
        }

        pub fn deinit(self: *Self) void {
            assert(!self.pending);
        }

        fn finish(self: *Self, result: Error!void) co.Poll(Error!void) {
            self.result = result;
            return .{ .ready = result };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(Error!void) {
            assert(self.result == null);

            const entered = if (ThreadGuard.enabled)
                self.loop.executor.thread_guard.enter()
            else
                false;
            defer if (ThreadGuard.enabled) self.loop.executor.thread_guard.leave(entered);

            if (task.cancel_requested or self.loop.control.isShuttingDown()) self.cancel();
            if (self.canceled and self.buffer.len == 0) return self.finish(error.OperationCanceled);

            while (self.offset < self.buffer.len) {
                if (!self.pending) {
                    if (self.canceled) return self.finish(error.OperationCanceled);

                    self.child = Child.init(self.loop, .{
                        .fd = self.fd,
                        .buffer = self.buffer[self.offset..],
                        .deadline = self.deadline,
                    });
                    self.pending = true;
                }

                const result = switch (self.child.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result,
                };
                self.pending = false;

                const n = result catch |err| return self.finish(err);
                if (n == 0) return self.finish(if (sending) error.WriteZero else error.EndOfStream);
                self.offset += n;
            }

            return self.finish({});
        }
    };
}
