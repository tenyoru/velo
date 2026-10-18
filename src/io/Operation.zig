const std = @import("std");
const linux = std.os.linux;
const assert = std.debug.assert;

const coroutines = @import("../coroutines/root.zig");
const io = @import("root.zig");
const states = @import("../state.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const backend = io.backend;
const Loop = io.Loop;
const Control = io.Control;
const Socket = io.Socket;
const SockAddr = io.SockAddr;
const SockLen = io.SockLen;
const Timespec = io.Timespec;
const NativeError = io.NativeError;
const Error = io.Error;
const Operation = @This();

loop: *Loop,
data: Data,
linked: bool,
time: Timespec,
node: std.DoublyLinkedList.Node = .{},
task: ?*coroutines.Task = null,
state: states.Operation = .idle,
primary: ?i32 = null,
timer: ?i32 = null,
completion: ?(Error!usize) = null,
cancel_requested: bool = false,
native: backend.OperationState = .{},
registration: Control.Registration = .{ .cancel = cancelRegistered },

pub const Kind = enum { connect, accept, send, recv, sendto, recvfrom, close, timeout };

pub const Data = union(Kind) {
    connect: struct {
        fd: Socket,
        address: *const SockAddr,
        address_len: SockLen,
        deadline: ?Timespec = null,
    },
    accept: struct {
        fd: Socket,
        deadline: ?Timespec = null,
    },
    send: struct {
        fd: Socket,
        buffer: []const u8,
        deadline: ?Timespec = null,
    },
    recv: struct {
        fd: Socket,
        buffer: []u8,
        deadline: ?Timespec = null,
    },
    sendto: struct {
        fd: Socket,
        buffer: []const u8,
        address: *const SockAddr,
        address_len: SockLen,
        deadline: ?Timespec = null,
    },
    recvfrom: struct {
        fd: Socket,
        buffer: []u8,
        address: *SockAddr,
        address_len: *SockLen,
        deadline: ?Timespec = null,
    },
    close: struct {
        fd: Socket,
    },
    timeout: struct {
        duration: Timespec,
    },
};

pub fn Coroutine(comptime kind: Kind) type {
    const T = switch (kind) {
        .accept => Socket,
        .send, .recv, .sendto, .recvfrom => usize,
        .connect, .close, .timeout => void,
    };

    return struct {
        operation: Operation,

        const Self = @This();

        pub fn init(loop: *Loop, options: @FieldType(Data, @tagName(kind))) Self {
            const deadline = switch (kind) {
                .connect, .accept, .send, .recv, .sendto, .recvfrom => options.deadline,
                else => null,
            };

            return .{
                .operation = .{
                    .loop = loop,
                    .data = @unionInit(Data, @tagName(kind), options),
                    .linked = deadline != null,
                    .time = if (kind == .timeout) options.duration else deadline orelse .{
                        .sec = 0,
                        .nsec = 0,
                    },
                },
            };
        }

        pub fn poll(self: *Self, task: *coroutines.Task) coroutines.Poll(Error!T) {
            const op = &self.operation;
            const entered = if (ThreadGuard.enabled)
                op.loop.executor.thread_guard.enter()
            else
                false;
            defer if (ThreadGuard.enabled) op.loop.executor.thread_guard.leave(entered);

            assert(task.executor == &op.loop.executor);

            switch (op.state) {
                .idle => {
                    op.task = task;

                    if (op.cancel_requested or task.cancel_requested or
                        op.loop.control.isShuttingDown())
                    {
                        op.state = .done;
                        op.completion = error.OperationCanceled;

                        return .{ .ready = error.OperationCanceled };
                    }

                    if (comptime kind == .sendto or kind == .recvfrom) {
                        if (@field(op.data, @tagName(kind)).buffer.len > std.math.maxInt(i32)) {
                            op.state = .done;
                            op.completion = error.MessageTooBig;

                            return .{ .ready = error.MessageTooBig };
                        }
                    }

                    task.outstanding_io += 1;
                    op.loop.active += 1;
                    op.loop.control.register(&op.registration);
                    op.state = .queued;
                    op.loop.pending.append(&op.node);

                    return .pending;
                },
                .queued, .inflight => {
                    assert(op.task == task);
                    return .pending;
                },
                .complete => {
                    assert(op.task == task);

                    task.outstanding_io -= 1;
                    op.loop.active -= 1;
                    op.loop.control.unregister(&op.registration);
                    op.state = .done;
                },
                .done => assert(op.task == task),
            }

            const n = op.completion.? catch |err| return .{ .ready = err };
            return .{ .ready = if (T == void) {} else @intCast(n) };
        }

        /// Requests cancellation; keep polling until ready before releasing storage.
        pub fn cancel(self: *Self) void {
            self.operation.requestCancel();
        }

        pub fn deinit(self: *Self) void {
            assert(self.operation.state == .idle or self.operation.state == .done);
        }

        pub fn primaryError(self: *const Self) ?NativeError {
            return backend.errno(self.operation.primary);
        }

        pub fn deadlineError(self: *const Self) ?NativeError {
            return backend.errno(self.operation.timer);
        }
    };
}

fn cancelRegistered(registration: *Control.Registration) void {
    const op: *Operation = @fieldParentPtr("registration", registration);
    op.requestCancel();
}

pub fn requestCancel(op: *Operation) void {
    if (op.cancel_requested or op.state == .complete or op.state == .done) return;

    const entered = if (ThreadGuard.enabled) op.loop.executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) op.loop.executor.thread_guard.leave(entered);

    op.cancel_requested = true;

    switch (op.state) {
        .idle => {},
        .queued => {
            op.loop.pending.remove(&op.node);
            op.finish(error.OperationCanceled);
        },
        .inflight => op.loop.cancel(op),
        .complete, .done => unreachable,
    }
}

pub fn prepare(op: *Operation, sqe: *linux.io_uring_sqe, timer: ?*linux.io_uring_sqe) void {
    backend.prepare(op, sqe, timer);
}

pub fn complete(op: *Operation, is_timer: bool, res: i32) void {
    backend.complete(op, is_timer, res);
}

pub fn finish(op: *Operation, result: Error!usize) void {
    assert(op.state == .queued or op.state == .inflight);

    op.completion = result;
    op.state = .complete;
    op.task.?.wake();
}
