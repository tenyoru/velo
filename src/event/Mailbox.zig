//! Bounded, typed MPSC messaging between loops and ordinary threads.
//!
//! Initialize in place on the receiving loop with nonempty caller-owned storage.
//! `send(source_loop, value)` is a coroutine on the source loop; `trySend(value)`
//! may be called from any thread and returns Full instead of waiting for capacity.
//! `recv()` and `tryRecv()` belong to the receiving loop. Multiple receiving tasks
//! are served FIFO, but must all run on that one loop. Short critical sections use
//! a native mutex; trySend/tryRecv do not wait for capacity/data, but are not lock-free.
//!
//! Send success means accepted, not processed; use a reply mailbox for acknowledgement.
//! Success transfers ownership of T (not a deep copy); failed Send retains its value.
//! Pointer/slice backing must survive until the receiver finishes using it, not just
//! until send completes. A pending Send's value must not be accessed until ready.
//! Granted sends/receives win late cancellation. Every pending operation drains its
//! notification before ready, even after cancellation, so its frame and both
//! participating loops must stay pinned.
//!
//! An open mailbox retains its receiving loop: run/block cannot finish until close.
//! `close()` is thread-safe and idempotent: it rejects uncommitted sends, wakes waiters,
//! and preserves buffered values. Receiver-loop shutdown also closes the mailbox;
//! use tryRecv to drain owned values during canceled cleanup. Sender-loop shutdown
//! cancels that loop's waiting sends, not the mailbox. Close does not wait for callbacks.
//!
//! Before deinit: close, drain buffered values and every Send/Recv result, let the
//! receiving loop process close, and stop/join all producers. Keep mailbox/storage
//! pinned throughout. Deinit itself needs no live loop and may follow Group.deinit.
//! Capacity bounds buffered values, not caller-owned pending Send frames.

const std = @import("std");
const co = @import("../coroutines/root.zig");
const states = @import("../state.zig");
const ThreadGuard = @import("../ThreadGuard.zig");
const io = @import("root.zig");
const assert = std.debug.assert;

pub fn Mailbox(comptime T: type) type {
    return struct {
        loop: *io.Loop,
        buffer: []T,
        mutex: std.Io.Mutex = .init,
        head: usize = 0,
        tail: usize = 0,
        count: usize = 0,
        closed: bool = false,
        close_done: bool = false,
        active_waits: usize = 0,
        senders: std.DoublyLinkedList = .{},
        receivers: std.DoublyLinkedList = .{},
        registration: io.Control.Registration = .{ .cancel = shutdown },
        close_post: io.Control.Post = .{ .callback = finishClose },

        const Self = @This();
        pub const Error = error{ Canceled, Closed };
        pub const TrySendError = error{ Full, Closed };
        pub const TryRecvError = error{ Empty, Closed };

        pub fn init(self: *Self, loop: *io.Loop, buffer: []T) error{InvalidCapacity}!void {
            if (buffer.len == 0) return error.InvalidCapacity;
            const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
            defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);
            self.* = .{ .loop = loop, .buffer = buffer };
            loop.retain();
            loop.control.register(&self.registration);
            if (loop.control.isShuttingDown()) self.close();
        }

        pub fn send(self: *Self, source_loop: *io.Loop, value: T) Send {
            return .{ .wait = .{ .mailbox = self, .loop = source_loop, .kind = .send }, .value = value };
        }

        pub fn recv(self: *Self) Recv {
            return .{ .wait = .{ .mailbox = self, .loop = self.loop, .kind = .recv } };
        }

        /// On error, the caller still owns value. FIFO waiting senders cannot be bypassed.
        pub fn trySend(self: *Self, value: T) TrySendError!void {
            self.lock();
            defer self.unlock();
            return self.put(value);
        }

        /// Receiving-loop only; also usable after shutdown to clean up buffered resources.
        pub fn tryRecv(self: *Self) TryRecvError!T {
            const entered = if (ThreadGuard.enabled) self.loop.executor.thread_guard.enter() else false;
            defer if (ThreadGuard.enabled) self.loop.executor.thread_guard.leave(entered);
            self.lock();
            defer self.unlock();
            return self.take();
        }

        pub fn close(self: *Self) void {
            self.lock();
            defer self.unlock();
            self.closeLocked();
        }

        pub fn deinit(self: *Self) void {
            assert(self.closed and self.close_done);
            assert(self.count == 0 and self.active_waits == 0);
            assert(self.senders.first == null and self.receivers.first == null);
            self.* = undefined;
        }

        fn lock(self: *Self) void {
            std.Io.Threaded.mutexLockUncancelable(&self.mutex);
        }

        fn unlock(self: *Self) void {
            std.Io.Threaded.mutexUnlock(&self.mutex);
        }

        fn closeLocked(self: *Self) void {
            if (self.closed) return;
            self.closed = true;
            while (self.senders.popFirst()) |node| Wait.fromNode(node).resolve(.closed);
            while (self.receivers.popFirst()) |node| Wait.fromNode(node).resolve(.closed);
            self.loop.post(&self.close_post);
        }

        fn shutdown(registration: *io.Control.Registration) void {
            const self: *Self = @alignCast(@fieldParentPtr("registration", registration));
            self.close();
        }

        fn finishClose(post: *io.Control.Post) void {
            const self: *Self = @alignCast(@fieldParentPtr("close_post", post));
            self.loop.control.unregister(&self.registration);
            self.loop.release();
            self.close_done = true;
        }

        fn push(self: *Self, value: T) void {
            assert(self.count < self.buffer.len);
            self.buffer[self.tail] = value;
            self.tail = (self.tail + 1) % self.buffer.len;
            self.count += 1;
        }

        fn put(self: *Self, value: T) TrySendError!void {
            if (self.closed) return error.Closed;
            if (self.loop.control.isShuttingDown()) {
                self.closeLocked();
                return error.Closed;
            }
            if (self.receivers.popFirst()) |node| {
                assert(self.count == 0);
                const wait = Wait.fromNode(node);
                const receiver: *Recv = @alignCast(@fieldParentPtr("wait", wait));
                receiver.value = value;
                wait.resolve(.granted);
            } else if (self.count < self.buffer.len) {
                assert(self.senders.first == null);
                self.push(value);
            } else return error.Full;
        }

        fn take(self: *Self) TryRecvError!T {
            if (!self.closed and self.loop.control.isShuttingDown()) self.closeLocked();
            if (self.count == 0) return if (self.closed) error.Closed else error.Empty;
            const value = self.buffer[self.head];
            self.head = (self.head + 1) % self.buffer.len;
            self.count -= 1;
            if (self.senders.popFirst()) |node| {
                assert(!self.closed);
                const wait = Wait.fromNode(node);
                const sender: *Send = @alignCast(@fieldParentPtr("wait", wait));
                self.push(sender.value);
                wait.resolve(.granted);
            }
            return value;
        }

        const Wait = struct {
            mailbox: *Self,
            loop: *io.Loop,
            kind: enum { send, recv },
            state: states.Channel = .idle,
            node: std.DoublyLinkedList.Node = .{},
            post: io.Control.Post = .{ .callback = notify },
            registration: io.Control.Registration = .{ .cancel = cancelRegistered },
            task: ?*co.Task = null,
            notified: bool = false,
            finished: bool = false,

            fn fromNode(node: *std.DoublyLinkedList.Node) *Wait {
                return @alignCast(@fieldParentPtr("node", node));
            }

            fn check(self: *Wait, task: *co.Task) void {
                assert(task.executor == &self.loop.executor);
                if (self.task) |owner| assert(owner == task);
                assert(self.state != .done);
                if (task.cancel_requested or self.loop.control.isShuttingDown()) self.cancelLocked();
            }

            fn begin(self: *Wait, task: *co.Task) void {
                assert(self.state == .idle);
                self.task = task;
                self.state = .waiting;
                self.mailbox.active_waits += 1;
                task.outstanding_io += 1;
                self.loop.retain();
                self.loop.control.register(&self.registration);
                switch (self.kind) {
                    .send => self.mailbox.senders.append(&self.node),
                    .recv => self.mailbox.receivers.append(&self.node),
                }
            }

            fn resolve(self: *Wait, state: states.Channel) void {
                assert(self.state == .waiting);
                self.state = state;
                self.loop.post(&self.post);
            }

            fn notify(post: *io.Control.Post) void {
                const self: *Wait = @fieldParentPtr("post", post);
                self.notified = true;
                self.task.?.wake();
            }

            fn finish(self: *Wait) ?Error!void {
                // A spurious wake/cancel must not free the frame before its post runs.
                if (self.state == .waiting or (self.task != null and !self.notified)) return null;
                if (self.task) |task| {
                    self.loop.control.unregister(&self.registration);
                    task.outstanding_io -= 1;
                    self.mailbox.active_waits -= 1;
                    self.loop.release();
                }
                const state = self.state;
                self.state = .done;
                self.finished = true;
                return switch (state) {
                    .granted => {},
                    .canceled => error.Canceled,
                    .closed => error.Closed,
                    else => unreachable,
                };
            }

            fn cancelRegistered(registration: *io.Control.Registration) void {
                const self: *Wait = @fieldParentPtr("registration", registration);
                self.cancel();
            }

            fn cancel(self: *Wait) void {
                // Completion releases the mailbox and loop borrows, so do not touch either again.
                if (self.finished) return;
                const entered = if (ThreadGuard.enabled) self.loop.executor.thread_guard.enter() else false;
                defer if (ThreadGuard.enabled) self.loop.executor.thread_guard.leave(entered);
                self.mailbox.lock();
                defer self.mailbox.unlock();
                self.cancelLocked();
            }

            fn cancelLocked(self: *Wait) void {
                switch (self.state) {
                    .idle => self.state = .canceled,
                    .waiting => {
                        switch (self.kind) {
                            .send => self.mailbox.senders.remove(&self.node),
                            .recv => self.mailbox.receivers.remove(&self.node),
                        }
                        self.resolve(.canceled);
                    },
                    .granted, .canceled, .closed, .done => {},
                }
            }
        };

        pub const Send = struct {
            wait: Wait,
            value: T,

            pub fn poll(self: *Send, task: *co.Task) co.Poll(Error!void) {
                assert(!self.wait.finished);
                const mailbox = self.wait.mailbox;
                const entered = if (ThreadGuard.enabled) self.wait.loop.executor.thread_guard.enter() else false;
                defer if (ThreadGuard.enabled) self.wait.loop.executor.thread_guard.leave(entered);
                mailbox.lock();
                defer mailbox.unlock();
                self.wait.check(task);
                if (self.wait.state == .idle) {
                    if (mailbox.put(self.value)) |_| {
                        self.wait.state = .granted;
                    } else |err| switch (err) {
                        error.Full => self.wait.begin(task),
                        error.Closed => self.wait.state = .closed,
                    }
                }
                return .{ .ready = self.wait.finish() orelse return .pending };
            }

            pub fn cancel(self: *Send) void {
                self.wait.cancel();
            }
        };

        pub const Recv = struct {
            wait: Wait,
            value: T = undefined,

            pub fn poll(self: *Recv, task: *co.Task) co.Poll(Error!T) {
                assert(!self.wait.finished);
                const mailbox = self.wait.mailbox;
                const entered = if (ThreadGuard.enabled) self.wait.loop.executor.thread_guard.enter() else false;
                defer if (ThreadGuard.enabled) self.wait.loop.executor.thread_guard.leave(entered);
                mailbox.lock();
                defer mailbox.unlock();
                self.wait.check(task);
                if (self.wait.state == .idle) {
                    if (mailbox.take()) |value| {
                        self.value = value;
                        self.wait.state = .granted;
                    } else |err| switch (err) {
                        error.Empty => self.wait.begin(task),
                        error.Closed => self.wait.state = .closed,
                    }
                }
                const result = self.wait.finish() orelse return .pending;
                result catch |err| return .{ .ready = err };
                return .{ .ready = self.value };
            }

            pub fn cancel(self: *Recv) void {
                self.wait.cancel();
            }
        };
    };
}
