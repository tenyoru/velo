//! Fixed-size blocking workers; caller-owned jobs have no queue-size bound.
//! Initialize in place; pin pool and loop through deinit. Only actions run off-thread.

const std = @import("std");
const builtin = @import("builtin");

const co = @import("../coroutines/root.zig");
const Control = @import("../event/Control.zig");
const Loop = @import("../event/root.zig").Loop;
const numa = @import("numa/root.zig");
const states = @import("../state.zig");
const ThreadGuard = @import("../ThreadGuard.zig");

const WorkerPool = @This();
const assert = std.debug.assert;

allocator: std.mem.Allocator,
loop: *Loop,
threaded: std.Io.Threaded,
threads: []std.Thread,
mutex: std.Io.Mutex = .init,
condition: std.Io.Condition = .init,
queue: std.DoublyLinkedList = .{},
stopping: bool = false,
outstanding: usize = 0,

pub const Options = struct {
    count: usize,
    /// Borrowed until initWithOptions returns; empty preserves inherited affinity.
    /// Every worker uses the entire Linux-only set; memory policy stays inherited.
    cpus: []const usize = &.{},
};

pub fn init(
    pool: *WorkerPool,
    allocator: std.mem.Allocator,
    loop: *Loop,
    thread_count: usize,
) !void {
    return pool.initWithOptions(allocator, loop, .{ .count = thread_count });
}

pub fn initWithOptions(
    pool: *WorkerPool,
    allocator: std.mem.Allocator,
    loop: *Loop,
    options: Options,
) !void {
    const entered = if (ThreadGuard.enabled) loop.executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) loop.executor.thread_guard.leave(entered);

    if (options.count == 0) return error.InvalidWorkerCount;
    if (builtin.os.tag != .linux and options.cpus.len != 0) return error.Unsupported;

    const threads = try allocator.alloc(std.Thread, options.count);
    pool.* = .{
        .allocator = allocator,
        .loop = loop,
        // Threaded.init swaps process-wide SIGIO/SIGPIPE handlers; pools must not.
        .threaded = .init_single_threaded,
        .threads = threads,
    };

    var startup: Startup = .{ .cpus = options.cpus, .remaining = options.count };
    var started: usize = 0;
    errdefer {
        pool.stopWorkers(started);
        pool.threaded.deinit();
        allocator.free(threads);
    }

    for (threads) |*thread| {
        thread.* = try std.Thread.spawn(.{}, worker, .{ pool, if (options.cpus.len == 0) null else &startup });
        started += 1;
    }

    if (options.cpus.len == 0) return;
    const io = pool.threaded.io();
    pool.mutex.lockUncancelable(io);
    while (startup.remaining != 0) pool.condition.waitUncancelable(io, &pool.mutex);
    const failure = startup.failure;
    pool.mutex.unlock(io);
    if (failure) |err| return err;
}

/// Consume every result before deinit.
pub fn deinit(pool: *WorkerPool) void {
    const executor: *co.Executor = if (ThreadGuard.enabled) &pool.loop.executor else undefined;
    const entered = if (ThreadGuard.enabled) executor.thread_guard.enter() else false;
    defer if (ThreadGuard.enabled) executor.thread_guard.leave(entered);

    assert(pool.outstanding == 0);

    pool.stopWorkers(pool.threads.len);
    pool.allocator.free(pool.threads);
    pool.threaded.deinit();
    pool.* = undefined;
}

fn stopWorkers(pool: *WorkerPool, started_count: usize) void {
    const io = pool.threaded.io();

    pool.mutex.lockUncancelable(io);
    assert(pool.queue.first == null);
    pool.stopping = true;
    pool.condition.broadcast(io);
    pool.mutex.unlock(io);

    for (pool.threads[0..started_count]) |thread| thread.join();
}

const Job = struct {
    pool: *WorkerPool,
    execute: *const fn (*Job, std.Io, bool) void,
    node: std.DoublyLinkedList.Node = .{},
    post: Control.Post = .{ .callback = completed },
    registration: Control.Registration = .{ .cancel = cancelRegistered },
    task: ?*co.Task = null,
    state: states.Job = .idle,
    cancel_requested: bool = false,

    fn completed(post: *Control.Post) void {
        const job: *Job = @alignCast(@fieldParentPtr("post", post));
        assert(job.state == .pending);

        job.state = .complete;
        job.task.?.wake();
    }

    fn cancelRegistered(registration: *Control.Registration) void {
        const job: *Job = @alignCast(@fieldParentPtr("registration", registration));
        job.cancel();
    }

    fn cancel(job: *Job) void {
        if (job.state == .complete or job.state == .done) return;

        const pool = job.pool;
        const executor: *co.Executor = if (ThreadGuard.enabled) &pool.loop.executor else undefined;
        const entered = if (ThreadGuard.enabled) executor.thread_guard.enter() else false;
        defer if (ThreadGuard.enabled) executor.thread_guard.leave(entered);

        const io = pool.threaded.io();

        pool.mutex.lockUncancelable(io);
        job.cancel_requested = true;
        pool.mutex.unlock(io);
    }
};

const Startup = struct {
    cpus: []const usize,
    remaining: usize,
    failure: ?anyerror = null,
};

fn worker(pool: *WorkerPool, startup: ?*Startup) void {
    const io = pool.threaded.io();
    if (startup) |ready| {
        var failure: ?anyerror = null;
        numa.pin(ready.cpus) catch |err| {
            failure = err;
        };
        pool.mutex.lockUncancelable(io);
        if (ready.failure == null) ready.failure = failure;
        ready.remaining -= 1;
        // The initializer and idle workers share this condition.
        if (ready.remaining == 0) pool.condition.broadcast(io);
        pool.mutex.unlock(io);
        if (failure != null) return;
    }

    const control = &pool.loop.control;

    while (true) {
        pool.mutex.lockUncancelable(io);
        while (pool.queue.first == null and !pool.stopping)
            pool.condition.waitUncancelable(io, &pool.mutex);

        if (pool.stopping) {
            pool.mutex.unlock(io);
            return;
        }

        const job: *Job = @alignCast(@fieldParentPtr("node", pool.queue.popFirst().?));
        const canceled = job.cancel_requested;
        pool.mutex.unlock(io);

        job.execute(job, io, canceled);

        // Publication may let the loop consume and destroy the entire frame.
        control.postRelease(&job.post);
    }
}

/// Pin action, buffers and result until ready, even after cancel.
/// Cancel skips unclaimed jobs; claimed jobs return their real result, including owned resources.
pub fn Work(comptime C: type) type {
    const Return = @typeInfo(@TypeOf(C.run)).@"fn".return_type.?;
    const T = @typeInfo(Return).error_union.payload;

    return struct {
        job: Job,
        action: C,
        result: anyerror!T = undefined,

        const Self = @This();

        pub fn init(pool: *WorkerPool, action: C) Self {
            return .{
                .job = .{
                    .pool = pool,
                    .execute = execute,
                },
                .action = action,
            };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(anyerror!T) {
            const job = &self.job;
            const pool = job.pool;
            const entered = if (ThreadGuard.enabled)
                pool.loop.executor.thread_guard.enter()
            else
                false;
            defer if (ThreadGuard.enabled) pool.loop.executor.thread_guard.leave(entered);

            assert(task.executor == &pool.loop.executor);

            switch (job.state) {
                .idle => {
                    job.task = task;

                    if (job.cancel_requested or task.cancel_requested or
                        pool.loop.control.isShuttingDown())
                    {
                        self.result = error.OperationCanceled;
                        job.state = .done;

                        return .{ .ready = self.result };
                    }

                    task.outstanding_io += 1;
                    pool.outstanding += 1;
                    pool.loop.control.register(&job.registration);
                    pool.loop.control.retain();
                    job.state = .pending;

                    const io = pool.threaded.io();
                    pool.mutex.lockUncancelable(io);
                    assert(!pool.stopping);

                    pool.queue.append(&job.node);
                    pool.condition.signal(io);
                    pool.mutex.unlock(io);

                    return .pending;
                },
                .pending => {
                    assert(job.task == task);
                    if (task.cancel_requested or pool.loop.control.isShuttingDown()) job.cancel();

                    return .pending;
                },
                .complete => {
                    assert(job.task == task);

                    pool.loop.control.unregister(&job.registration);
                    task.outstanding_io -= 1;
                    pool.outstanding -= 1;
                    job.state = .done;
                },
                .done => unreachable,
            }

            return .{ .ready = self.result };
        }

        pub fn cancel(self: *Self) void {
            self.job.cancel();
        }

        pub fn deinit(self: *Self) void {
            assert(self.job.state == .idle or self.job.state == .done);
        }

        fn execute(job: *Job, io: std.Io, canceled: bool) void {
            const self: *Self = @alignCast(@fieldParentPtr("job", job));
            self.result = if (canceled) error.OperationCanceled else self.action.run(io);
        }
    };
}

test {
    _ = @import("pool_placement_test.zig");
}
