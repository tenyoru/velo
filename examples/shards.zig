const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const numa = velo.workers.numa;
const Placement = velo.workers.Group.Placement;
const Pool = velo.workers.Pool;

const max_shards = 64;
const Mode = enum { auto, epoll };
const Scenario = enum { normal, cancel, shutdown, closed, late_cancel, shutdown_all };

fn holdsReply(scenario: Scenario) bool {
    return scenario == .cancel or scenario == .shutdown or scenario == .shutdown_all;
}
const Config = struct {
    shards: usize = 2,
    requests: usize = 10_000,
    mode: Mode = .auto,
    numa: bool = false,
    scenario: Scenario = .normal,
    fail_after: ?usize = null,
    fail_prepare_after: ?usize = null,
    fail_work_after: ?usize = null,
};

fn hash(key: u64) u64 {
    return std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
}

fn route(key: u64, count: usize) usize {
    return @intCast(hash(key) % count);
}

const Reply = struct { id: u64, destination: usize, value: u64, canceled: bool };

const Request = struct {
    // Separate nodes let the receiver publish a reply before its request callback returns.
    request_post: io.Control.Post = .{ .callback = receive },
    reply_post: io.Control.Post = .{ .callback = respond },
    source: *Shard = undefined,
    destination: *Shard = undefined,
    id: u64 = 0,
    key: u64 = 0,
    payload: u64 = 0,
    // Only the receiver writes the reply; publication hands it back to the sender.
    reply: Reply = undefined,
    canceled: std.atomic.Value(bool) = .init(false),
    task: ?*co.Task = null,
    active: bool = false,
    available: bool = false,
    started: std.Io.Timestamp = .zero,

    fn poll(self: *@This(), task: *co.Task) co.Poll(error{Canceled}!Reply) {
        if (!self.active) {
            if (task.cancel_requested or self.source.loop.control.isShuttingDown() or
                self.canceled.load(.acquire))
                return .{ .ready = error.Canceled };

            self.task = task;
            self.active = true;
            task.outstanding_io += 1;
            self.source.accepted += 1;
            if (self.source.coordinator.config.numa) {
                if (self.source.node.? == self.destination.node.?) {
                    self.source.local_requests += 1;
                } else self.source.cross_node_requests += 1;
            }
            self.started = std.Io.Clock.awake.now(self.source.coordinator.std_io);
            self.destination.loop.post(&self.request_post);
            return .pending;
        }

        if (!self.available) return .pending;

        self.source.round_trip_ns += @intCast(self.started.durationTo(
            std.Io.Clock.awake.now(self.source.coordinator.std_io),
        ).nanoseconds);
        // A published reply still borrows the frame until its owner consumes it.
        task.outstanding_io -= 1;
        self.active = false;
        self.available = false;
        self.task = null;
        return .{ .ready = self.reply };
    }

    fn cancel(self: *@This()) void {
        self.canceled.store(true, .release);
    }

    fn receive(post: *io.Control.Post) void {
        const self: *@This() = @alignCast(@fieldParentPtr("request_post", post));
        const destination = self.destination;
        const source = self.source.index;
        const config = destination.coordinator.config;
        destination.bad = destination.bad or
            std.Thread.getCurrentId() != destination.thread_id or
            route(self.key, config.shards) != destination.index or
            self.key != self.id or self.payload != self.id ^ 0xa0761d6478bd642f or
            self.id < source * config.requests or self.id >= (source + 1) * config.requests;
        if (destination.last_ids[source]) |last| {
            destination.bad = destination.bad or self.id <= last;
        }
        destination.last_ids[source] = self.id;
        destination.received += 1;

        if (self.id == 0 and holdsReply(config.scenario)) {
            destination.held = self;
            destination.coordinator.held.set(destination.coordinator.std_io);
            return;
        }

        self.finish();
    }

    fn finish(self: *@This()) void {
        self.reply = .{
            .id = self.id,
            .destination = self.destination.index,
            .value = self.payload +% hash(self.key),
            .canceled = self.canceled.load(.acquire) or self.destination.loop.control.isShuttingDown(),
        };
        // Ownership passes back here; the receiver must not touch the request afterward.
        self.source.loop.post(&self.reply_post);
    }

    fn respond(post: *io.Control.Post) void {
        const self: *@This() = @alignCast(@fieldParentPtr("reply_post", post));
        const source = self.source;
        std.debug.assert(self.active and !self.available and self.task.?.outstanding_io == 1);
        source.bad = source.bad or std.Thread.getCurrentId() != source.thread_id;
        source.responses += 1;
        self.available = true;
        if (source.coordinator.config.scenario == .late_cancel and self.id == 0) source.frame.cancel();
        self.task.?.wake();
    }
};

const Warmup = struct {
    cpus: []const usize,
    fail: bool = false,
    started: ?*std.Io.Event = null,
    release: ?*std.Io.Event = null,

    pub fn run(self: *@This(), std_io: std.Io) !std.Thread.Id {
        if (self.started) |started| started.set(std_io);
        if (self.release) |release| release.waitUncancelable(std_io);
        if (self.cpus.len != 0) {
            const actual = try numa.allowedCpus(std.heap.page_allocator);
            defer std.heap.page_allocator.free(actual);
            if (!std.mem.eql(usize, self.cpus, actual)) return error.WorkerAffinityMismatch;
        }
        if (self.fail) return error.WorkerWarmupFailed;
        return std.Thread.getCurrentId();
    }
};

const Sender = struct {
    shard: *Shard = undefined,
    request: Request = .{},
    work: ?*Pool.Work(Warmup) = null,
    exchange_ready: bool = false,
    consumed: usize = 0,
    succeeded: usize = 0,
    canceled: usize = 0,
    rejected: usize = 0,

    pub fn cancel(self: *@This()) void {
        if (self.work) |work| work.cancel();
        self.request.cancel();
    }

    fn done(self: *@This()) co.Poll(void) {
        if (self.shard.pool_live) {
            // Group.cleanup runs after loop storage is gone; join workers here instead.
            self.shard.pool.deinit();
            self.shard.pool_live = false;
        }
        self.shard.done.set(self.shard.coordinator.std_io);
        return .{ .ready = {} };
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
        const config = self.shard.coordinator.config;
        if (self.work) |work| {
            const result = switch (work.poll(task)) {
                .pending => return .pending,
                .ready => |result| result,
            };
            self.shard.worker_completed_on = std.Thread.getCurrentId();
            if (result) |thread| {
                self.shard.worker_thread = thread;
                if (thread == self.shard.thread_id or self.shard.worker_completed_on.? != self.shard.thread_id)
                    self.shard.worker_error = error.WorkerCompletionMismatch;
            } else |err| self.shard.worker_error = err;
            work.deinit();
            self.shard.arena.?.allocator().destroy(work);
            self.work = null;
            self.shard.warmed.set(self.shard.coordinator.std_io);
        }
        if (self.shard.pool_live and !self.exchange_ready) {
            if (task.cancel_requested or self.shard.loop.control.isShuttingDown() or self.shard.worker_error != null)
                return self.done();
            return .pending;
        }
        if (config.numa and config.scenario == .closed and self.shard.index == 0)
            self.shard.loop.shutdown();
        if (self.shard.first_poll == null)
            self.shard.first_poll = std.Io.Clock.awake.now(self.shard.coordinator.std_io);
        if (!self.request.active) {
            self.request.id = self.shard.index * config.requests + self.consumed;
            self.request.key = self.request.id;
            self.request.payload = self.request.id ^ 0xa0761d6478bd642f;
            self.request.destination = self.shard.coordinator.shards[route(self.request.key, config.shards)];
            if (!task.cancel_requested) self.request.canceled.store(false, .release);
        }

        const reply = switch (self.request.poll(task)) {
            .pending => {
                if (task.cancel_requested and self.shard.index == 0) {
                    self.shard.borrow_held = task.outstanding_io == 1 and task.registered and
                        self.shard.frame.result == null;
                    self.shard.coordinator.cancel_parked.set(self.shard.coordinator.std_io);
                }
                return .pending;
            },
            .ready => |result| result catch {
                self.rejected += 1;
                return self.done();
            },
        };

        self.shard.bad = self.shard.bad or reply.id != self.request.id or
            reply.destination != route(self.request.key, config.shards) or
            reply.value != self.request.payload +% hash(self.request.key);
        self.consumed += 1;
        if (reply.canceled) self.canceled += 1 else self.succeeded += 1;
        if (task.cancel_requested or self.consumed == config.requests) return self.done();

        task.wake();
        return .pending;
    }
};

const Shard = struct {
    loop: *io.Loop = undefined,
    coordinator: *Coordinator,
    index: usize,
    cpu: ?usize = null,
    node: ?usize = null,
    state_region: ?numa.Region = null,
    pages: numa.PageAllocator = undefined,
    arena: ?std.heap.ArenaAllocator = null,
    cpu_storage: []usize = &.{},
    worker_cpus: []const usize = &.{},
    buffers: [1][]u8 = undefined,
    prepared: bool = false,
    cleaned: bool = false,
    pool: Pool = undefined,
    pool_live: bool = false,
    warmed: std.Io.Event = .unset,
    worker_thread: ?std.Thread.Id = null,
    worker_completed_on: ?std.Thread.Id = null,
    worker_error: ?anyerror = null,
    start_post: io.Control.Post = .{ .callback = startExchange },
    frame: co.Frame(Sender) = .{ .coroutine = .{} },
    done: std.Io.Event = .unset,
    thread_id: std.Thread.Id = undefined,
    first_poll: ?std.Io.Timestamp = null,
    accepted: usize = 0,
    received: usize = 0,
    responses: usize = 0,
    local_requests: usize = 0,
    cross_node_requests: usize = 0,
    round_trip_ns: u128 = 0,
    last_ids: [max_shards]?u64 = @splat(null),
    held: ?*Request = null,
    borrow_held: bool = false,
    bad: bool = false,

    fn startExchange(post: *io.Control.Post) void {
        const self: *@This() = @alignCast(@fieldParentPtr("start_post", post));
        self.frame.coroutine.exchange_ready = true;
        self.frame.task.wake();
    }
};

const Coordinator = struct {
    config: Config,
    std_io: std.Io,
    shards: []*Shard,
    topology: ?*const numa.Topology = null,
    group: velo.workers.Group = undefined,
    held: std.Io.Event = .unset,
    cancel_parked: std.Io.Event = .unset,
    cancel_post: io.Control.Post = .{ .callback = cancelSource },
    resume_post: io.Control.Post = .{ .callback = resumeReply },

    fn prepare(self: *@This(), index: usize, base: io.LoopOptions) !io.LoopOptions {
        if (!self.config.numa) return base;
        const shard = self.shards[index];
        shard.pages = try numa.PageAllocator.init(.{ .bind = shard.node.? }, .{ .prefault = true });
        shard.arena = .init(shard.pages.allocator());
        errdefer {
            shard.arena.?.deinit();
            shard.arena = null;
        }
        shard.cpu_storage = try self.topology.?.nodeCpus(std.heap.page_allocator, shard.node.?);
        errdefer {
            std.heap.page_allocator.free(shard.cpu_storage);
            shard.cpu_storage = &.{};
            shard.worker_cpus = &.{};
        }
        shard.worker_cpus = shard.cpu_storage;
        const cpu = shard.cpu.?;
        if (shard.cpu_storage.len > 1) {
            var count: usize = 0;
            for (shard.cpu_storage) |id| {
                if (id == cpu) continue;
                shard.cpu_storage[count] = id;
                count += 1;
            }
            // Overlapping masks can leave io-wq pinned to the loop's inherited CPU.
            shard.worker_cpus = shard.cpu_storage[0..count];
        }
        const buffer = try shard.arena.?.allocator().alignedAlloc(u8, .fromByteUnits(std.heap.page_size_min), std.heap.pageSize());
        @memset(buffer, 0);
        if (self.config.fail_prepare_after == index) return error.ThreadPrepareFailed;
        shard.buffers = .{buffer};
        shard.prepared = true;
        var options = base;
        options.buffers = &shard.buffers;
        options.iowq_cpus = shard.worker_cpus;
        return options;
    }

    fn cleanup(self: *@This(), index: usize) void {
        if (!self.config.numa) return;
        const shard = self.shards[index];
        std.debug.assert(shard.prepared and !shard.cleaned and !shard.pool_live);
        shard.arena.?.deinit();
        shard.arena = null;
        std.heap.page_allocator.free(shard.cpu_storage);
        shard.cpu_storage = &.{};
        shard.worker_cpus = &.{};
        shard.cleaned = true;
    }

    fn entry(self: *@This(), loop: *io.Loop, index: usize) !void {
        const shard = self.shards[index];
        shard.loop = loop;
        shard.thread_id = std.Thread.getCurrentId();
        errdefer {
            if (shard.frame.coroutine.work) |work| {
                work.deinit();
                shard.arena.?.allocator().destroy(work);
                shard.frame.coroutine.work = null;
            }
            if (shard.pool_live) {
                shard.pool.deinit();
                shard.pool_live = false;
            }
        }
        if (shard.prepared) {
            const allocator = shard.arena.?.allocator();
            try shard.pool.initWithOptions(allocator, loop, .{ .count = 1, .cpus = shard.worker_cpus });
            shard.pool_live = true;
            const work = try allocator.create(Pool.Work(Warmup));
            work.* = .init(&shard.pool, .{ .cpus = shard.worker_cpus, .fail = self.config.fail_work_after == index });
            shard.frame.coroutine.work = work;
        }
        if (self.config.fail_after == index) return error.ThreadStartFailed;
        if (!self.config.numa and self.config.scenario == .closed and index == 0) loop.shutdown();
        loop.executor.spawn(&shard.frame.task);
    }

    fn cancelSource(post: *io.Control.Post) void {
        const self: *@This() = @fieldParentPtr("cancel_post", post);
        const source = self.shards[0];
        if (self.config.scenario == .shutdown_all) {
            self.group.shutdown();
        } else if (self.config.scenario == .shutdown) source.loop.shutdown() else source.frame.cancel();
    }

    fn resumeReply(post: *io.Control.Post) void {
        const self: *@This() = @fieldParentPtr("resume_post", post);
        const destination = self.shards[route(0, self.shards.len)];
        const request = destination.held.?;
        destination.held = null;
        request.finish();
    }
};

const Result = struct {
    requests: usize = 0,
    succeeded: usize = 0,
    canceled: usize = 0,
    rejected: usize = 0,
    destinations: usize = 0,
    elapsed_ns: u64 = 0,
    drain_ns: u64 = 0,
    mean_round_trip_ns: u64 = 0,
    // Count accepted posts, including canceled replies, but not unsubmitted rejections.
    local_requests: usize = 0,
    cross_node_requests: usize = 0,
    workers_verified: usize = 0,
    placement: [max_shards]Placement = @splat(.{}),
};

// Loops are gone by now; their own deinit asserts they drained.
fn drained(shard: *const Shard) !void {
    if (shard.held != null or shard.frame.task.outstanding_io != 0) return error.LoopNotDrained;
    if (shard.pool_live or shard.frame.coroutine.work != null) return error.WorkersNotReleased;
    if (shard.arena != null or shard.cpu_storage.len != 0 or shard.prepared != shard.cleaned)
        return error.BuffersNotReleased;
}

fn exercise(allocator: std.mem.Allocator, std_io: std.Io, config: Config) !Result {
    if (config.shards == 0 or config.shards > max_shards or config.requests == 0 or
        config.requests > 10_000_000 or config.shards * config.requests > 100_000_000 or
        (config.mode == .epoll and builtin.os.tag != .linux)) return error.InvalidArguments;

    var placements: [max_shards]Placement = @splat(.{});
    var topology: ?numa.Topology = null;
    defer if (topology) |*snapshot| snapshot.deinit();
    if (config.numa) {
        topology = try numa.Topology.discover(allocator, std_io);
        const planned = try numa.plan(allocator, &topology.?, .{ .count = config.shards, .memory = .bind });
        defer allocator.free(planned);
        @memcpy(placements[0..config.shards], planned);
    }

    const shards = try allocator.alloc(*Shard, config.shards);
    defer allocator.free(shards);
    var initialized: usize = 0;
    defer for (shards[0..initialized]) |shard| {
        if (shard.state_region) |backing| {
            // The Region handle must outlive the state mapping that contains its copy.
            var region = backing;
            region.deinit();
        } else allocator.destroy(shard);
    };
    var coordinator: Coordinator = .{
        .config = config,
        .std_io = std_io,
        .shards = shards,
        .topology = if (topology) |*snapshot| snapshot else null,
    };
    for (shards, 0..) |*slot, index| {
        const region: ?numa.Region = if (config.numa)
            try numa.Region.initWithOptions(@sizeOf(Shard), placements[index].memory.?, .{ .prefault = true })
        else
            null;
        const shard: *Shard = if (region) |backing|
            @ptrCast(@alignCast(backing.bytes.ptr))
        else
            try allocator.create(Shard);
        shard.* = .{
            .coordinator = &coordinator,
            .index = index,
            .cpu = if (config.numa) placements[index].cpu else null,
            .node = if (config.numa) placements[index].memory.?.bind else null,
            .state_region = region,
        };
        slot.* = shard;
        initialized += 1;
        shard.frame.coroutine.shard = shard;
        shard.frame.coroutine.request.source = shard;
    }

    // The group keeps every destination alive until every producer has consumed its last reply.
    coordinator.group.initWithHooks(allocator, .{
        .count = config.shards,
        .loop = .{ .entries = 8 },
        .backend = if (builtin.os.tag == .linux and config.mode == .epoll) .epoll else .auto,
        .placement = if (config.numa) placements[0..config.shards] else &.{},
    }, &coordinator, Coordinator.entry, .{ .prepare = Coordinator.prepare, .cleanup = Coordinator.cleanup }) catch |err| {
        for (shards) |shard| try drained(shard);
        return err;
    };

    if (config.numa) {
        var failure: ?anyerror = null;
        for (shards) |shard| {
            shard.warmed.waitUncancelable(std_io);
            if (failure == null) failure = shard.worker_error;
        }
        if (failure) |err| {
            coordinator.group.shutdown();
            coordinator.group.deinit();
            for (shards) |shard| try drained(shard);
            return err;
        }
        for (shards) |shard| shard.loop.post(&shard.start_post);
    }

    if (holdsReply(config.scenario)) {
        coordinator.held.waitUncancelable(std_io);
        shards[0].loop.post(&coordinator.cancel_post);
        coordinator.cancel_parked.waitUncancelable(std_io);
        shards[route(0, shards.len)].loop.post(&coordinator.resume_post);
    }
    for (shards) |shard| shard.done.waitUncancelable(std_io);
    const finished = std.Io.Clock.awake.now(std_io);
    coordinator.group.shutdown();
    coordinator.group.deinit();

    // Thread spawn and loop setup stay out of the throughput window.
    var started = finished;
    for (shards) |shard| {
        const first = shard.first_poll.?;
        if (first.nanoseconds < started.nanoseconds) started = first;
    }
    var result: Result = .{
        .elapsed_ns = @intCast(started.durationTo(finished).nanoseconds),
        .drain_ns = @intCast(finished.durationTo(std.Io.Clock.awake.now(std_io)).nanoseconds),
        .placement = placements,
    };
    var received: usize = 0;
    var round_trip_ns: u128 = 0;
    for (shards) |shard| {
        try drained(shard);
        const sender = &shard.frame.coroutine;
        if (shard.bad or shard.frame.result == null or shard.accepted != sender.consumed or
            shard.responses != sender.consumed or sender.succeeded + sender.canceled != sender.consumed)
            return error.IncorrectResults;
        if (config.scenario == .normal and
            (sender.succeeded != config.requests or sender.rejected != 0)) return error.IncorrectResults;
        if (config.numa and (!shard.prepared or
            shard.local_requests + shard.cross_node_requests != shard.accepted)) return error.IncorrectResults;
        if (config.numa) {
            if (shard.worker_error) |err| return err;
            if (shard.worker_thread == null or shard.worker_thread.? == shard.thread_id or
                shard.worker_completed_on != shard.thread_id) return error.WorkerCompletionMismatch;
            result.workers_verified += 1;
        }
        result.requests += sender.consumed;
        result.succeeded += sender.succeeded;
        result.canceled += sender.canceled;
        result.rejected += sender.rejected;
        result.local_requests += shard.local_requests;
        result.cross_node_requests += shard.cross_node_requests;
        result.destinations += @intFromBool(shard.received != 0);
        received += shard.received;
        round_trip_ns += shard.round_trip_ns;
    }
    if (received != result.requests) return error.IncorrectResults;
    if (config.numa and result.local_requests + result.cross_node_requests != result.requests)
        return error.IncorrectResults;
    result.mean_round_trip_ns = @intCast(round_trip_ns / @max(result.requests, 1));
    if (holdsReply(config.scenario)) {
        const source = shards[0];
        if (!source.borrow_held or source.accepted != 1 or source.frame.coroutine.canceled != 1 or
            source.frame.coroutine.rejected != 0) return error.CancelDidNotDrain;
    }
    if (config.scenario == .closed and
        (shards[0].accepted != 0 or shards[0].frame.coroutine.rejected != 1))
        return error.ShutdownDidNotReject;
    if (config.scenario == .late_cancel and
        (shards[0].accepted != 1 or shards[0].frame.coroutine.succeeded != 1 or result.canceled != 0))
        return error.LateCancelLostSuccess;
    return result;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var config: Config = .{};
    if (args.len != 1 and args.len != 3 and args.len != 4 and args.len != 5) {
        std.debug.print("Usage: {s} [shards requests-per-shard [auto|epoll [numa]]]\n", .{args[0]});
        return error.InvalidArguments;
    }
    if (args.len >= 3) {
        config.shards = std.fmt.parseInt(usize, args[1], 10) catch return error.InvalidArguments;
        config.requests = std.fmt.parseInt(usize, args[2], 10) catch return error.InvalidArguments;
    }
    if (args.len >= 4) config.mode = std.meta.stringToEnum(Mode, args[3]) orelse return error.InvalidArguments;
    if (args.len == 5) {
        if (!std.mem.eql(u8, args[4], "numa")) return error.InvalidArguments;
        config.numa = true;
    }
    const result = try exercise(init.gpa, init.io, config);
    const rate = @as(f64, @floatFromInt(result.requests)) * 1_000_000_000 /
        @as(f64, @floatFromInt(@max(result.elapsed_ns, 1)));
    std.debug.print("shards={d} requests={d} elapsed_ns={d} requests_per_second={d:.0} mean_round_trip_ns={d} drain_ns={d}\n", .{
        config.shards, result.requests, result.elapsed_ns, rate, result.mean_round_trip_ns, result.drain_ns,
    });
    if (config.numa) {
        std.debug.print("local_requests={d} cross_node_requests={d} workers_verified={d}\n", .{
            result.local_requests, result.cross_node_requests, result.workers_verified,
        });
        for (result.placement[0..config.shards], 0..) |placement, index| {
            std.debug.print("shard={d} cpu={d} node={d}\n", .{ index, placement.cpu.?, placement.memory.?.bind });
        }
        std.debug.print("Per-shard buffers demonstrate registration setup (ignored by epoll); messages are not fixed-buffer file I/O.\n", .{});
    }
}

test "shards route bounded requests and consume each ID exactly once" {
    inline for (if (builtin.os.tag == .linux) .{ Mode.auto, Mode.epoll } else .{Mode.auto}) |mode| {
        for ([_]usize{ 1, 2, 4, 8 }) |count| {
            const result = try exercise(std.testing.allocator, std.testing.io, .{
                .shards = count,
                .requests = 64,
                .mode = mode,
            });
            try std.testing.expectEqual(count * 64, result.requests);
            try std.testing.expectEqual(count * 64, result.succeeded);
            try std.testing.expectEqual(count, result.destinations);
        }
    }
}

test "shards cancellation and shutdown keep the request borrowed until delayed reply consumption" {
    inline for (if (builtin.os.tag == .linux) .{ Mode.auto, Mode.epoll } else .{Mode.auto}) |mode| {
        for ([_]Scenario{ .cancel, .shutdown, .closed, .late_cancel, .shutdown_all }) |scenario| {
            const result = try exercise(std.testing.allocator, std.testing.io, .{
                .shards = 3,
                .requests = 13,
                .mode = mode,
                .scenario = scenario,
            });
            if (scenario == .shutdown_all) {
                try std.testing.expect(result.requests >= 1 and result.requests <= 27);
                try std.testing.expect(result.canceled >= 1);
            } else {
                try std.testing.expectEqual(if (scenario == .closed) @as(usize, 26) else 27, result.requests);
                try std.testing.expectEqual(@intFromBool(scenario == .closed), result.rejected);
            }
            try std.testing.expectEqual(result.requests, result.succeeded + result.canceled);
        }
    }
}

test "shards failed thread start joins started threads and releases every loop" {
    try std.testing.expectError(error.ThreadStartFailed, exercise(std.testing.allocator, std.testing.io, .{
        .shards = 3,
        .requests = 1,
        .fail_after = 1,
    }));
}

test "shards sender drains queued and running warmup cancellation before joining its pool" {
    inline for (if (builtin.os.tag == .linux) .{ Mode.auto, Mode.epoll } else .{Mode.auto}) |mode| {
        for ([_]bool{ false, true }) |queued| {
            var loop = if (builtin.os.tag == .linux)
                try io.Loop.initWithBackend(.{ .entries = 8 }, if (mode == .epoll) .epoll else .auto)
            else
                try io.Loop.init(.{ .entries = 8 });
            defer loop.deinit();
            var shard: Shard = undefined;
            var shards = [_]*Shard{&shard};
            var coordinator: Coordinator = .{ .config = .{}, .std_io = std.testing.io, .shards = &shards };
            shard = .{
                .coordinator = &coordinator,
                .index = 0,
                .loop = &loop,
                .thread_id = std.Thread.getCurrentId(),
                .arena = .init(std.testing.allocator),
            };
            defer shard.arena.?.deinit();
            shard.frame.coroutine.shard = &shard;
            const W = Pool.Work(Warmup);
            const work = try shard.arena.?.allocator().create(W);
            try shard.pool.init(std.testing.allocator, &loop, 1);
            shard.pool_live = true;
            var started: std.Io.Event = .unset;
            var release: std.Io.Event = .unset;
            var blocker: co.Frame(W) = .{ .coroutine = .init(&shard.pool, .{
                .cpus = &.{},
                .started = &started,
                .release = &release,
            }) };
            if (queued) {
                loop.executor.spawn(&blocker.task);
                loop.executor.run();
                started.waitUncancelable(std.testing.io);
            }
            work.* = .init(&shard.pool, .{
                .cpus = &.{},
                .started = if (queued) null else &started,
                .release = if (queued) null else &release,
            });
            shard.frame.coroutine.work = work;
            loop.executor.spawn(&shard.frame.task);
            loop.executor.run();
            started.waitUncancelable(std.testing.io);
            defer {
                release.set(std.testing.io);
                loop.run() catch |err| std.debug.panic("warmup drain failed: {s}", .{@errorName(err)});
                if (queued) blocker.coroutine.deinit();
            }
            shard.frame.cancel();
            shard.frame.cancel();
            loop.executor.run();
            try std.testing.expectEqual(1, shard.frame.task.outstanding_io);
            try std.testing.expectEqual(if (queued) @as(usize, 2) else 1, shard.pool.outstanding);
            try std.testing.expect(shard.frame.result == null and shard.pool_live and shard.frame.coroutine.work != null);

            release.set(std.testing.io);
            try loop.run();
            try std.testing.expect(shard.frame.result != null and !shard.pool_live and shard.frame.coroutine.work == null);
            try std.testing.expectEqual(0, shard.frame.task.outstanding_io);
            try std.testing.expectEqual(0, shard.accepted);
            try std.testing.expectEqual(null, shard.first_poll);
            try std.testing.expectEqual(shard.thread_id, shard.worker_completed_on.?);
            if (queued) {
                try std.testing.expectEqual(error.OperationCanceled, shard.worker_error.?);
                try std.testing.expectEqual(null, shard.worker_thread);
                try std.testing.expect(try blocker.getResult() != shard.thread_id);
            } else {
                try std.testing.expectEqual(null, shard.worker_error);
                try std.testing.expect(shard.worker_thread.? != shard.thread_id);
            }
            try std.testing.expect(!loop.control.hasWork());
        }
    }
}

test "shards reject invalid admission bounds" {
    for ([_]Config{
        .{ .shards = 0 },
        .{ .shards = max_shards + 1 },
        .{ .requests = 0 },
        .{ .requests = 10_000_001 },
        .{ .shards = max_shards, .requests = 10_000_000 },
    }) |config| {
        try std.testing.expectError(error.InvalidArguments, exercise(std.testing.allocator, std.testing.io, config));
    }
}

test "shards placement spreads across permitted memory nodes and uses cores before siblings" {
    const cpus = [_]numa.Topology.Cpu{
        .{ .id = 2, .node = 1, .core = 2 },
        .{ .id = 3, .node = 1, .core = 2 },
        .{ .id = 8, .node = 1, .core = 8 },
        .{ .id = 16, .node = 7, .core = 16 },
        .{ .id = 17, .node = 7, .core = 16 },
        .{ .id = 24, .node = 7, .core = 24 },
        .{ .id = 32, .node = 9, .core = 32 },
    };
    const nodes = [_]numa.Topology.Node{
        .{ .id = 1, .memory = true, .distances = &.{} },
        .{ .id = 4, .memory = true, .distances = &.{} },
        .{ .id = 7, .memory = true, .distances = &.{} },
        .{ .id = 9, .memory = false, .distances = &.{} },
    };
    var topology: numa.Topology = .{ .cpus = &cpus, .nodes = &nodes, .arena = .init(std.testing.allocator) };
    defer topology.deinit();
    const selected = try numa.plan(std.testing.allocator, &topology, .{ .count = 6, .memory = .bind });
    defer std.testing.allocator.free(selected);
    for (selected, [_]usize{ 2, 16, 8, 24, 3, 17 }) |placement, id| {
        try std.testing.expectEqual(id, placement.cpu.?);
        try std.testing.expectEqual(topology.cpu(id).?.node, placement.memory.?.bind);
    }
    try std.testing.expectError(error.NotEnoughCpus, numa.plan(std.testing.allocator, &topology, .{ .count = 7, .memory = .bind }));
    topology.cpus = cpus[0..5];
    const restricted = try numa.plan(std.testing.allocator, &topology, .{ .count = 5, .memory = .bind });
    defer std.testing.allocator.free(restricted);
    for (restricted, [_]usize{ 2, 16, 8, 3, 17 }) |placement, id|
        try std.testing.expectEqual(id, placement.cpu.?);
}

test "shards setup allocation failures release only initialized states" {
    const Run = struct {
        fn run(allocator: std.mem.Allocator) !void {
            _ = try exercise(allocator, std.testing.io, .{ .shards = 2, .requests = 1 });
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Run.run, .{});
}

test "shards count accepted posts by synthetic node ownership on any host" {
    var states: [3]Shard = undefined;
    var shards = [_]*Shard{ &states[0], &states[1], &states[2] };
    var coordinator: Coordinator = .{
        .config = .{ .shards = shards.len, .requests = 64, .numa = true },
        .std_io = std.testing.io,
        .shards = &shards,
    };
    for (&states, 0..) |*shard, i| {
        shard.* = .{ .coordinator = &coordinator, .index = i, .node = if (i == 1) 7 else 1 };
        shard.frame.coroutine.shard = shard;
        shard.frame.coroutine.request.source = shard;
    }
    try coordinator.group.init(std.testing.allocator, .{ .count = shards.len, .loop = .{ .entries = 8 } }, &coordinator, Coordinator.entry);
    for (shards) |shard| shard.done.waitUncancelable(std.testing.io);
    coordinator.group.shutdown();
    coordinator.group.deinit();

    var cross: usize = 0;
    for (shards, 0..) |shard, source| {
        var local: usize = 0;
        for (0..64) |sequence| {
            const destination = route(@intCast(source * 64 + sequence), shards.len);
            local += @intFromBool(shard.node.? == shards[destination].node.?);
        }
        try drained(shard);
        try std.testing.expect(!shard.bad and shard.frame.result != null);
        try std.testing.expectEqual(64, shard.accepted);
        try std.testing.expectEqual(64, shard.frame.coroutine.consumed);
        try std.testing.expectEqual(local, shard.local_requests);
        try std.testing.expectEqual(64 - local, shard.cross_node_requests);
        cross += shard.cross_node_requests;
    }
    try std.testing.expect(cross > 0);
}

fn numaTestError(err: anyerror) anyerror {
    return switch (err) {
        error.Unsupported, error.PermissionDenied, error.FixedBuffersUnsupported, error.IowqAffinityUnsupported => error.SkipZigTest,
        else => err,
    };
}

fn testNuma(mode: Mode) !void {
    var topology = numa.Topology.discover(std.testing.allocator, std.testing.io) catch |err| return numaTestError(err);
    defer topology.deinit();
    var selected: []Placement = undefined;
    var count: usize = 3;
    while (count != 0) : (count -= 1) {
        selected = numa.plan(std.testing.allocator, &topology, .{ .count = count, .memory = .bind }) catch |err| switch (err) {
            error.NotEnoughCpus => continue,
            else => return err,
        };
        break;
    }
    if (count == 0) return error.SkipZigTest;
    defer std.testing.allocator.free(selected);

    for ([_]Scenario{ .normal, .cancel, .shutdown, .closed, .late_cancel, .shutdown_all }) |scenario| {
        const requests: usize = if (scenario == .normal) 64 else 13;
        const result = exercise(std.testing.allocator, std.testing.io, .{
            .numa = true,
            .shards = count,
            .requests = requests,
            .mode = mode,
            .scenario = scenario,
        }) catch |err| return numaTestError(err);
        for (selected, result.placement[0..count]) |expected, placement| {
            try std.testing.expectEqualDeep(expected, placement);
        }
        try std.testing.expectEqual(count, result.workers_verified);
        try std.testing.expectEqual(result.requests, result.succeeded + result.canceled);
        try std.testing.expectEqual(result.requests, result.local_requests + result.cross_node_requests);
        if (scenario == .shutdown_all) {
            try std.testing.expect(result.requests >= 1 and result.requests <= (count - 1) * requests + 1);
            try std.testing.expect(result.canceled >= 1);
            continue;
        }

        var local: usize = 0;
        var cross: usize = 0;
        for (selected, 0..) |placement, source| {
            const accepted = if (scenario == .normal or source != 0) requests else @intFromBool(scenario != .closed);
            for (0..accepted) |sequence| {
                const destination = route(@intCast(source * requests + sequence), count);
                if (placement.memory.?.bind == selected[destination].memory.?.bind) local += 1 else cross += 1;
            }
        }
        try std.testing.expectEqual(local, result.local_requests);
        try std.testing.expectEqual(cross, result.cross_node_requests);
        try std.testing.expectEqual(local + cross, result.requests);
        try std.testing.expectEqual(@intFromBool(scenario == .closed), result.rejected);
        if (scenario == .normal) {
            try std.testing.expectEqual(count * requests, result.succeeded);
            try std.testing.expectEqual(count, result.destinations);
            if (count >= 2 and selected[0].memory.?.bind != selected[1].memory.?.bind)
                try std.testing.expect(result.cross_node_requests > 0);
        }
    }

    for ([_]bool{ true, false }) |prepare_failure| {
        const result = exercise(std.testing.allocator, std.testing.io, .{
            .numa = true,
            .shards = count,
            .requests = 1,
            .mode = mode,
            .fail_prepare_after = if (prepare_failure) count - 1 else null,
            .fail_after = if (prepare_failure) null else count - 1,
        });
        if (result) |_| {
            return error.TestExpectedError;
        } else |err| {
            if (err != (if (prepare_failure) error.ThreadPrepareFailed else error.ThreadStartFailed))
                return numaTestError(err);
        }
    }
    for ([_]Scenario{ .normal, .cancel }) |scenario| {
        _ = exercise(std.testing.allocator, std.testing.io, .{
            .numa = true,
            .shards = count,
            .requests = 1,
            .mode = mode,
            .scenario = scenario,
            .fail_work_after = count - 1,
        }) catch |err| {
            if (err != error.WorkerWarmupFailed) return numaTestError(err);
            continue;
        };
        return error.TestExpectedError;
    }
}

test "shards NUMA automatic backend preserves ownership locality cancellation and rollback" {
    try testNuma(.auto);
}

test "shards NUMA epoll preserves ownership locality cancellation and rollback" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    try testNuma(.epoll);
}

test "shards NUMA prepare lends separate descriptors and buffers until cleanup" {
    var topology = numa.Topology.discover(std.testing.allocator, std.testing.io) catch |err| return numaTestError(err);
    defer topology.deinit();
    const cpu = for (topology.cpus) |cpu| {
        for (topology.nodes) |node| {
            if (node.id == cpu.node and node.memory) break;
        } else continue;
        break cpu;
    } else return error.SkipZigTest;
    var shards: [2]Shard = undefined;
    var pointers = [_]*Shard{ &shards[0], &shards[1] };
    var coordinator: Coordinator = .{
        .config = .{ .numa = true },
        .std_io = std.testing.io,
        .shards = &pointers,
        .topology = &topology,
    };
    for (&shards, 0..) |*shard, i| shard.* = .{ .coordinator = &coordinator, .index = i, .cpu = cpu.id, .node = cpu.node };
    const first = coordinator.prepare(0, .{ .entries = 16 }) catch |err| return numaTestError(err);
    defer if (!shards[0].cleaned) coordinator.cleanup(0);
    var single_cpu: numa.Topology = .{ .cpus = &.{cpu}, .nodes = topology.nodes, .arena = .init(std.testing.allocator) };
    defer single_cpu.deinit();
    coordinator.topology = &single_cpu;
    const second = coordinator.prepare(1, .{ .entries = 32 }) catch |err| return numaTestError(err);
    defer if (!shards[1].cleaned) coordinator.cleanup(1);
    coordinator.topology = &topology;
    try std.testing.expectEqual(16, first.entries);
    try std.testing.expectEqual(32, second.entries);
    for ([_]io.LoopOptions{ first, second }, &shards) |options, *shard| {
        try std.testing.expectEqual(1, options.buffers.len);
        try std.testing.expectEqual(@as([*]const []u8, &shard.buffers), options.buffers.ptr);
        try std.testing.expect(shard.pages.options.prefault);
        try std.testing.expectEqual(cpu.node, shard.pages.policy.bind);
        const page: [*]const u8 = @ptrFromInt(std.mem.alignBackward(usize, @intFromPtr(options.buffers[0].ptr), std.heap.pageSize()));
        const actual_node = numa.pageNode(page) catch |err| return numaTestError(err);
        try std.testing.expectEqual(cpu.node, actual_node);
        try std.testing.expectEqual(@as(u8, 0), options.buffers[0][0]);
        try std.testing.expectEqualSlices(usize, shard.worker_cpus, options.iowq_cpus);
        try std.testing.expect(options.iowq_cpus.len != 0);
        for (options.iowq_cpus) |id| {
            try std.testing.expectEqual(cpu.node, topology.cpu(id).?.node);
            if (shard.cpu_storage.len > 1) try std.testing.expect(id != cpu.id);
        }
        if (shard.cpu_storage.len == 1) try std.testing.expectEqualSlices(usize, &.{cpu.id}, options.iowq_cpus);
    }
    try std.testing.expect(first.buffers.ptr != second.buffers.ptr);
    try std.testing.expect(first.buffers[0].ptr != second.buffers[0].ptr);
    for (0..2) |i| {
        coordinator.cleanup(i);
        try drained(&shards[i]);
        try std.testing.expect(shards[i].cleaned);
    }
}
