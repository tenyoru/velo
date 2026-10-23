const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;

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
    scenario: Scenario = .normal,
    fail_after: ?usize = null,
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

const Sender = struct {
    shard: *Shard = undefined,
    request: Request = .{},
    consumed: usize = 0,
    succeeded: usize = 0,
    canceled: usize = 0,
    rejected: usize = 0,

    pub fn cancel(self: *@This()) void {
        self.request.cancel();
    }

    fn done(self: *@This()) co.Poll(void) {
        self.shard.done.set(self.shard.coordinator.std_io);
        return .{ .ready = {} };
    }

    pub fn poll(self: *@This(), task: *co.Task) co.Poll(void) {
        const config = self.shard.coordinator.config;
        if (self.shard.first_poll == null)
            self.shard.first_poll = std.Io.Clock.awake.now(self.shard.coordinator.std_io);
        if (!self.request.active) {
            self.request.id = self.shard.index * config.requests + self.consumed;
            self.request.key = self.request.id;
            self.request.payload = self.request.id ^ 0xa0761d6478bd642f;
            self.request.destination = &self.shard.coordinator.shards[route(self.request.key, config.shards)];
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
    frame: co.Frame(Sender) = .{ .coroutine = .{} },
    done: std.Io.Event = .unset,
    thread_id: std.Thread.Id = undefined,
    first_poll: ?std.Io.Timestamp = null,
    accepted: usize = 0,
    received: usize = 0,
    responses: usize = 0,
    round_trip_ns: u128 = 0,
    last_ids: [max_shards]?u64 = @splat(null),
    held: ?*Request = null,
    borrow_held: bool = false,
    bad: bool = false,
};

const Coordinator = struct {
    config: Config,
    std_io: std.Io,
    shards: []Shard,
    group: velo.workers.Group = undefined,
    held: std.Io.Event = .unset,
    cancel_parked: std.Io.Event = .unset,
    cancel_post: io.Control.Post = .{ .callback = cancelSource },
    resume_post: io.Control.Post = .{ .callback = resumeReply },

    fn entry(self: *@This(), loop: *io.Loop, index: usize) error{ThreadStartFailed}!void {
        const shard = &self.shards[index];
        shard.loop = loop;
        shard.thread_id = std.Thread.getCurrentId();
        if (self.config.fail_after == index) return error.ThreadStartFailed;
        if (self.config.scenario == .closed and index == 0) loop.shutdown();
        loop.executor.spawn(&shard.frame.task);
    }

    fn cancelSource(post: *io.Control.Post) void {
        const self: *@This() = @fieldParentPtr("cancel_post", post);
        const source = &self.shards[0];
        if (self.config.scenario == .shutdown_all) {
            self.group.shutdown();
        } else if (self.config.scenario == .shutdown) source.loop.shutdown() else source.frame.cancel();
    }

    fn resumeReply(post: *io.Control.Post) void {
        const self: *@This() = @fieldParentPtr("resume_post", post);
        const destination = &self.shards[route(0, self.shards.len)];
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
};

// Loops are gone by now; their own deinit asserts they drained.
fn drained(shard: *const Shard) !void {
    if (shard.held != null or shard.frame.task.outstanding_io != 0) return error.LoopNotDrained;
}

fn exercise(allocator: std.mem.Allocator, std_io: std.Io, config: Config) !Result {
    if (config.shards == 0 or config.shards > max_shards or config.requests == 0 or
        config.requests > 10_000_000 or config.shards * config.requests > 100_000_000 or
        (config.mode == .epoll and builtin.os.tag != .linux)) return error.InvalidArguments;

    const shards = try allocator.alloc(Shard, config.shards);
    defer allocator.free(shards);
    var coordinator: Coordinator = .{ .config = config, .std_io = std_io, .shards = shards };
    for (shards, 0..) |*shard, index| {
        shard.* = .{ .coordinator = &coordinator, .index = index };
        shard.frame.coroutine.shard = shard;
        shard.frame.coroutine.request.source = shard;
    }

    // The group keeps every destination alive until every producer has consumed its last reply.
    coordinator.group.init(allocator, .{
        .count = config.shards,
        .loop = .{ .entries = 8 },
        .backend = if (builtin.os.tag == .linux and config.mode == .epoll) .epoll else .auto,
    }, &coordinator, Coordinator.entry) catch |err| {
        for (shards) |*shard| try drained(shard);
        return err;
    };

    if (holdsReply(config.scenario)) {
        coordinator.held.waitUncancelable(std_io);
        shards[0].loop.post(&coordinator.cancel_post);
        coordinator.cancel_parked.waitUncancelable(std_io);
        shards[route(0, shards.len)].loop.post(&coordinator.resume_post);
    }
    for (shards) |*shard| shard.done.waitUncancelable(std_io);
    const finished = std.Io.Clock.awake.now(std_io);
    coordinator.group.shutdown();
    coordinator.group.deinit();

    // Thread spawn and loop setup stay out of the throughput window.
    var started = finished;
    for (shards) |*shard| {
        const first = shard.first_poll.?;
        if (first.nanoseconds < started.nanoseconds) started = first;
    }
    var result: Result = .{
        .elapsed_ns = @intCast(started.durationTo(finished).nanoseconds),
        .drain_ns = @intCast(finished.durationTo(std.Io.Clock.awake.now(std_io)).nanoseconds),
    };
    var received: usize = 0;
    var round_trip_ns: u128 = 0;
    for (shards) |*shard| {
        try drained(shard);
        const sender = &shard.frame.coroutine;
        if (shard.bad or shard.frame.result == null or shard.accepted != sender.consumed or
            shard.responses != sender.consumed or sender.succeeded + sender.canceled != sender.consumed)
            return error.IncorrectResults;
        if (config.scenario == .normal and
            (sender.succeeded != config.requests or sender.rejected != 0)) return error.IncorrectResults;
        result.requests += sender.consumed;
        result.succeeded += sender.succeeded;
        result.canceled += sender.canceled;
        result.rejected += sender.rejected;
        result.destinations += @intFromBool(shard.received != 0);
        received += shard.received;
        round_trip_ns += shard.round_trip_ns;
    }
    if (received != result.requests) return error.IncorrectResults;
    result.mean_round_trip_ns = @intCast(round_trip_ns / @max(result.requests, 1));
    if (holdsReply(config.scenario)) {
        const source = &shards[0];
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
    if (args.len != 1 and args.len != 3 and args.len != 4) {
        std.debug.print("Usage: {s} [shards requests-per-shard [auto|epoll]]\n", .{args[0]});
        return error.InvalidArguments;
    }
    if (args.len >= 3) {
        config.shards = std.fmt.parseInt(usize, args[1], 10) catch return error.InvalidArguments;
        config.requests = std.fmt.parseInt(usize, args[2], 10) catch return error.InvalidArguments;
    }
    if (args.len == 4) config.mode = std.meta.stringToEnum(Mode, args[3]) orelse return error.InvalidArguments;
    const result = try exercise(init.gpa, init.io, config);
    const rate = @as(f64, @floatFromInt(result.requests)) * 1_000_000_000 /
        @as(f64, @floatFromInt(@max(result.elapsed_ns, 1)));
    std.debug.print("shards={d} requests={d} elapsed_ns={d} requests_per_second={d:.0} mean_round_trip_ns={d} drain_ns={d}\n", .{
        config.shards, result.requests, result.elapsed_ns, rate, result.mean_round_trip_ns, result.drain_ns,
    });
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
