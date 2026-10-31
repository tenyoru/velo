//! Cross-loop messaging: what one post costs, in time and CPU, by how busy its target is.
//! `ping-pong`: closed loop, the target always parked. `busy`: senders outrun one receiver.
//! `paced`: one sender thread at a fixed rate; only receiver CPU and latency are meaningful.

const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const io = velo.event;
const workers = velo.workers;
const numa = workers.numa;
const Post = io.Control.Post;

const Backend = enum { auto, uring, epoll };
const Workload = enum { @"ping-pong", busy, paced };
const window = 16;
const paced_slots = 4096;
const usage_text = "usage: crossloop <auto|uring|epoll> <ping-pong|busy|paced> <messages> [loops=N] [rate=N] [work=N] [spin=NS] [pinned]\n";

const Config = struct {
    backend: Backend,
    workload: Workload,
    messages: usize,
    /// ping-pong: loops in pairs; busy: senders plus one receiver; paced: always one.
    loops: usize = 2,
    /// paced messages per second.
    rate: usize = 10_000,
    /// busy receiver hash rounds per message.
    work: usize = 256,
    /// `LoopOptions.idle_spin_ns`.
    spin: u32 = 0,
    pinned: bool = false,

    fn parse(args: []const []const u8) !Config {
        if (args.len < 3) return error.Usage;
        const backend = std.meta.stringToEnum(Backend, args[0]) orelse return error.InvalidBackend;
        if (builtin.os.tag != .linux and backend != .auto) return error.UnsupportedBackend;
        const workload = std.meta.stringToEnum(Workload, args[1]) orelse return error.InvalidWorkload;
        var config: Config = .{
            .backend = backend,
            .workload = workload,
            .messages = std.fmt.parseInt(usize, args[2], 10) catch return error.InvalidMessages,
        };
        if (config.messages == 0) return error.InvalidMessages;
        for (args[3..]) |arg| {
            if (std.mem.eql(u8, arg, "pinned")) {
                config.pinned = true;
                continue;
            }
            const split = std.mem.indexOfScalar(u8, arg, '=') orelse return error.Usage;
            const value = std.fmt.parseInt(usize, arg[split + 1 ..], 10) catch return error.InvalidOption;
            const key = arg[0..split];
            if (std.mem.eql(u8, key, "loops")) {
                config.loops = value;
            } else if (std.mem.eql(u8, key, "rate")) {
                config.rate = value;
            } else if (std.mem.eql(u8, key, "work")) {
                config.work = value;
            } else if (std.mem.eql(u8, key, "spin")) {
                config.spin = std.math.cast(u32, value) orelse return error.InvalidOption;
            } else return error.Usage;
        }
        switch (workload) {
            .@"ping-pong" => if (config.loops == 0 or config.loops % 2 != 0) return error.InvalidOption,
            .busy => if (config.loops < 2) return error.InvalidOption,
            .paced => config.loops = 1,
        }
        if (config.rate == 0 or config.rate > std.time.ns_per_s) return error.InvalidOption;
        return config;
    }
};

const Report = struct {
    backend: []const u8,
    workload: []const u8,
    messages: usize,
    loops: usize,
    pinned: bool,
    spin_ns: u32,
    rate: ?usize,
    elapsed_ns: u64,
    messages_per_second: u64,
    /// Whole process; includes a paced sender's busy wait.
    user_ns: u64,
    system_ns: u64,
    cpu_ns_per_message: u64,
    voluntary_switches: u64,
    /// Receiver thread only, from its first message to its last; paced runs alone.
    receiver_cpu_ns: ?u64,
    receiver_cpu_ns_per_message: ?u64,
    latency_p50_ns: ?u64,
    latency_p99_ns: ?u64,
    latency_max_ns: ?u64,
    turns: u64,
    polls: u64,
    parks: u64,
    posts: u64,
    wakes: u64,
};

fn clock() u64 {
    const time = io.now() catch @panic("clock failed");
    return @intCast(@as(i128, time.sec) * std.time.ns_per_s + time.nsec);
}

const Usage = struct {
    user_ns: u64 = 0,
    system_ns: u64 = 0,
    voluntary: u64 = 0,

    fn read() Usage {
        if (builtin.os.tag != .linux) return .{};
        var raw: std.os.linux.rusage = undefined;
        if (std.os.linux.getrusage(std.os.linux.rusage.SELF, &raw) != 0) @panic("getrusage failed");
        return .{
            .user_ns = nanoseconds(raw.utime),
            .system_ns = nanoseconds(raw.stime),
            .voluntary = @intCast(raw.nvcsw),
        };
    }

    fn nanoseconds(time: std.os.linux.timeval) u64 {
        return @intCast(@as(i128, time.sec) * std.time.ns_per_s + @as(i128, time.usec) * std.time.ns_per_us);
    }
};

fn threadCpu() u64 {
    if (builtin.os.tag != .linux) return 0;
    var time: std.os.linux.timespec = undefined;
    if (std.os.linux.clock_gettime(.THREAD_CPUTIME_ID, &time) != 0) @panic("thread clock failed");
    return @intCast(@as(i128, time.sec) * std.time.ns_per_s + time.nsec);
}

const Shard = struct {
    bench: *Bench,
    loop: *io.Loop = undefined,
    stats: @TypeOf(@as(*const io.Loop, undefined).stats()) = .{},
    snapshot: Post = .{ .callback = takeSnapshot },

    fn takeSnapshot(post: *Post) void {
        const self: *Shard = @fieldParentPtr("snapshot", post);
        self.stats = self.loop.stats();
        if (self.bench.pending_snapshots.fetchSub(1, .acq_rel) == 1) self.bench.snapshots.set(self.bench.std_io);
    }
};

const Bench = struct {
    config: Config,
    std_io: std.Io,
    shards: []Shard,
    finished: std.Io.Event = .unset,
    snapshots: std.Io.Event = .unset,
    pending_snapshots: std.atomic.Value(usize) = .init(0),
    active: std.atomic.Value(usize) = .init(0),

    fn entry(bench: *Bench, loop: *io.Loop, index: usize) !void {
        bench.shards[index].loop = loop;
    }

    fn done(bench: *Bench) void {
        if (bench.active.fetchSub(1, .acq_rel) == 1) bench.finished.set(bench.std_io);
    }
};

const Pair = struct {
    bench: *Bench,
    left: *io.Loop,
    right: *io.Loop,
    remaining: usize,
    ping: Post = .{ .callback = onPing },
    pong: Post = .{ .callback = onPong },

    fn onPing(post: *Post) void {
        const self: *Pair = @fieldParentPtr("ping", post);
        self.left.post(&self.pong);
    }

    fn onPong(post: *Post) void {
        const self: *Pair = @fieldParentPtr("pong", post);
        self.remaining -= 1;
        if (self.remaining == 0) return self.bench.done();
        self.right.post(&self.ping);
    }
};

const Sender = struct {
    bench: *Bench,
    loop: *io.Loop,
    receiver: *io.Loop,
    work: usize,
    unsent: usize,
    unanswered: usize,
    begin: Post = .{ .callback = start },
    messages: [window]Message = undefined,

    const Message = struct {
        sender: *Sender,
        request: Post = .{ .callback = onRequest },
        reply: Post = .{ .callback = onReply },

        fn onRequest(post: *Post) void {
            const self: *Message = @fieldParentPtr("request", post);
            var hash: u64 = 0x9e3779b97f4a7c15;
            for (0..self.sender.work) |_| hash = (hash ^ (hash >> 7)) *% 0xff51afd7ed558ccd;
            std.mem.doNotOptimizeAway(hash);
            self.sender.loop.post(&self.reply);
        }

        fn onReply(post: *Post) void {
            const self: *Message = @fieldParentPtr("reply", post);
            self.sender.settle(self);
        }
    };

    fn start(post: *Post) void {
        const sender: *Sender = @fieldParentPtr("begin", post);
        for (&sender.messages) |*message| {
            message.* = .{ .sender = sender };
            if (sender.unsent == 0) continue;
            sender.unsent -= 1;
            sender.receiver.post(&message.request);
        }
    }

    fn settle(sender: *Sender, message: *Message) void {
        sender.unanswered -= 1;
        if (sender.unanswered == 0) return sender.bench.done();
        if (sender.unsent == 0) return;
        sender.unsent -= 1;
        sender.receiver.post(&message.request);
    }
};

const Paced = struct {
    bench: *Bench,
    latencies: []u64,
    received: usize = 0,
    cpu_start: u64 = 0,
    cpu_end: u64 = 0,

    const Slot = struct {
        paced: *Paced,
        post: Post = .{ .callback = onSlot },
        // A queued flag clears before its callback reads `sent`, so reuse needs its own gate.
        free: std.atomic.Value(bool) = .init(true),
        sent: u64 = 0,

        fn onSlot(post: *Post) void {
            const self: *Slot = @fieldParentPtr("post", post);
            const paced = self.paced;
            const latency = clock() - self.sent;
            self.free.store(true, .release);

            if (paced.received == 0) paced.cpu_start = threadCpu();
            paced.latencies[paced.received] = latency;
            paced.received += 1;
            if (paced.received != paced.latencies.len) return;
            paced.cpu_end = threadCpu();
            paced.bench.done();
        }
    };

    fn send(paced: *Paced, slots: []Slot, loop: *io.Loop, rate: usize) void {
        const period: u64 = std.time.ns_per_s / rate;
        var next = clock();
        for (0..paced.latencies.len) |index| {
            next += period;
            while (clock() < next) std.atomic.spinLoopHint();

            const slot = &slots[index % slots.len];
            while (!slot.free.load(.acquire)) std.atomic.spinLoopHint();
            slot.free.store(false, .monotonic);
            slot.sent = clock();
            loop.post(&slot.post);
        }
    }
};

fn percentile(sorted: []const u64, part: usize) u64 {
    return sorted[(sorted.len - 1) * part / 100];
}

fn run(allocator: std.mem.Allocator, std_io: std.Io, config: Config) !Report {
    const shards = try allocator.alloc(Shard, config.loops);
    defer allocator.free(shards);
    const pairs = try allocator.alloc(Pair, if (config.workload == .@"ping-pong") config.loops / 2 else 0);
    defer allocator.free(pairs);
    const senders = try allocator.alloc(Sender, if (config.workload == .busy) config.loops - 1 else 0);
    defer allocator.free(senders);
    const is_paced = config.workload == .paced;
    const latencies = try allocator.alloc(u64, if (is_paced) config.messages else 0);
    defer allocator.free(latencies);
    const slots = try allocator.alloc(Paced.Slot, if (is_paced) paced_slots else 0);
    defer allocator.free(slots);

    var bench: Bench = .{ .config = config, .std_io = std_io, .shards = shards };
    for (shards) |*shard| shard.* = .{ .bench = &bench };
    var paced: Paced = .{ .bench = &bench, .latencies = latencies };
    for (slots) |*slot| slot.* = .{ .paced = &paced };

    var placement: []numa.Placement = &.{};
    defer allocator.free(placement);
    var topology: ?numa.Topology = null;
    defer if (topology) |*known| known.deinit();
    if (config.pinned) {
        topology = try numa.Topology.discover(allocator, std_io);
        placement = try numa.plan(allocator, &topology.?, .{ .count = config.loops });
    }

    var group: workers.Group = undefined;
    try group.init(allocator, .{
        .count = config.loops,
        .loop = .{ .idle_spin_ns = config.spin },
        .backend = switch (config.backend) {
            .auto => .auto,
            .uring => if (builtin.os.tag == .linux) .uring else unreachable,
            .epoll => if (builtin.os.tag == .linux) .epoll else unreachable,
        },
        .name = "crossloop",
        .placement = placement,
    }, &bench, Bench.entry);

    const before = Usage.read();
    const started = clock();
    switch (config.workload) {
        .@"ping-pong" => {
            bench.active.store(pairs.len, .release);
            for (pairs, 0..) |*pair, index| pair.* = .{
                .bench = &bench,
                .left = shards[index * 2].loop,
                .right = shards[index * 2 + 1].loop,
                .remaining = config.messages,
            };
            for (pairs) |*pair| pair.right.post(&pair.ping);
        },
        .busy => {
            bench.active.store(senders.len, .release);
            for (senders, shards[1..]) |*sender, *shard| {
                sender.* = .{
                    .bench = &bench,
                    .loop = shard.loop,
                    .receiver = shards[0].loop,
                    .work = config.work,
                    .unsent = config.messages,
                    .unanswered = config.messages,
                };
                sender.loop.post(&sender.begin);
            }
        },
        .paced => {
            bench.active.store(1, .release);
            paced.send(slots, shards[0].loop, config.rate);
        },
    }
    bench.finished.waitUncancelable(std_io);
    const elapsed = clock() - started;
    const after = Usage.read();

    bench.pending_snapshots.store(shards.len, .release);
    for (shards) |*shard| shard.loop.post(&shard.snapshot);
    bench.snapshots.waitUncancelable(std_io);
    group.deinit();

    var totals: @TypeOf(shards[0].stats) = .{};
    for (shards) |*shard| {
        totals.turns += shard.stats.turns;
        totals.polls += shard.stats.polls;
        totals.parks += shard.stats.parks;
        totals.posts += shard.stats.posts;
        totals.wakes += shard.stats.wakes;
    }

    const total_messages: u64 = switch (config.workload) {
        .@"ping-pong" => config.messages * config.loops / 2,
        .busy => config.messages * (config.loops - 1),
        .paced => config.messages,
    };
    const user = after.user_ns - before.user_ns;
    const system = after.system_ns - before.system_ns;
    const receiver_cpu = paced.cpu_end - paced.cpu_start;
    if (is_paced) std.mem.sort(u64, latencies, {}, std.sort.asc(u64));

    return .{
        .backend = @tagName(config.backend),
        .workload = @tagName(config.workload),
        .messages = config.messages,
        .loops = config.loops,
        .pinned = config.pinned,
        .spin_ns = config.spin,
        .rate = if (is_paced) config.rate else null,
        .elapsed_ns = elapsed,
        .messages_per_second = @intCast(@as(u128, total_messages) * std.time.ns_per_s / elapsed),
        .user_ns = user,
        .system_ns = system,
        .cpu_ns_per_message = (user + system) / total_messages,
        .voluntary_switches = after.voluntary - before.voluntary,
        .receiver_cpu_ns = if (is_paced) receiver_cpu else null,
        .receiver_cpu_ns_per_message = if (is_paced) receiver_cpu / total_messages else null,
        .latency_p50_ns = if (is_paced) percentile(latencies, 50) else null,
        .latency_p99_ns = if (is_paced) percentile(latencies, 99) else null,
        .latency_max_ns = if (is_paced) latencies[latencies.len - 1] else null,
        .turns = totals.turns,
        .polls = totals.polls,
        .parks = totals.parks,
        .posts = totals.posts,
        .wakes = totals.wakes,
    };
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const config = Config.parse(args[1..]) catch |err| {
        std.debug.print(usage_text, .{});
        return err;
    };
    const report = try run(init.gpa, init.io, config);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try std.json.Stringify.value(report, .{}, &stdout.interface);
    try stdout.interface.writeByte('\n');
    try stdout.interface.flush();
}

test "every workload delivers each message once" {
    const modes = if (builtin.os.tag == .linux) [_]Backend{ .auto, .epoll } else [_]Backend{.auto};
    for (modes) |backend| {
        const ping = try run(std.testing.allocator, std.testing.io, .{
            .backend = backend,
            .workload = .@"ping-pong",
            .messages = 64,
        });
        try std.testing.expect(ping.posts >= 128);

        const busy = try run(std.testing.allocator, std.testing.io, .{
            .backend = backend,
            .workload = .busy,
            .messages = 64,
            .loops = 3,
            .work = 1,
        });
        try std.testing.expect(busy.posts >= 256);

        const paced = try run(std.testing.allocator, std.testing.io, .{
            .backend = backend,
            .workload = .paced,
            .messages = 64,
            .rate = 100_000,
            .loops = 1,
        });
        try std.testing.expect(paced.posts >= 64);
        try std.testing.expect(paced.latency_p50_ns.? <= paced.latency_p99_ns.?);
    }
}

test "CLI rejects invalid arguments" {
    try std.testing.expectError(error.Usage, Config.parse(&.{}));
    try std.testing.expectError(error.InvalidBackend, Config.parse(&.{ "invalid", "busy", "4" }));
    try std.testing.expectError(error.InvalidWorkload, Config.parse(&.{ "auto", "invalid", "4" }));
    try std.testing.expectError(error.InvalidMessages, Config.parse(&.{ "auto", "busy", "0" }));
    try std.testing.expectError(error.InvalidOption, Config.parse(&.{ "auto", "ping-pong", "4", "loops=3" }));
    try std.testing.expectError(error.InvalidOption, Config.parse(&.{ "auto", "busy", "4", "loops=1" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "busy", "4", "nonsense" }));
    const parsed = try Config.parse(&.{ "auto", "busy", "4", "loops=5", "work=9", "spin=20000", "pinned" });
    try std.testing.expectEqual(5, parsed.loops);
    try std.testing.expectEqual(9, parsed.work);
    try std.testing.expectEqual(20_000, parsed.spin);
    try std.testing.expect(parsed.pinned);
}
