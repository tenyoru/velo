//! CPU queries with shard-owned accumulators: local execution versus explicit distribution.
const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const linux = std.os.linux;

const Distribution = enum { balanced, skewed };
const Policy = enum { local, spread };
const quantum = 1024;
const multiplier: u64 = 1664525;
const increment: u64 = 1013904223;

const Config = struct {
    distribution: Distribution,
    policy: Policy,
    loops: usize,
    jobs: usize,
    rounds: usize,

    fn parse(args: []const []const u8) !Config {
        if (args.len != 5) return error.Usage;
        const c: Config = .{
            .distribution = std.meta.stringToEnum(Distribution, args[0]) orelse return error.Usage,
            .policy = std.meta.stringToEnum(Policy, args[1]) orelse return error.Usage,
            .loops = try std.fmt.parseInt(usize, args[2], 10),
            .jobs = try std.fmt.parseInt(usize, args[3], 10),
            .rounds = try std.fmt.parseInt(usize, args[4], 10),
        };
        if (c.loops == 0 or c.loops > 64 or c.jobs == 0 or c.jobs > 100_000 or
            c.rounds == 0 or c.rounds > 1_000_000 or
            @as(u64, c.jobs) * c.rounds > 2_000_000_000) return error.Usage;
        return c;
    }

    fn owner(c: Config, id: usize) usize {
        if (c.loops == 1) return 0;
        if (c.distribution == .balanced) return id % c.loops;
        const hot = (c.jobs * 9 + 9) / 10;
        return if (id < hot) 0 else 1 + (id - hot) % (c.loops - 1);
    }

    fn executor(c: Config, id: usize) usize {
        return if (c.policy == .spread) id % c.loops else c.owner(id);
    }
};

fn clock(id: linux.clockid_t) u64 {
    var ts: linux.timespec = undefined;
    if (linux.errno(linux.clock_gettime(id, &ts)) != .SUCCESS) @panic("clock_gettime");
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn compute(value: u64, rounds: usize) u64 {
    var result = value;
    for (0..rounds) |_| result = result *% multiplier +% increment;
    return result;
}

const LoopReport = struct {
    cpu: usize,
    cpu_ns: u64,
    computed: usize,
    committed: usize,
    checksum: u64,
    polls: u64,
    posts: u64,
    parks: u64,
};

const Report = struct {
    config: Config,
    elapsed_ns: u64,
    process_cpu_ns: u64,
    completed: usize,
    checksum: u64,
    remote_commits: usize,
    completion_p50_ns: u64,
    completion_p99_ns: u64,
    shards: []LoopReport,
};

const Bench = struct {
    std_io: std.Io,
    shards: []Shard,
    remaining: std.atomic.Value(usize),
    armed: std.Io.Event = .unset,
    pending_arms: std.atomic.Value(usize),
    finished: std.Io.Event = .unset,
    snapshots: std.Io.Event = .unset,
    pending_snapshots: std.atomic.Value(usize),
    started: u64 = 0,
    ended: u64 = 0,

    fn entry(self: *Bench, loop: *io.Loop, index: usize) !void {
        const shard = &self.shards[index];
        shard.loop = loop;
        shard.thread_id = std.Thread.getCurrentId();
        const cpus = try velo.workers.numa.allowedCpus(std.heap.page_allocator);
        defer std.heap.page_allocator.free(cpus);
        if (cpus.len != 1 or cpus[0] != shard.cpu) return error.AffinityMismatch;
        std.mem.doNotOptimizeAway(compute(index + 1, 100_000));
    }
};

const Shard = struct {
    bench: *Bench,
    cpu: usize,
    loop: *io.Loop = undefined,
    thread_id: std.Thread.Id = undefined,
    first: ?*Job = null,
    last: ?*Job = null,
    arm: io.Control.Post = .{ .callback = armClock },
    start: io.Control.Post = .{ .callback = begin },
    snapshot: io.Control.Post = .{ .callback = takeSnapshot },
    cpu_started: u64 = 0,
    cpu_ns: u64 = 0,
    computed: usize = 0,
    committed: usize = 0,
    checksum: u64 = 0,
    wrong_thread: bool = false,
    stats: co.Executor.Stats = .{},

    fn armClock(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("arm", post);
        self.cpu_started = clock(.THREAD_CPUTIME_ID);
        if (self.bench.pending_arms.fetchSub(1, .acq_rel) == 1)
            self.bench.armed.set(self.bench.std_io);
    }

    fn begin(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("start", post);
        var next = self.first;
        while (next) |job| {
            self.loop.spawn(&job.frame);
            next = job.next;
        }
    }

    fn takeSnapshot(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("snapshot", post);
        self.cpu_ns = clock(.THREAD_CPUTIME_ID) - self.cpu_started;
        self.stats = self.loop.stats();
        if (self.bench.pending_snapshots.fetchSub(1, .acq_rel) == 1)
            self.bench.snapshots.set(self.bench.std_io);
    }
};

const Job = struct {
    owner: *Shard,
    executor: *Shard,
    frame: co.Frame(Compute) = undefined,
    next: ?*Job = null,
    commit: io.Control.Post = .{ .callback = apply },
    value: u64 = 0,
    completion_ns: u64 = 0,
    commits: usize = 0,

    fn apply(post: *io.Control.Post) void {
        const self: *Job = @fieldParentPtr("commit", post);
        self.finish();
    }

    fn finish(self: *Job) void {
        const owner = self.owner;
        owner.wrong_thread = owner.wrong_thread or std.Thread.getCurrentId() != owner.thread_id;
        owner.checksum +%= self.value;
        owner.committed += 1;
        self.commits += 1;
        const now = clock(.MONOTONIC);
        const bench = owner.bench;
        self.completion_ns = now - bench.started;
        if (bench.remaining.fetchSub(1, .acq_rel) == 1) {
            bench.ended = now;
            bench.finished.set(bench.std_io);
        }
    }
};

const Compute = struct {
    job: *Job,
    value: u64,
    remaining: usize,

    pub fn poll(self: *Compute, task: *co.Task) co.Poll(error{Canceled}!void) {
        if (task.cancel_requested) return .{ .ready = error.Canceled };
        const count = @min(quantum, self.remaining);
        self.value = compute(self.value, count);
        self.remaining -= count;
        if (self.remaining != 0) {
            task.wake();
            return .pending;
        }
        const job = self.job;
        job.executor.wrong_thread = job.executor.wrong_thread or std.Thread.getCurrentId() != job.executor.thread_id;
        job.executor.computed += 1;
        job.value = self.value;
        if (job.executor == job.owner) {
            job.finish();
        } else {
            // Job storage survives Group.deinit; the owner alone consumes the published value.
            job.owner.loop.post(&job.commit);
        }
        return .{ .ready = {} };
    }
};

fn run(allocator: std.mem.Allocator, std_io: std.Io, c: Config) !Report {
    if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const placement = try velo.workers.numa.plan(allocator, null, .{ .count = c.loops });
    defer allocator.free(placement);
    const shards = try allocator.alloc(Shard, c.loops);
    defer allocator.free(shards);
    const jobs = try allocator.alloc(Job, c.jobs);
    defer allocator.free(jobs);
    const latencies = try allocator.alloc(u64, c.jobs);
    defer allocator.free(latencies);
    const reports = try allocator.alloc(LoopReport, c.loops);
    errdefer allocator.free(reports);
    var bench: Bench = .{
        .std_io = std_io,
        .shards = shards,
        .remaining = .init(c.jobs),
        .pending_arms = .init(c.loops),
        .pending_snapshots = .init(c.loops),
    };
    for (shards, placement) |*shard, pin| shard.* = .{ .bench = &bench, .cpu = pin.cpu.? };
    var remote: usize = 0;
    for (jobs, 0..) |*job, id| {
        const executor = &shards[c.executor(id)];
        job.* = .{ .owner = &shards[c.owner(id)], .executor = executor };
        job.frame = .init(.{ .job = job, .value = id + 1, .remaining = c.rounds });
        if (executor.last) |last| last.next = job else executor.first = job;
        executor.last = job;
        remote += @intFromBool(job.owner != executor);
    }

    var group: velo.workers.Group = undefined;
    try group.init(allocator, .{
        .count = c.loops,
        .placement = placement,
        .backend = if (builtin.os.tag == .linux) .epoll else .auto,
        .name = "balance",
    }, &bench, Bench.entry);
    for (shards) |*shard| shard.loop.post(&shard.arm);
    bench.armed.waitUncancelable(std_io);
    const cpu_started = clock(.PROCESS_CPUTIME_ID);
    bench.started = clock(.MONOTONIC);
    for (shards) |*shard| shard.loop.post(&shard.start);
    bench.finished.waitUncancelable(std_io);
    const process_cpu_ns = clock(.PROCESS_CPUTIME_ID) - cpu_started;
    for (shards) |*shard| shard.loop.post(&shard.snapshot);
    bench.snapshots.waitUncancelable(std_io);
    group.deinit();

    var a: u64 = 1;
    var b: u64 = 0;
    for (0..c.rounds) |_| {
        a *%= multiplier;
        b = b *% multiplier +% increment;
    }
    var checksum: u64 = 0;
    for (jobs, latencies, 0..) |*job, *latency, id| {
        if (job.frame.result == null) return error.Incomplete;
        try job.frame.getResult();
        if (job.commits != 1 or job.value != a *% (id + 1) +% b) return error.IncorrectResult;
        checksum +%= job.value;
        latency.* = job.completion_ns;
    }
    for (shards, reports, 0..) |*shard, *report, index| {
        if (shard.wrong_thread) return error.WrongThread;
        var expected_count: usize = 0;
        var expected_computed: usize = 0;
        var expected_checksum: u64 = 0;
        for (0..c.jobs) |id| {
            expected_computed += @intFromBool(c.executor(id) == index);
            if (c.owner(id) == index) {
                expected_count += 1;
                expected_checksum +%= a *% (id + 1) +% b;
            }
        }
        if (shard.committed != expected_count or shard.computed != expected_computed or
            shard.checksum != expected_checksum) return error.IncorrectShard;
        report.* = .{
            .cpu = shard.cpu,
            .cpu_ns = shard.cpu_ns,
            .computed = shard.computed,
            .committed = shard.committed,
            .checksum = shard.checksum,
            .polls = shard.stats.polls,
            .posts = shard.stats.posts,
            .parks = shard.stats.parks,
        };
    }
    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{
        .config = c,
        .elapsed_ns = bench.ended - bench.started,
        .process_cpu_ns = process_cpu_ns,
        .completed = c.jobs,
        .checksum = checksum,
        .remote_commits = remote,
        .completion_p50_ns = latencies[(latencies.len * 50 + 99) / 100 - 1],
        .completion_p99_ns = latencies[(latencies.len * 99 + 99) / 100 - 1],
        .shards = reports,
    };
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const c = Config.parse(args[1..]) catch |err| {
        std.debug.print("usage: balance <balanced|skewed> <local|spread> <loops> <jobs> <rounds>\n", .{});
        return err;
    };
    const report = try run(init.gpa, init.io, c);
    defer init.gpa.free(report.shards);
    var buffer: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try std.json.Stringify.value(report, .{}, &stdout.interface);
    try stdout.interface.writeByte('\n');
    try stdout.interface.flush();
}

test "local and spread queries preserve results and owner-only commits" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const cpus = try velo.workers.numa.allowedCpus(std.testing.allocator);
    defer std.testing.allocator.free(cpus);
    for ([_]usize{ 1, @min(2, cpus.len) }) |loops| {
        inline for (.{ Distribution.balanced, Distribution.skewed }) |distribution| {
            inline for (.{ Policy.local, Policy.spread }) |policy| {
                const report = try run(std.testing.allocator, std.testing.io, .{
                    .distribution = distribution,
                    .policy = policy,
                    .loops = loops,
                    .jobs = 23,
                    .rounds = quantum + 7,
                });
                defer std.testing.allocator.free(report.shards);
                try std.testing.expectEqual(23, report.completed);
                try std.testing.expectEqual(46, blk: {
                    var polls: u64 = 0;
                    for (report.shards) |shard| polls += shard.polls;
                    break :blk polls;
                });
                if (policy == .local) try std.testing.expectEqual(0, report.remote_commits);
                try std.testing.expect(report.completion_p50_ns <= report.completion_p99_ns);
            }
        }
    }
}

test "benchmark rejects unbounded work and invalid configuration" {
    try std.testing.expectError(error.Usage, Config.parse(&.{}));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "skewed", "local", "0", "23", "12" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "skewed", "local", "1", "100000", "1000000" }));
}
