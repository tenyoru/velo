//! Game-server matches on per-loop shards: does placing each match by load when it is
//! created keep ticks on time, or does one loop still overload while others idle?
//! Matches own their state on one server loop; clients run on separate loops and talk to
//! them over loopback UDP. The placer sees only player counts, never lifetimes or fights.

const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const numa = velo.workers.numa;
const linux = std.os.linux;

const Backend = enum { auto, uring, epoll };
const Placement = enum { @"round-robin", @"least-loaded" };
const Grain = enum {
    /// Yields every `quantum` rounds: close to preemptive sharing.
    fixed,
    /// Yields once the loop's slice quota is spent, as velo asks of long polls.
    quota,
    /// Runs the whole tick in one poll, as simulation steps usually do.
    tick,
};
const max_players = 64;
const quantum = 1024;
const input_size = 32;
const window_ns = 100 * std.time.ns_per_ms;
/// A peer below this share of one window's CPU had room for more work.
const idle_peer = 0.5;
const snapshot_size = 256;
const receive_batch = 32;
const usage_text = "usage: matches <auto|uring|epoll> <round-robin|least-loaded> [loops=N] [clients=N] [ms=N] [rate=N] [work=N] [count=N] [seed=N] [grain=fixed|quota|tick] [pinned]\n";

const Config = struct {
    backend: Backend,
    placement: Placement,
    /// Server loops; a match never leaves the loop it was placed on.
    loops: usize = 4,
    /// Client loops, disjoint from the server loops.
    clients: usize = 2,
    duration_ms: u64 = 10_000,
    /// Ticks per second for matches and clients alike.
    rate: u64 = 60,
    /// Compute rounds per player per tick; a fight quadruples them.
    work: u64 = 10_000,
    /// Each match holds about 63 KiB and two sockets, and placement scans earlier matches.
    count: usize = 200,
    seed: u64 = 1,
    grain: Grain = .fixed,
    pinned: bool = false,

    fn parse(args: []const []const u8) !Config {
        if (args.len < 2) return error.Usage;
        var c: Config = .{
            .backend = std.meta.stringToEnum(Backend, args[0]) orelse return error.Usage,
            .placement = std.meta.stringToEnum(Placement, args[1]) orelse return error.Usage,
        };
        for (args[2..]) |arg| {
            if (std.mem.eql(u8, arg, "pinned")) {
                c.pinned = true;
                continue;
            }
            const split = std.mem.indexOfScalar(u8, arg, '=') orelse return error.Usage;
            const key = arg[0..split];
            if (std.mem.eql(u8, key, "grain")) {
                c.grain = std.meta.stringToEnum(Grain, arg[split + 1 ..]) orelse return error.Usage;
                continue;
            }
            const value = std.fmt.parseInt(u64, arg[split + 1 ..], 10) catch return error.Usage;
            if (std.mem.eql(u8, key, "loops")) {
                c.loops = std.math.cast(usize, value) orelse return error.Usage;
            } else if (std.mem.eql(u8, key, "clients")) {
                c.clients = std.math.cast(usize, value) orelse return error.Usage;
            } else if (std.mem.eql(u8, key, "ms")) {
                c.duration_ms = value;
            } else if (std.mem.eql(u8, key, "rate")) {
                c.rate = value;
            } else if (std.mem.eql(u8, key, "work")) {
                c.work = value;
            } else if (std.mem.eql(u8, key, "count")) {
                c.count = std.math.cast(usize, value) orelse return error.Usage;
            } else if (std.mem.eql(u8, key, "seed")) {
                c.seed = value;
            } else return error.Usage;
        }
        if (c.loops == 0 or c.loops > 64 or c.clients == 0 or c.clients > 64 or
            c.duration_ms < 100 or c.duration_ms > 600_000 or c.rate == 0 or c.rate > 1000 or
            c.work > 1_000_000 or c.count == 0 or c.count > 1_000) return error.Usage;
        return c;
    }

    fn period(c: Config) u64 {
        return std.time.ns_per_s / c.rate;
    }

    /// Fights last two seconds and cover about a quarter of each match.
    fn fight(c: Config, match: usize, tick: u64) bool {
        return mix(c.seed ^ (@as(u64, match) << 32) ^ (tick / (2 * c.rate))) % 4 == 0;
    }
};

fn mix(value: u64) u64 {
    var z = value +% 0x9e3779b97f4a7c15;
    z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
    z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
    return z ^ (z >> 31);
}

fn clock(id: linux.clockid_t) u64 {
    var ts: linux.timespec = undefined;
    if (linux.errno(linux.clock_gettime(id, &ts)) != .SUCCESS) @panic("clock_gettime");
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn timespec(ns: u64) io.Timespec {
    return .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
}

/// A dependent, non-affine chain: an affine step lets the optimizer collapse the loop.
fn compute(value: u64, rounds: u64) u64 {
    var result = value;
    for (0..rounds) |_| result = (result ^ (result >> 31)) *% 0x9e3779b97f4a7c15 +% 1;
    return result;
}

/// Log-linear buckets, 32 per power of two: about 3% resolution in constant space.
const Histogram = struct {
    counts: [1920]u64 = @splat(0),
    total: u64 = 0,
    max: u64 = 0,

    fn index(value: u64) usize {
        if (value < 32) return @intCast(value);
        const exponent: u6 = @intCast(63 - @clz(value));
        return 32 * @as(usize, exponent - 5) + @as(usize, @intCast(value >> (exponent - 5)));
    }

    fn upper(i: usize) u64 {
        if (i < 64) return i;
        const shift: u6 = @intCast(i / 32 - 1);
        return ((@as(u64, i % 32 + 33)) << shift) - 1;
    }

    fn record(self: *Histogram, value: u64) void {
        self.counts[index(value)] += 1;
        self.total += 1;
        self.max = @max(self.max, value);
    }

    fn merge(self: *Histogram, other: *const Histogram) void {
        for (&self.counts, other.counts) |*count, add| count.* += add;
        self.total += other.total;
        self.max = @max(self.max, other.max);
    }

    /// Upper bound of the bucket holding this rank, in microseconds.
    fn percentile(self: *const Histogram, permille: u64) f64 {
        if (self.total == 0) return 0;
        const rank = (self.total * permille + 999) / 1000;
        var seen: u64 = 0;
        for (self.counts, 0..) |count, i| {
            seen += count;
            if (seen >= rank) return micros(@min(upper(i), self.max));
        }
        return micros(self.max);
    }
};

fn micros(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_us;
}

const Plan = struct {
    start_ns: u64,
    end_ns: u64,
    players: u8,

    fn lessThan(_: void, a: Plan, b: Plan) bool {
        return a.start_ns < b.start_ns;
    }
};

/// Mostly small matches with a heavy tail, starting at random and all ending in time.
fn schedule(c: Config, plans: []Plan) void {
    var prng: std.Random.DefaultPrng = .init(c.seed);
    const random = prng.random();
    const run_ns = c.duration_ms * std.time.ns_per_ms;
    for (plans) |*plan| {
        const roll = random.uintLessThan(u8, 100);
        plan.players = if (roll < 70)
            random.intRangeAtMost(u8, 2, 8)
        else if (roll < 95)
            random.intRangeAtMost(u8, 9, 24)
        else
            random.intRangeAtMost(u8, 32, max_players);
        const life = random.intRangeAtMost(u64, run_ns / 5, run_ns / 2);
        plan.start_ns = random.uintAtMost(u64, run_ns - life);
        plan.end_ns = plan.start_ns + life;
    }
    std.mem.sort(Plan, plans, {}, Plan.lessThan);
}

const Shard = struct {
    bench: *Bench,
    cpu: ?usize = null,
    loop: *io.Loop = undefined,
    thread_id: std.Thread.Id = undefined,
    arm: io.Control.Post = .{ .callback = armClock },
    snapshot: io.Control.Post = .{ .callback = takeSnapshot },
    cpu_started: u64 = 0,
    cpu_ns: u64 = 0,
    stats: co.Executor.Stats = .{},
    wrong_thread: bool = false,
    // Server loops; the windows divide the run into `window_ns` slices.
    late: Histogram = .{},
    ticks: u64 = 0,
    missed: u64 = 0,
    inputs: u64 = 0,
    busy: []u64 = &.{},
    span: []u64 = &.{},
    missed_at: []u64 = &.{},
    sample: io.Control.Post = .{ .callback = startSampler },
    sampler: co.Frame(Sampler) = undefined,
    // Written by the coordinator, read after the run.
    matches: usize = 0,
    peak_players: u64 = 0,
    // Client loops.
    latency: Histogram = .{},
    sent: u64 = 0,
    snapshots: u64 = 0,

    fn armClock(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("arm", post);
        self.cpu_started = clock(.THREAD_CPUTIME_ID);
        const bench = self.bench;
        if (bench.pending_arms.fetchSub(1, .acq_rel) == 1) bench.armed.set(bench.std_io);
    }

    fn takeSnapshot(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("snapshot", post);
        self.cpu_ns = clock(.THREAD_CPUTIME_ID) - self.cpu_started;
        self.stats = self.loop.stats();
        const bench = self.bench;
        if (bench.pending_snapshots.fetchSub(1, .acq_rel) == 1) bench.snapshots.set(bench.std_io);
    }

    fn startSampler(post: *io.Control.Post) void {
        const self: *Shard = @fieldParentPtr("sample", post);
        self.sampler = .init(.{ .shard = self, .cpu = clock(.THREAD_CPUTIME_ID), .time = self.bench.started });
        self.loop.spawn(&self.sampler);
    }

    /// CPU share of one window; the sampler can wake late, so this divides by its real span.
    fn utilization(self: *const Shard, window: usize) f64 {
        if (self.span[window] == 0) return 0;
        return @as(f64, @floatFromInt(self.busy[window])) / @as(f64, @floatFromInt(self.span[window]));
    }

    fn check(self: *Shard) void {
        self.wrong_thread = self.wrong_thread or std.Thread.getCurrentId() != self.thread_id;
    }
};

const Bench = struct {
    config: Config,
    std_io: std.Io,
    shards: []Shard,
    matches: []Match,
    started: u64 = 0,
    ended: u64 = 0,
    remaining: std.atomic.Value(usize),
    pending_arms: std.atomic.Value(usize),
    pending_snapshots: std.atomic.Value(usize),
    armed: std.Io.Event = .unset,
    finished: std.Io.Event = .unset,
    snapshots: std.Io.Event = .unset,
    coordinator: co.Frame(Coordinator) = undefined,
    launch: io.Control.Post = .{ .callback = begin },

    fn entry(self: *Bench, loop: *io.Loop, index: usize) !void {
        const shard = &self.shards[index];
        shard.loop = loop;
        shard.thread_id = std.Thread.getCurrentId();
        if (shard.cpu) |cpu| {
            const cpus = try numa.allowedCpus(std.heap.page_allocator);
            defer std.heap.page_allocator.free(cpus);
            if (cpus.len != 1 or cpus[0] != cpu) return error.AffinityMismatch;
        }
    }

    fn begin(post: *io.Control.Post) void {
        const self: *Bench = @fieldParentPtr("launch", post);
        self.coordinator = .init(.{ .bench = self });
        self.shards[self.shards.len - 1].loop.spawn(&self.coordinator);
    }

    fn done(self: *Bench) void {
        if (self.remaining.fetchSub(1, .acq_rel) == 1) {
            self.ended = clock(.MONOTONIC);
            self.finished.set(self.std_io);
        }
    }
};

/// Sends one datagram per player at once and completes when every send has.
const Burst = struct {
    ops: [max_players]io.SendTo = undefined,
    ready: [max_players]bool = undefined,
    count: usize = 0,
    done: usize = 0,
    failed: u64 = 0,

    fn start(
        self: *Burst,
        loop: *io.Loop,
        fd: io.Socket,
        comptime size: usize,
        buffers: *const [max_players][size]u8,
        count: usize,
        address: *const io.SockAddr.in,
    ) void {
        for (self.ops[0..count], buffers[0..count], self.ready[0..count]) |*op, *buffer, *ready| {
            op.* = .init(loop, .{
                .fd = fd,
                .buffer = buffer,
                .address = @ptrCast(address),
                .address_len = @sizeOf(io.SockAddr.in),
            });
            ready.* = false;
        }
        self.count = count;
        self.done = 0;
    }

    fn poll(self: *Burst, task: *co.Task) bool {
        for (self.ops[0..self.count], self.ready[0..self.count]) |*op, *ready| {
            if (ready.*) continue;
            const result = op.poll(task).value() orelse continue;
            ready.* = true;
            self.done += 1;
            _ = result catch {
                self.failed += 1;
            };
        }
        return self.done == self.count;
    }
};

const Latest = struct { seq: u32 = 0, sent_ns: u64 = 0 };

const Match = struct {
    bench: *Bench,
    id: usize,
    plan: Plan,
    server: *Shard = undefined,
    client: *Shard = undefined,
    server_fd: io.Socket,
    client_fd: io.Socket,
    server_address: io.SockAddr.in,
    client_address: io.SockAddr.in,
    start_server: io.Control.Post = .{ .callback = startServer },
    start_client: io.Control.Post = .{ .callback = startClient },
    ticker: co.Frame(Ticker) = undefined,
    listener: co.Frame(Listener) = undefined,
    sender: co.Frame(Sender) = undefined,
    viewer: co.Frame(Viewer) = undefined,
    latest: [max_players]Latest = @splat(.{}),
    snapshots: [max_players][snapshot_size]u8 = @splat(@splat(0)),
    server_burst: Burst = .{},
    ticks: u64 = 0,
    missed: u64 = 0,
    inputs: u64 = 0,
    inputs_out: [max_players][input_size]u8 = @splat(@splat(0)),
    client_burst: Burst = .{},
    acked: [max_players]u32 = @splat(0),
    sent: u64 = 0,
    received: u64 = 0,

    fn expected(self: *const Match) u64 {
        const period = self.bench.config.period();
        return (self.plan.end_ns - self.plan.start_ns + period - 1) / period;
    }

    /// Clients are not synchronized with their match; each runs at its own phase.
    fn offset(self: *const Match) u64 {
        return mix(self.bench.config.seed +% self.id) % self.bench.config.period();
    }

    fn clientExpected(self: *const Match) u64 {
        const life = self.plan.end_ns - self.plan.start_ns;
        const period = self.bench.config.period();
        return if (life > self.offset()) (life - self.offset() + period - 1) / period else 0;
    }

    fn due(self: *const Match, tick: u64, shift: u64) u64 {
        return self.bench.started + self.plan.start_ns + shift + tick * self.bench.config.period();
    }

    fn startServer(post: *io.Control.Post) void {
        const self: *Match = @fieldParentPtr("start_server", post);
        self.listener = .init(.{ .match = self });
        self.ticker = .init(.{ .match = self, .expected = self.expected() });
        self.server.loop.spawn(&self.listener);
        self.server.loop.spawn(&self.ticker);
    }

    fn startClient(post: *io.Control.Post) void {
        const self: *Match = @fieldParentPtr("start_client", post);
        self.viewer = .init(.{ .match = self });
        self.sender = .init(.{ .match = self, .expected = self.clientExpected() });
        self.client.loop.spawn(&self.viewer);
        self.client.loop.spawn(&self.sender);
    }
};

/// Server tick: wait for the scheduled time, simulate, send every player a snapshot.
/// A tick later than one period skips ahead; ran plus missed ticks equal `expected`.
const Ticker = struct {
    match: *Match,
    expected: u64,
    tick: u64 = 0,
    phase: enum { wait, compute, send } = .wait,
    timer: io.Timeout = undefined,
    timing: bool = false,
    remaining: u64 = 0,
    value: u64 = 0,

    pub fn poll(self: *Ticker, task: *co.Task) co.Poll(void) {
        const match = self.match;
        const shard = match.server;
        const config = match.bench.config;
        const period = config.period();
        while (true) switch (self.phase) {
            .wait => {
                if (self.timing) {
                    const result = self.timer.poll(task).value() orelse return .pending;
                    result catch @panic("tick timer");
                    self.timing = false;
                }
                const due = match.due(self.tick, 0);
                const now = clock(.MONOTONIC);
                if (now < due) {
                    self.timer = .init(shard.loop, .{ .duration = timespec(due - now) });
                    self.timing = true;
                    continue;
                }
                const late = now - due;
                shard.late.record(late);
                const skipped = @min(late / period, self.expected - 1 - self.tick);
                self.tick += skipped;
                shard.missed += skipped;
                match.missed += skipped;
                shard.missed_at[@min((now - match.bench.started) / window_ns, shard.missed_at.len - 1)] += skipped;
                const fights: u64 = if (config.fight(match.id, self.tick)) 4 else 1;
                self.remaining = match.plan.players * config.work * fights;
                self.phase = .compute;
            },
            .compute => {
                switch (config.grain) {
                    .fixed => self.step(@min(quantum, self.remaining)),
                    .quota => while (self.remaining != 0) {
                        self.step(@min(quantum, self.remaining));
                        if (shard.loop.shouldYield()) break;
                    },
                    .tick => self.step(self.remaining),
                }
                if (self.remaining != 0) {
                    task.wake();
                    return .pending;
                }
                for (match.snapshots[0..match.plan.players], match.latest[0..match.plan.players], 0..) |*snapshot, latest, player| {
                    std.mem.writeInt(u16, snapshot[0..2], @intCast(player), .little);
                    std.mem.writeInt(u32, snapshot[4..8], @truncate(self.tick), .little);
                    std.mem.writeInt(u32, snapshot[8..12], latest.seq, .little);
                    std.mem.writeInt(u64, snapshot[16..24], latest.sent_ns, .little);
                    std.mem.writeInt(u64, snapshot[24..32], self.value, .little);
                }
                match.server_burst.start(shard.loop, match.server_fd, snapshot_size, &match.snapshots, match.plan.players, &match.client_address);
                self.phase = .send;
            },
            .send => {
                if (!match.server_burst.poll(task)) return .pending;
                shard.check();
                shard.ticks += 1;
                match.ticks += 1;
                self.tick += 1;
                if (self.tick == self.expected) {
                    match.listener.cancel();
                    return .{ .ready = {} };
                }
                self.phase = .wait;
            },
        };
    }

    fn step(self: *Ticker, rounds: u64) void {
        self.value = compute(self.value, rounds);
        self.remaining -= rounds;
    }
};

/// Server receive loop: keeps each player's newest input; closes the socket when stopped.
/// Batches drain what queued while the loop was busy; one datagram per turn falls behind.
const Listener = struct {
    match: *Match,
    recv: io.RecvBatch = undefined,
    receiving: bool = false,
    stopping: bool = false,
    buffers: [receive_batch][input_size]u8 = undefined,
    datagrams: [receive_batch]io.Datagram = undefined,

    pub fn poll(self: *Listener, task: *co.Task) co.Poll(void) {
        const match = self.match;
        while (true) {
            if (!self.receiving) {
                if (self.stopping) {
                    io.closeSocket(match.server_fd) catch @panic("close server socket");
                    match.bench.done();
                    return .{ .ready = {} };
                }
                for (&self.datagrams, &self.buffers) |*datagram, *buffer| datagram.* = .{ .buffer = buffer };
                self.recv = .init(match.server.loop, .{ .fd = match.server_fd, .datagrams = &self.datagrams });
                self.receiving = true;
            }
            const result = self.recv.poll(task).value() orelse return .pending;
            self.receiving = false;
            const count = result catch |err| switch (err) {
                error.OperationCanceled => {
                    self.stopping = true;
                    continue;
                },
                else => @panic("server receive"),
            };
            for (self.datagrams[0..count]) |datagram| {
                if (datagram.len != input_size or datagram.truncated) continue;
                const input = datagram.buffer[0..input_size];
                const player = std.mem.readInt(u16, input[0..2], .little);
                if (player >= match.plan.players) continue;
                const seq = std.mem.readInt(u32, input[4..8], .little);
                const latest = &match.latest[player];
                if (seq > latest.seq) latest.* = .{ .seq = seq, .sent_ns = std.mem.readInt(u64, input[8..16], .little) };
                match.inputs += 1;
                match.server.inputs += 1;
            }
        }
    }

    pub fn cancel(self: *Listener) void {
        self.stopping = true;
        if (self.receiving) self.recv.cancel();
    }
};

/// Client tick: every player sends its input, numbered by tick and stamped with send time.
const Sender = struct {
    match: *Match,
    expected: u64,
    tick: u64 = 0,
    phase: enum { wait, send } = .wait,
    timer: io.Timeout = undefined,
    timing: bool = false,

    pub fn poll(self: *Sender, task: *co.Task) co.Poll(void) {
        const match = self.match;
        const shard = match.client;
        const period = match.bench.config.period();
        while (true) switch (self.phase) {
            .wait => {
                if (self.timing) {
                    const result = self.timer.poll(task).value() orelse return .pending;
                    result catch @panic("client timer");
                    self.timing = false;
                }
                if (self.tick == self.expected) {
                    match.viewer.cancel();
                    return .{ .ready = {} };
                }
                const due = match.due(self.tick, match.offset());
                const now = clock(.MONOTONIC);
                if (now < due) {
                    self.timer = .init(shard.loop, .{ .duration = timespec(due - now) });
                    self.timing = true;
                    continue;
                }
                self.tick += @min((now - due) / period, self.expected - 1 - self.tick);
                for (match.inputs_out[0..match.plan.players], 0..) |*input, player| {
                    std.mem.writeInt(u16, input[0..2], @intCast(player), .little);
                    std.mem.writeInt(u32, input[4..8], @intCast(self.tick + 1), .little);
                    std.mem.writeInt(u64, input[8..16], now, .little);
                }
                match.client_burst.start(shard.loop, match.client_fd, input_size, &match.inputs_out, match.plan.players, &match.server_address);
                self.phase = .send;
            },
            .send => {
                if (!match.client_burst.poll(task)) return .pending;
                shard.check();
                shard.sent += match.plan.players;
                match.sent += match.plan.players;
                self.tick += 1;
                self.phase = .wait;
            },
        };
    }
};

/// Client receive loop: input-to-snapshot latency, once per newly acknowledged input.
const Viewer = struct {
    match: *Match,
    recv: io.RecvFrom = undefined,
    receiving: bool = false,
    stopping: bool = false,
    buffer: [snapshot_size]u8 = undefined,
    peer: io.SockAddr.in = undefined,
    peer_len: io.SockLen = 0,

    pub fn poll(self: *Viewer, task: *co.Task) co.Poll(void) {
        const match = self.match;
        while (true) {
            if (!self.receiving) {
                if (self.stopping) {
                    io.closeSocket(match.client_fd) catch @panic("close client socket");
                    match.bench.done();
                    return .{ .ready = {} };
                }
                self.peer_len = @sizeOf(io.SockAddr.in);
                self.recv = .init(match.client.loop, .{
                    .fd = match.client_fd,
                    .buffer = &self.buffer,
                    .address = @ptrCast(&self.peer),
                    .address_len = &self.peer_len,
                });
                self.receiving = true;
            }
            const result = self.recv.poll(task).value() orelse return .pending;
            self.receiving = false;
            const n = result catch |err| switch (err) {
                error.OperationCanceled => {
                    self.stopping = true;
                    continue;
                },
                else => @panic("client receive"),
            };
            if (n != snapshot_size) continue;
            const player = std.mem.readInt(u16, self.buffer[0..2], .little);
            if (player >= match.plan.players) continue;
            match.received += 1;
            match.client.snapshots += 1;
            const seq = std.mem.readInt(u32, self.buffer[8..12], .little);
            if (seq > match.acked[player]) {
                match.acked[player] = seq;
                const sent_ns = std.mem.readInt(u64, self.buffer[16..24], .little);
                match.client.latency.record(clock(.MONOTONIC) -| sent_ns);
            }
        }
    }

    pub fn cancel(self: *Viewer) void {
        self.stopping = true;
        if (self.receiving) self.recv.cancel();
    }
};

const Sampler = struct {
    shard: *Shard,
    cpu: u64,
    time: u64,
    window: usize = 0,
    timer: io.Timeout = undefined,
    timing: bool = false,

    pub fn poll(self: *Sampler, task: *co.Task) co.Poll(void) {
        const shard = self.shard;
        while (self.window < shard.busy.len) {
            if (self.timing) {
                const result = self.timer.poll(task).value() orelse return .pending;
                result catch @panic("sampler timer");
                self.timing = false;
            }
            const due = shard.bench.started + (self.window + 1) * window_ns;
            const now = clock(.MONOTONIC);
            if (now < due) {
                self.timer = .init(shard.loop, .{ .duration = timespec(due - now) });
                self.timing = true;
                continue;
            }
            const cpu = clock(.THREAD_CPUTIME_ID);
            shard.busy[self.window] = cpu - self.cpu;
            shard.span[self.window] = now - self.time;
            self.cpu = cpu;
            self.time = now;
            self.window += 1;
        }
        shard.bench.done();
        return .{ .ready = {} };
    }
};

/// Missed ticks in windows where another server loop used under `idle_peer` of its CPU.
fn idlePeerMisses(servers: []const Shard) u64 {
    var total: u64 = 0;
    for (servers) |*shard| {
        for (shard.missed_at, 0..) |missed, window| {
            if (missed == 0) continue;
            for (servers) |*peer| {
                if (peer != shard and peer.span[window] != 0 and peer.utilization(window) < idle_peer) {
                    total += missed;
                    break;
                }
            }
        }
    }
    return total;
}

/// Starts matches on schedule. Least-loaded counts the players of matches still running
/// on each server loop; nothing else about a match is known when it is placed.
const Coordinator = struct {
    bench: *Bench,
    next: usize = 0,
    timer: io.Timeout = undefined,
    timing: bool = false,

    pub fn poll(self: *Coordinator, task: *co.Task) co.Poll(void) {
        const bench = self.bench;
        const loop = bench.shards[bench.shards.len - 1].loop;
        while (self.next < bench.matches.len) {
            if (self.timing) {
                const result = self.timer.poll(task).value() orelse return .pending;
                result catch @panic("coordinator timer");
                self.timing = false;
            }
            const match = &bench.matches[self.next];
            const due = bench.started + match.plan.start_ns;
            const now = clock(.MONOTONIC);
            if (now < due) {
                self.timer = .init(loop, .{ .duration = timespec(due - now) });
                self.timing = true;
                continue;
            }
            const c = bench.config;
            const index = switch (c.placement) {
                .@"round-robin" => self.next % c.loops,
                .@"least-loaded" => self.leastLoaded(match.plan.start_ns),
            };
            match.server = &bench.shards[index];
            match.client = &bench.shards[c.loops + self.next % c.clients];
            match.server.matches += 1;
            match.server.peak_players = @max(match.server.peak_players, self.load(index, match.plan.start_ns) + match.plan.players);
            match.server.loop.post(&match.start_server);
            match.client.loop.post(&match.start_client);
            self.next += 1;
        }
        return .{ .ready = {} };
    }

    fn load(self: *const Coordinator, index: usize, at: u64) u64 {
        var players: u64 = 0;
        for (self.bench.matches[0..self.next]) |*placed| {
            if (placed.server == &self.bench.shards[index] and placed.plan.end_ns > at) players += placed.plan.players;
        }
        return players;
    }

    fn leastLoaded(self: *const Coordinator, at: u64) usize {
        var best: usize = 0;
        var lowest: u64 = std.math.maxInt(u64);
        for (0..self.bench.config.loops) |index| {
            const players = self.load(index, at);
            if (players < lowest) {
                lowest = players;
                best = index;
            }
        }
        return best;
    }
};

const ServerReport = struct {
    cpu: ?usize,
    utilization: f64,
    peak_window_utilization: f64,
    matches: usize,
    peak_players: u64,
    ticks: u64,
    missed: u64,
    inputs: u64,
    late_p50_us: f64,
    late_p99_us: f64,
    late_max_us: f64,
    polls: u64,
    parks: u64,
};

const ClientReport = struct {
    cpu: ?usize,
    utilization: f64,
    sent: u64,
    snapshots: u64,
};

const Report = struct {
    config: Config,
    elapsed_ms: f64,
    ticks: u64,
    missed: u64,
    missed_ratio: f64,
    late_p50_us: f64,
    late_p99_us: f64,
    late_p999_us: f64,
    late_max_us: f64,
    latency_p50_us: f64,
    latency_p99_us: f64,
    latency_p999_us: f64,
    latency_max_us: f64,
    inputs_sent: u64,
    inputs_received: u64,
    snapshots_sent: u64,
    snapshots_received: u64,
    send_failures: u64,
    /// Missed ticks in windows where another server loop used under `idle_peer` of its CPU.
    missed_with_idle_peer: u64,
    utilization_max: f64,
    utilization_min: f64,
    servers: []ServerReport,
    clients: []ClientReport,

    fn deinit(self: Report, allocator: std.mem.Allocator) void {
        allocator.free(self.servers);
        allocator.free(self.clients);
    }
};

fn bindLoopback(address: *io.SockAddr.in) !io.Socket {
    const fd = try io.udpSocket(.ipv4);
    errdefer io.closeSocket(fd) catch {};
    address.* = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
    var len: io.SockLen = @sizeOf(io.SockAddr.in);
    try io.bind(fd, @ptrCast(address), len);
    try io.localAddress(fd, @ptrCast(address), &len);
    return fd;
}

fn run(allocator: std.mem.Allocator, std_io: std.Io, c: Config) !Report {
    if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const total = c.loops + c.clients + 1;

    var placement: []numa.Placement = &.{};
    defer allocator.free(placement);
    var topology: ?numa.Topology = null;
    defer if (topology) |*known| known.deinit();
    if (c.pinned) {
        topology = try numa.Topology.discover(allocator, std_io);
        placement = try numa.plan(allocator, &topology.?, .{ .count = total });
    }

    const plans = try allocator.alloc(Plan, c.count);
    defer allocator.free(plans);
    schedule(c, plans);

    const shards = try allocator.alloc(Shard, total);
    defer allocator.free(shards);
    const matches = try allocator.alloc(Match, c.count);
    defer allocator.free(matches);

    var bench: Bench = .{
        .config = c,
        .std_io = std_io,
        .shards = shards,
        .matches = matches,
        .remaining = .init(2 * c.count + c.loops),
        .pending_arms = .init(total),
        .pending_snapshots = .init(total),
    };
    for (shards, 0..) |*shard, index| shard.* = .{
        .bench = &bench,
        .cpu = if (placement.len != 0) placement[index].cpu else null,
    };
    const windows = (c.duration_ms * std.time.ns_per_ms + window_ns - 1) / window_ns;
    const series = try allocator.alloc(u64, 3 * windows * c.loops);
    defer allocator.free(series);
    @memset(series, 0);
    for (shards[0..c.loops], 0..) |*shard, index| {
        const base = 3 * windows * index;
        shard.busy = series[base..][0..windows];
        shard.span = series[base + windows ..][0..windows];
        shard.missed_at = series[base + 2 * windows ..][0..windows];
    }

    // Sockets close as their matches end; until then, a failed setup closes them here.
    var opened: usize = 0;
    errdefer for (matches[0..opened]) |*match| {
        io.closeSocket(match.server_fd) catch {};
        io.closeSocket(match.client_fd) catch {};
    };
    for (matches, plans, 0..) |*match, plan, id| {
        var server_address: io.SockAddr.in = undefined;
        const server_fd = try bindLoopback(&server_address);
        var client_address: io.SockAddr.in = undefined;
        const client_fd = bindLoopback(&client_address) catch |err| {
            io.closeSocket(server_fd) catch {};
            return err;
        };
        match.* = .{
            .bench = &bench,
            .id = id,
            .plan = plan,
            .server_fd = server_fd,
            .client_fd = client_fd,
            .server_address = server_address,
            .client_address = client_address,
        };
        opened += 1;
    }

    var group: velo.workers.Group = undefined;
    try group.init(allocator, .{
        .count = total,
        .loop = .{ .entries = 256 },
        .backend = switch (c.backend) {
            .auto => .auto,
            .uring => .uring,
            .epoll => .epoll,
        },
        .name = "matches",
        .placement = placement,
    }, &bench, Bench.entry);
    opened = 0;

    for (shards) |*shard| shard.loop.post(&shard.arm);
    bench.armed.waitUncancelable(std_io);
    bench.started = clock(.MONOTONIC);
    for (shards[0..c.loops]) |*shard| shard.loop.post(&shard.sample);
    shards[total - 1].loop.post(&bench.launch);
    bench.finished.waitUncancelable(std_io);
    for (shards) |*shard| shard.loop.post(&shard.snapshot);
    bench.snapshots.waitUncancelable(std_io);
    group.deinit();

    var report: Report = .{
        .config = c,
        .elapsed_ms = @as(f64, @floatFromInt(bench.ended - bench.started)) / std.time.ns_per_ms,
        .ticks = 0,
        .missed = 0,
        .missed_ratio = 0,
        .late_p50_us = 0,
        .late_p99_us = 0,
        .late_p999_us = 0,
        .late_max_us = 0,
        .latency_p50_us = 0,
        .latency_p99_us = 0,
        .latency_p999_us = 0,
        .latency_max_us = 0,
        .inputs_sent = 0,
        .inputs_received = 0,
        .snapshots_sent = 0,
        .snapshots_received = 0,
        .send_failures = 0,
        .missed_with_idle_peer = 0,
        .utilization_max = 0,
        .utilization_min = std.math.inf(f64),
        .servers = try allocator.alloc(ServerReport, c.loops),
        .clients = undefined,
    };
    errdefer allocator.free(report.servers);
    report.clients = try allocator.alloc(ClientReport, c.clients);
    errdefer allocator.free(report.clients);

    for (matches) |*match| {
        if (match.ticker.result == null or match.listener.result == null or
            match.sender.result == null or match.viewer.result == null) return error.Incomplete;
        if (match.ticks + match.missed != match.expected()) return error.TickAccounting;
        if (match.inputs > match.sent or match.received > match.ticks * match.plan.players) return error.PacketAccounting;
        report.inputs_sent += match.sent;
        report.inputs_received += match.inputs;
        report.snapshots_sent += match.ticks * match.plan.players;
        report.snapshots_received += match.received;
        report.send_failures += match.server_burst.failed + match.client_burst.failed;
    }

    const elapsed: f64 = @floatFromInt(bench.ended - bench.started);
    var late: Histogram = .{};
    var latency: Histogram = .{};
    for (shards[0..c.loops], report.servers) |*shard, *server| {
        if (shard.wrong_thread) return error.WrongThread;
        const utilization = @as(f64, @floatFromInt(shard.cpu_ns)) / elapsed;
        report.utilization_max = @max(report.utilization_max, utilization);
        report.utilization_min = @min(report.utilization_min, utilization);
        report.ticks += shard.ticks;
        report.missed += shard.missed;
        late.merge(&shard.late);
        var peak: f64 = 0;
        for (0..shard.busy.len) |window| peak = @max(peak, shard.utilization(window));
        server.* = .{
            .cpu = shard.cpu,
            .utilization = utilization,
            .peak_window_utilization = peak,
            .matches = shard.matches,
            .peak_players = shard.peak_players,
            .ticks = shard.ticks,
            .missed = shard.missed,
            .inputs = shard.inputs,
            .late_p50_us = shard.late.percentile(500),
            .late_p99_us = shard.late.percentile(990),
            .late_max_us = micros(shard.late.max),
            .polls = shard.stats.polls,
            .parks = shard.stats.parks,
        };
    }
    for (shards[c.loops .. c.loops + c.clients], report.clients) |*shard, *client| {
        if (shard.wrong_thread) return error.WrongThread;
        latency.merge(&shard.latency);
        client.* = .{
            .cpu = shard.cpu,
            .utilization = @as(f64, @floatFromInt(shard.cpu_ns)) / elapsed,
            .sent = shard.sent,
            .snapshots = shard.snapshots,
        };
    }
    report.missed_with_idle_peer = idlePeerMisses(shards[0..c.loops]);
    report.missed_ratio = @as(f64, @floatFromInt(report.missed)) / @as(f64, @floatFromInt(report.ticks + report.missed));
    report.late_p50_us = late.percentile(500);
    report.late_p99_us = late.percentile(990);
    report.late_p999_us = late.percentile(999);
    report.late_max_us = micros(late.max);
    report.latency_p50_us = latency.percentile(500);
    report.latency_p99_us = latency.percentile(990);
    report.latency_p999_us = latency.percentile(999);
    report.latency_max_us = micros(latency.max);
    return report;
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const c = Config.parse(args[1..]) catch |err| {
        std.debug.print(usage_text, .{});
        return err;
    };
    const report = try run(init.gpa, init.io, c);
    defer report.deinit(init.gpa);
    var buffer: [8192]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &buffer);
    try std.json.Stringify.value(report, .{}, &stdout.interface);
    try stdout.interface.writeByte('\n');
    try stdout.interface.flush();
}

test "matches account for every tick, input and snapshot under each placement and grain" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    inline for (.{
        .{ Placement.@"round-robin", Grain.fixed },
        .{ Placement.@"least-loaded", Grain.quota },
        .{ Placement.@"least-loaded", Grain.tick },
    }) |case| {
        const report = try run(std.testing.allocator, std.testing.io, .{
            .backend = .auto,
            .placement = case[0],
            .grain = case[1],
            .loops = 2,
            .clients = 1,
            .duration_ms = 300,
            .rate = 50,
            .work = 64,
            .count = 6,
        });
        defer report.deinit(std.testing.allocator);
        try std.testing.expect(report.ticks > 0);
        try std.testing.expect(report.inputs_received > 0 and report.inputs_received <= report.inputs_sent);
        try std.testing.expect(report.snapshots_received > 0 and report.snapshots_received <= report.snapshots_sent);
        var matches: usize = 0;
        for (report.servers) |server| matches += server.matches;
        try std.testing.expectEqual(6, matches);
    }
}

test "matches histogram buckets bound their values" {
    var histogram: Histogram = .{};
    for ([_]u64{ 0, 1, 31, 32, 63, 64, 65, 1000, 123_456, std.math.maxInt(u32) }) |value| {
        const i = Histogram.index(value);
        try std.testing.expect(value <= Histogram.upper(i));
        if (i > 0) try std.testing.expect(value > Histogram.upper(i - 1));
        histogram.record(value);
    }
    try std.testing.expectEqual(10, histogram.total);
    try std.testing.expectEqual(micros(std.math.maxInt(u32)), histogram.percentile(1000));
}

test "matches count misses only while a sampled peer had idle CPU" {
    // Shard callbacks read Linux clocks, so even building one is Linux-only.
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    // Windows: loop 0 is saturated throughout; loop 1 idles in window 1 only; loop 2
    // sits exactly at the threshold, then goes unsampled in window 2.
    var busy = [_][3]u64{ .{ 100, 100, 100 }, .{ 90, 30, 95 }, .{ 50, 50, 0 } };
    var span = [_][3]u64{ .{ 100, 100, 100 }, .{ 100, 100, 100 }, .{ 100, 100, 0 } };
    var missed = [_][3]u64{ .{ 2, 5, 7 }, .{ 0, 0, 0 }, .{ 0, 0, 0 } };
    var servers: [3]Shard = undefined;
    for (&servers, &busy, &span, &missed) |*server, *b, *w, *m| server.* = .{
        .bench = undefined,
        .busy = b,
        .span = w,
        .missed_at = m,
    };
    try std.testing.expectEqual(5, idlePeerMisses(&servers));

    missed[1][0] = 3;
    try std.testing.expectEqual(5, idlePeerMisses(&servers));
    missed[1][2] = 4;
    try std.testing.expectEqual(5, idlePeerMisses(&servers));
}

test "matches rejects unbounded or invalid configuration" {
    try std.testing.expectError(error.Usage, Config.parse(&.{}));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "random" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "round-robin", "loops=0" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "round-robin", "ms=50" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "round-robin", "speed=2" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "round-robin", "count=1001" }));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "auto", "round-robin", "grain=5" }));
    try std.testing.expectEqual(Grain.tick, (try Config.parse(&.{ "auto", "round-robin", "grain=tick" })).grain);
    const c = try Config.parse(&.{ "uring", "least-loaded", "loops=3", "work=5", "pinned" });
    try std.testing.expectEqual(3, c.loops);
    try std.testing.expect(c.pinned);
}
