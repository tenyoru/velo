const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const numa = velo.workers.numa;
const Group = velo.workers.Group;

const Mode = enum { unpinned, pinned, local, remote };
const Preparation = enum { lazy, prefault, huge };
const mib = 1024 * 1024;
const line_bytes = std.atomic.cache_line;
const seed = 0x6a09e667f3bcc909;

const Config = struct {
    mode: Mode,
    threads: usize = 2,
    memory_mib: usize = 64,
    iterations: usize = 1_000_000,
    preparation: Preparation = .lazy,

    fn parse(args: []const []const u8) !Config {
        if (args.len == 0 or args.len > 5) return error.Usage;
        var config: Config = .{ .mode = std.meta.stringToEnum(Mode, args[0]) orelse return error.InvalidMode };
        if (args.len > 1) config.threads = try number(args[1], 64);
        if (args.len > 2) config.memory_mib = try number(args[2], 4096);
        if (args.len > 3) config.iterations = try number(args[3], 1_000_000_000);
        if (args.len > 4) config.preparation = std.meta.stringToEnum(Preparation, args[4]) orelse return error.InvalidPreparation;
        if ((config.mode == .unpinned or config.mode == .pinned) and config.preparation != .lazy)
            return error.UnsupportedPreparationMode;
        if (config.threads * config.memory_mib > 4096) return error.MemoryLimit;
        return config;
    }

    fn number(text: []const u8, max: usize) !usize {
        if (text.len == 0) return error.InvalidNumber;
        for (text) |c| if (c < '0' or c > '9') return error.InvalidNumber;
        const value = std.fmt.parseInt(usize, text, 10) catch return error.InvalidNumber;
        if (value == 0 or value > max) return error.InvalidNumber;
        return value;
    }
};

const Cell = struct {
    next: usize align(line_bytes),
    padding: [line_bytes - @sizeOf(usize)]u8,
};

const Traversal = struct { accesses: usize, index: usize, checksum: u64 };

fn initialize(allocator: std.mem.Allocator, cells: []Cell, iterations: usize) !Traversal {
    const order = try allocator.alloc(usize, cells.len);
    defer allocator.free(order);
    for (order, 0..) |*index, i| index.* = i;
    var prng = std.Random.DefaultPrng.init(seed);
    prng.random().shuffle(usize, order[1..]);
    for (order, 0..) |index, i| cells[index] = .{
        .next = order[(i + 1) % order.len],
        .padding = @splat(0),
    };

    const remainder = iterations % cells.len;
    var checksum = cycleSum(cells.len) *% (iterations / cells.len);
    for (order[1 .. remainder + 1]) |index| checksum +%= index;
    return .{ .accesses = iterations, .index = order[remainder], .checksum = checksum };
}

fn cycleSum(count: usize) u64 {
    return @as(u64, @intCast(count)) * (count - 1) / 2;
}

fn chase(cells: []const Cell, iterations: usize) Traversal {
    var result: Traversal = .{ .accesses = 0, .index = 0, .checksum = 0 };
    while (result.accesses < iterations) : (result.accesses += 1) {
        const next: *const volatile usize = &cells[result.index].next;
        result.index = next.*;
        result.checksum +%= result.index;
    }
    return result;
}

fn verify(actual: Traversal, expected: Traversal) !void {
    if (!std.meta.eql(actual, expected)) return error.IncorrectTraversal;
}

fn remoteNode(topology: *const numa.Topology, cpu_node: usize) ?usize {
    for (topology.nodes) |node| if (node.memory and node.id != cpu_node) return node.id;
    return null;
}

fn readText(allocator: std.mem.Allocator, io: std.Io, path: []const u8, limit: std.Io.Limit) ![]u8 {
    const file = try std.Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);
    var reader = file.readerStreaming(io, &.{});
    // procfs reports size zero; size-based reads can silently return no data.
    reader.mode = .streaming_simple;
    return reader.interface.allocRemaining(allocator, limit) catch |err| switch (err) {
        error.ReadFailed => return reader.err.?,
        else => return err,
    };
}

fn pmdSize(text: []const u8) !usize {
    const size = std.fmt.parseInt(usize, std.mem.trim(u8, text, " \t\r\n"), 10) catch return error.Unsupported;
    if (size <= std.heap.pageSize() or !std.math.isPowerOfTwo(size)) return error.Unsupported;
    return size;
}

fn preparationOptions(allocator: std.mem.Allocator, io: std.Io, preparation: Preparation) !numa.Region.Options {
    if (preparation != .huge) return .{ .prefault = preparation == .prefault };
    const text = readText(allocator, io, "/sys/kernel/mm/transparent_hugepage/hpage_pmd_size", .limited(64)) catch |err| switch (err) {
        error.FileNotFound => return error.Unsupported,
        else => return err,
    };
    defer allocator.free(text);
    return .{ .alignment = try pmdSize(text), .prefault = true, .huge_pages = true };
}

fn allocate(bytes: usize, node: ?usize, options: numa.Region.Options) !numa.Region {
    if (builtin.os.tag != .linux) return error.Unsupported;
    if (node) |id| return numa.Region.initWithOptions(bytes, .{ .bind = id }, options);
    if (!std.meta.eql(options, numa.Region.Options{})) return error.UnsupportedPreparationMode;
    const linux = std.os.linux;
    const address = linux.mmap(null, bytes, .{ .READ = true, .WRITE = true }, .{
        .TYPE = .PRIVATE,
        .ANONYMOUS = true,
    }, -1, 0);
    if (linux.errno(address) != .SUCCESS) return error.MappingFailed;
    return .{ .bytes = @as([*]align(std.heap.page_size_min) u8, @ptrFromInt(address))[0..bytes] };
}

const Observation = struct { cpu: usize, node: usize };

fn currentCpu() !Observation {
    if (builtin.os.tag != .linux) return error.Unsupported;
    var cpu: u32 = 0;
    var node: u32 = 0;
    const rc = std.os.linux.syscall3(.getcpu, @intFromPtr(&cpu), @intFromPtr(&node), 0);
    if (std.os.linux.errno(rc) != .SUCCESS) return error.GetCpuFailed;
    return .{ .cpu = cpu, .node = node };
}

const Worker = struct {
    coordinator: *Coordinator,
    chosen: numa.Topology.Cpu,
    memory_node: ?usize,
    region: numa.Region,
    expected: Traversal,
    task: co.Task = .{ .poll_fn = poll },
    done: std.Io.Event = .unset,
    result: anyerror!Traversal = error.NotRun,
    observed_start: Observation = undefined,
    observed_end: Observation = undefined,
    started_ns: i128 = 0,
    finished_ns: i128 = 0,
    entry_warmup_ns: u64 = 0,

    fn cells(self: *const Worker) []Cell {
        return @as([*]Cell, @ptrCast(@alignCast(self.region.bytes.ptr)))[0 .. self.region.bytes.len / @sizeOf(Cell)];
    }

    fn poll(task: *co.Task) bool {
        const self: *Worker = @alignCast(@fieldParentPtr("task", task));
        self.result = self.measure(task);
        self.done.set(self.coordinator.std_io);
        return true;
    }

    fn measure(self: *Worker, task: *co.Task) !Traversal {
        // Failed Group initialization drains canceled tasks without opening our start gate.
        if (task.cancel_requested) return error.Canceled;
        self.coordinator.go.waitUncancelable(self.coordinator.std_io);
        self.observed_start = try currentCpu();
        self.started_ns = std.Io.Clock.awake.now(self.coordinator.std_io).nanoseconds;
        const result = chase(self.cells(), self.coordinator.config.iterations);
        self.finished_ns = std.Io.Clock.awake.now(self.coordinator.std_io).nanoseconds;
        self.observed_end = try currentCpu();
        try verify(result, self.expected);
        if (self.coordinator.config.mode != .unpinned and
            (self.observed_start.cpu != self.chosen.id or self.observed_end.cpu != self.chosen.id))
            return error.AffinityMismatch;
        return result;
    }
};

const Coordinator = struct {
    config: Config,
    std_io: std.Io,
    workers: []Worker,
    go: std.Io.Event = .unset,

    fn entry(self: *Coordinator, loop: *velo.event.Loop, index: usize) !void {
        const worker = &self.workers[index];
        const cells = worker.cells();
        const started = std.Io.Clock.awake.now(self.std_io).nanoseconds;
        try verify(chase(cells, cells.len), .{ .accesses = cells.len, .index = 0, .checksum = cycleSum(cells.len) });
        worker.entry_warmup_ns = @intCast(std.Io.Clock.awake.now(self.std_io).nanoseconds - started);
        loop.executor.spawn(&worker.task);
    }
};

const Residency = struct {
    local: usize = 0,
    remote: usize = 0,
    unavailable: usize = 0,
    query_error: []const u8 = "none",
};

fn sampleResidency(worker: *const Worker) !Residency {
    var result: Residency = .{};
    const pages = worker.region.bytes.len / std.heap.pageSize();
    const samples = @min(pages, 16);
    for (0..samples) |i| {
        const page = i * (pages - 1) / @max(samples - 1, 1);
        const node = numa.pageNode(worker.region.bytes.ptr + page * std.heap.pageSize()) catch |err| {
            result.unavailable += 1;
            result.query_error = @errorName(err);
            continue;
        };
        if (worker.memory_node) |expected| {
            if (node != expected) return error.MemoryPlacementMismatch;
        }
        if (node == worker.observed_start.node) result.local += 1 else result.remote += 1;
    }
    return result;
}

const Smaps = struct { anon_huge_bytes: usize = 0, vma_bytes: usize = 0, region_bytes: usize = 0 };

fn parseSmaps(text: []const u8, workers: []const Worker) !Smaps {
    var result: Smaps = .{};
    var matched = false;
    var awaiting_huge = false;
    var vma_size: usize = 0;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.tokenizeAny(u8, line, " \t");
        const first = fields.next() orelse continue;
        if (std.mem.indexOfScalar(u8, first, '-')) |dash| {
            if (awaiting_huge) return error.InvalidSmaps;
            const start = std.fmt.parseInt(usize, first[0..dash], 16) catch return error.InvalidSmaps;
            const end = std.fmt.parseInt(usize, first[dash + 1 ..], 16) catch return error.InvalidSmaps;
            if (end <= start) return error.InvalidSmaps;
            matched = false;
            for (workers) |*worker| {
                const base = @intFromPtr(worker.region.bytes.ptr);
                const overlap_start = @max(start, base);
                const overlap_end = @min(end, base + worker.region.bytes.len);
                if (overlap_start < overlap_end) {
                    matched = true;
                    result.region_bytes += overlap_end - overlap_start;
                }
            }
            vma_size = end - start;
            if (matched) result.vma_bytes += vma_size;
            awaiting_huge = matched;
        } else if (matched and std.mem.eql(u8, first, "AnonHugePages:")) {
            if (!awaiting_huge) return error.InvalidSmaps;
            const kib = std.fmt.parseInt(usize, fields.next() orelse return error.InvalidSmaps, 10) catch return error.InvalidSmaps;
            if (!std.mem.eql(u8, fields.next() orelse return error.InvalidSmaps, "kB") or kib > vma_size / 1024)
                return error.InvalidSmaps;
            // Adjacent mappings can merge, so count each whole overlapping VMA only once.
            result.anon_huge_bytes += kib * 1024;
            awaiting_huge = false;
        }
    }
    if (awaiting_huge) return error.InvalidSmaps;
    var expected: usize = 0;
    for (workers) |*worker| expected += worker.region.bytes.len;
    if (result.region_bytes != expected) return error.MappingNotFound;
    return result;
}

const Summary = struct {
    accesses: usize = 0,
    elapsed_ns: u64 = 0,
    worker_elapsed_ns: u64 = 0,
    checksum: u64 = 0,
    allocation_ns: u64 = 0,
    initialization_ns: u64 = 0,
    total_entry_warmup_ns: u64 = 0,
    mapping_alignment: usize = 0,
    smaps: Smaps = .{},
    residency: Residency = .{},
};

fn run(allocator: std.mem.Allocator, std_io: std.Io, config: Config) !?Summary {
    if (builtin.os.tag != .linux) return error.Unsupported;
    var topology = try numa.Topology.discover(allocator, std_io);
    defer topology.deinit();
    if (config.mode == .remote) {
        var permitted: usize = 0;
        for (topology.nodes) |node| permitted += @intFromBool(node.memory);
        if (permitted < 2) {
            if (!builtin.is_test) std.debug.print("SKIP mode=remote workload=memory cross_node_requests=0 reason=no_different_permitted_memory_node permitted_memory_nodes={d}\n", .{permitted});
            return null;
        }
    }
    // Keep the chosen CPUs comparable across modes, including restricted memory-node sets.
    const placement = try numa.plan(allocator, &topology, .{ .count = config.threads, .memory = .bind });
    defer allocator.free(placement);
    const options = try preparationOptions(allocator, std_io, config.preparation);
    var summary: Summary = .{ .mapping_alignment = @max(options.alignment, std.heap.pageSize()) };

    const workers = try allocator.alloc(Worker, config.threads);
    defer allocator.free(workers);
    var coordinator: Coordinator = .{ .config = config, .std_io = std_io, .workers = workers };
    var initialized: usize = 0;
    defer for (workers[0..initialized]) |*worker| worker.region.deinit();
    for (workers, placement) |*worker, *place| {
        const cpu = topology.cpu(place.cpu.?).?;
        const memory_node: ?usize = switch (config.mode) {
            .unpinned, .pinned => null,
            .local => cpu.node,
            .remote => remoteNode(&topology, cpu.node).?,
        };
        const allocation_started = std.Io.Clock.awake.now(std_io).nanoseconds;
        var region = try allocate(config.memory_mib * mib, memory_node, options);
        errdefer region.deinit();
        summary.allocation_ns += @intCast(std.Io.Clock.awake.now(std_io).nanoseconds - allocation_started);
        const cells = @as([*]Cell, @ptrCast(@alignCast(region.bytes.ptr)))[0 .. region.bytes.len / @sizeOf(Cell)];
        // Fresh baseline mappings are first-touched here, never by the workers.
        const initialization_started = std.Io.Clock.awake.now(std_io).nanoseconds;
        const expected = try initialize(allocator, cells, config.iterations);
        summary.initialization_ns += @intCast(std.Io.Clock.awake.now(std_io).nanoseconds - initialization_started);
        worker.* = .{ .coordinator = &coordinator, .chosen = cpu, .memory_node = memory_node, .region = region, .expected = expected };
        initialized += 1;
        place.* = .{ .cpu = if (config.mode == .unpinned) null else cpu.id };
    }

    var group: Group = undefined;
    try group.init(allocator, .{ .placement = placement, .loop = .{ .entries = 8 } }, &coordinator, Coordinator.entry);
    defer {
        group.shutdown();
        group.deinit();
    }
    // Group's entry barrier has completed every allocation, initialization and warmup.
    coordinator.go.set(std_io);
    for (workers) |*worker| worker.done.waitUncancelable(std_io);

    const smaps = try readText(allocator, std_io, "/proc/self/smaps", .limited(16 * mib));
    defer allocator.free(smaps);
    summary.smaps = try parseSmaps(smaps, workers);
    var first: i128 = std.math.maxInt(i128);
    var last: i128 = 0;
    for (workers, 0..) |*worker, i| {
        const result = try worker.result;
        const elapsed: u64 = @intCast(worker.finished_ns - worker.started_ns);
        if (elapsed == 0) return error.InvalidTiming;
        first = @min(first, worker.started_ns);
        last = @max(last, worker.finished_ns);
        summary.accesses += result.accesses;
        summary.worker_elapsed_ns += elapsed;
        summary.checksum +%= result.checksum;
        summary.total_entry_warmup_ns += worker.entry_warmup_ns;
        const residency = try sampleResidency(worker);
        summary.residency.local += residency.local;
        summary.residency.remote += residency.remote;
        summary.residency.unavailable += residency.unavailable;
        if (!builtin.is_test) std.debug.print("worker={d} chosen_cpu={d} chosen_node={d} pinned={} observed_cpu_start={d} observed_cpu_end={d} observed_node_start={d} observed_node_end={d} memory_policy={s} requested_memory_node={?d} residency_reference_node={d} residency_local_samples={d} residency_remote_samples={d} residency_unavailable_samples={d} residency_query_error={s} accesses={d} final_index={d} checksum={d} elapsed_ns={d} ns/access={d:.3} entry_warmup_ns={d}\n", .{
            i,                                                       worker.chosen.id,        worker.chosen.node,         config.mode != .unpinned,
            worker.observed_start.cpu,                               worker.observed_end.cpu, worker.observed_start.node, worker.observed_end.node,
            if (worker.memory_node == null) "inherited" else "bind", worker.memory_node,      worker.observed_start.node, residency.local,
            residency.remote,                                        residency.unavailable,   residency.query_error,      result.accesses,
            result.index,                                            result.checksum,         elapsed,                    @as(f64, @floatFromInt(elapsed)) / @as(f64, @floatFromInt(result.accesses)),
            worker.entry_warmup_ns,
        });
    }
    summary.elapsed_ns = @intCast(last - first);
    if (summary.accesses != config.threads * config.iterations) return error.IncorrectTraversal;
    return summary;
}

pub fn main(init: std.process.Init) !void {
    if (builtin.os.tag != .linux) {
        std.debug.print("numa: unsupported on this platform (Linux only)\n", .{});
        return error.Unsupported;
    }
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const config = Config.parse(args[1..]) catch |err| {
        std.debug.print("usage: numa <unpinned|pinned|local|remote> [threads:1..64] [MiB-per-thread:1..4096] [iterations:1..1000000000] [lazy|prefault|huge] (total memory <=4096 MiB)\nprep defaults to lazy; unpinned/pinned reject prefault/huge. Prefault is outside steady timing; initialization already writes every page. Huge requests a THP hint, not guaranteed huge pages.\n", .{});
        return err;
    };
    const result = (try run(init.gpa, init.io, config)) orelse return;
    const count: f64 = @floatFromInt(result.accesses);
    std.debug.print("preparation={s} allocation_ns={d} allocation_includes_preparation=true initialization_ns={d} initialization_writes_all_pages=true total_entry_warmup_ns={d} warmup_aggregation=sum_workers mapping_alignment_bytes={d} thp_hint_requested={} smaps_anon_huge_bytes={d} smaps_vma_bytes={d} smaps_region_bytes={d} smaps_granularity=whole_overlapping_vmas smaps_snapshot=post_run\n", .{
        @tagName(config.preparation), result.allocation_ns,        result.initialization_ns,     result.total_entry_warmup_ns,
        result.mapping_alignment,     config.preparation == .huge, result.smaps.anon_huge_bytes, result.smaps.vma_bytes,
        result.smaps.region_bytes,
    });
    std.debug.print("mode={s} workload=memory cross_node_requests=0 threads={d} MiB_per_thread={d} iterations_per_thread={d} cell_bytes={d} seed={d} accesses={d} checksum={d} elapsed_ns={d} accesses_per_second={d:.0} aggregate_elapsed_ns/access={d:.3} mean_worker_ns/access={d:.3} residency_local_samples={d} residency_remote_samples={d} residency_unavailable_samples={d}\n", .{
        @tagName(config.mode),  config.threads,          config.memory_mib,            config.iterations,                                                  @sizeOf(Cell),                                      seed,
        result.accesses,        result.checksum,         result.elapsed_ns,            count * 1_000_000_000 / @as(f64, @floatFromInt(result.elapsed_ns)), @as(f64, @floatFromInt(result.elapsed_ns)) / count, @as(f64, @floatFromInt(result.worker_elapsed_ns)) / count,
        result.residency.local, result.residency.remote, result.residency.unavailable,
    });
}

test "random dependent cycle visits every cache-line cell and checks partial traversals" {
    try std.testing.expectEqual(line_bytes, @sizeOf(Cell));
    var cells: [257]Cell = undefined;
    for ([_]usize{ 1, 256, 257, 258, 1000 }) |iterations| {
        const expected = try initialize(std.testing.allocator, &cells, iterations);
        try verify(chase(&cells, iterations), expected);
        var seen: [257]bool = @splat(false);
        var index: usize = 0;
        for (0..cells.len) |_| {
            try std.testing.expect(!seen[index]);
            seen[index] = true;
            index = cells[index].next;
        }
        try std.testing.expectEqual(0, index);
        const whole = chase(&cells, cells.len);
        try std.testing.expectEqual(cells.len, whole.accesses);
        try std.testing.expectEqual(cycleSum(cells.len), whole.checksum);
        var wrong = expected;
        wrong.accesses += 1;
        try std.testing.expectError(error.IncorrectTraversal, verify(chase(&cells, iterations), wrong));
    }
}

test "CLI bounds argument count, mode, memory and work" {
    try std.testing.expectError(error.Usage, Config.parse(&.{}));
    try std.testing.expectError(error.Usage, Config.parse(&.{ "local", "1", "1", "1", "lazy", "extra" }));
    try std.testing.expectError(error.InvalidMode, Config.parse(&.{"invalid"}));
    for ([_][]const u8{ "0", "-1", "+1", "abc", "", "184467440737095516160" }) |bad| {
        try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "local", bad }));
        try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "local", "1", bad }));
        try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "local", "1", "1", bad }));
    }
    try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "pinned", "65" }));
    try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "pinned", "1", "4097" }));
    try std.testing.expectError(error.InvalidNumber, Config.parse(&.{ "pinned", "1", "1", "1000000001" }));
    try std.testing.expectError(error.MemoryLimit, Config.parse(&.{ "pinned", "64", "65" }));
    const defaults = try Config.parse(&.{"unpinned"});
    try std.testing.expectEqual(2, defaults.threads);
    try std.testing.expectEqual(64, defaults.memory_mib);
    try std.testing.expectEqual(1_000_000, defaults.iterations);
    try std.testing.expectEqual(Preparation.lazy, defaults.preparation);
    _ = try Config.parse(&.{ "remote", "64", "64", "1000000000" });
    try std.testing.expectError(error.InvalidPreparation, Config.parse(&.{ "local", "1", "1", "1", "invalid" }));
    for ([_][]const u8{ "unpinned", "pinned", "local", "remote" }) |mode| {
        const lazy = try Config.parse(&.{ mode, "1", "1", "1", "lazy" });
        try std.testing.expectEqual(Preparation.lazy, lazy.preparation);
        for ([_][]const u8{ "prefault", "huge" }) |preparation| {
            if (lazy.mode == .unpinned or lazy.mode == .pinned) {
                try std.testing.expectError(error.UnsupportedPreparationMode, Config.parse(&.{ mode, "1", "1", "1", preparation }));
            } else {
                const config = try Config.parse(&.{ mode, "1", "1", "1", preparation });
                try std.testing.expectEqualStrings(preparation, @tagName(config.preparation));
            }
        }
    }
}

test "remote memory selection excludes the CPU node and forbidden memory nodes" {
    var topology: numa.Topology = .{
        .cpus = &.{},
        .nodes = &.{
            .{ .id = 1, .memory = false, .distances = &.{} },
            .{ .id = 2, .memory = true, .distances = &.{} },
            .{ .id = 4, .memory = true, .distances = &.{} },
            .{ .id = 8, .memory = false, .distances = &.{} },
        },
        .arena = .init(std.testing.allocator),
    };
    defer topology.deinit();
    try std.testing.expectEqual(4, remoteNode(&topology, 2).?);
    try std.testing.expectEqual(2, remoteNode(&topology, 4).?);
    topology.nodes = topology.nodes[0..2];
    try std.testing.expectEqual(null, remoteNode(&topology, 2));
}

test "preparation options are opt-in and PMD alignment accepts host sizes" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    try std.testing.expectEqualDeep(numa.Region.Options{}, try preparationOptions(allocator, io, .lazy));
    try std.testing.expectEqualDeep(numa.Region.Options{ .prefault = true }, try preparationOptions(allocator, io, .prefault));
    var text: [32]u8 = undefined;
    const size = 512 * std.heap.pageSize();
    try std.testing.expectEqual(size, try pmdSize(try std.fmt.bufPrint(&text, "{d}\n", .{size})));
    try std.testing.expectEqual(16 * size, try pmdSize(try std.fmt.bufPrint(&text, "{d}", .{16 * size})));
    for ([_][]const u8{ "", "0", "3", "invalid", "184467440737095516160" }) |bad| {
        try std.testing.expectError(error.Unsupported, pmdSize(bad));
    }
    try std.testing.expectError(error.Unsupported, pmdSize(try std.fmt.bufPrint(&text, "{d}", .{std.heap.pageSize()})));
    if (builtin.os.tag == .linux) {
        try std.testing.expectError(error.UnsupportedPreparationMode, allocate(mib, null, .{ .prefault = true }));
        try std.testing.expectError(error.UnsupportedPreparationMode, allocate(mib, null, .{ .huge_pages = true }));
    }
}

test "smaps counts merged and partially overlapping VMAs once and requires full coverage" {
    var workers: [3]Worker = undefined;
    for (&workers, [_]usize{ 0x20000, 0x30000, 0x100000 }) |*worker, address| {
        worker.region = .{ .bytes = @as([*]align(std.heap.page_size_min) u8, @ptrFromInt(address))[0..0x10000] };
    }
    const text =
        \\10000-60000 rw-p 00000000 00:00 0
        \\AnonHugePages: 8 kB
        \\80000-a0000 rw-p 00000000 00:00 0
        \\AnonHugePages: 8 kB
        \\100000-110000 rw-p 00000000 00:00 0
        \\AnonHugePages: 0 kB
        \\
    ;
    try std.testing.expectEqualDeep(Smaps{ .anon_huge_bytes = 8 * 1024, .vma_bytes = 0x60000, .region_bytes = 0x30000 }, try parseSmaps(text, &workers));
    try std.testing.expectError(error.MappingNotFound, parseSmaps("", &workers));
    try std.testing.expectError(error.InvalidSmaps, parseSmaps("10000-60000 rw-p\n", &workers));
    try std.testing.expectError(error.InvalidSmaps, parseSmaps("10000-60000 rw-p\nAnonHugePages: 321 kB\n", &workers));
    try std.testing.expectError(error.InvalidSmaps, parseSmaps("10000-60000 rw-p\nAnonHugePages: 8 MB\n", &workers));
    try std.testing.expectError(error.InvalidSmaps, parseSmaps("10000-60000 rw-p\nAnonHugePages: 8 kB\nAnonHugePages: 8 kB\n", &workers));
}

test "Group smoke verifies memory traversal and completion counts" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const config: Config = .{ .mode = .unpinned, .threads = 1, .memory_mib = 1, .iterations = 1003 };
    const result = (try run(std.testing.allocator, std.testing.io, config)).?;
    try std.testing.expectEqual(config.iterations, result.accesses);
    try std.testing.expect(result.elapsed_ns > 0);
    try std.testing.expectEqual(result.elapsed_ns, result.worker_elapsed_ns);
    try std.testing.expect(result.allocation_ns > 0);
    try std.testing.expect(result.initialization_ns > 0);
    try std.testing.expect(result.total_entry_warmup_ns > 0);
    try std.testing.expectEqual(mib, result.smaps.region_bytes);
    try std.testing.expectEqual(16, result.residency.local + result.residency.remote + result.residency.unavailable);
}

test "Group bound optional preparations smoke with explicit capability skips" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    for ([_]Preparation{ .prefault, .huge }) |preparation| {
        for ([_]Mode{ .local, .remote }) |mode| {
            const config: Config = .{ .mode = mode, .threads = 1, .memory_mib = 1, .iterations = 1003, .preparation = preparation };
            const result = run(std.testing.allocator, std.testing.io, config) catch |err| switch (err) {
                error.Unsupported, error.PermissionDenied => {
                    std.debug.print("SKIP mode={s} preparation={s} capability_error={s}\n", .{ @tagName(mode), @tagName(preparation), @errorName(err) });
                    return error.SkipZigTest;
                },
                else => return err,
            };
            if (result) |summary| {
                try std.testing.expectEqual(config.iterations, summary.accesses);
                try std.testing.expect(summary.allocation_ns > 0);
                try std.testing.expect(summary.initialization_ns > 0);
                try std.testing.expect(summary.total_entry_warmup_ns > 0);
                try std.testing.expectEqual(mib, summary.smaps.region_bytes);
                try std.testing.expect(summary.smaps.anon_huge_bytes <= summary.smaps.vma_bytes);
                if (preparation == .huge) try std.testing.expect(summary.mapping_alignment > std.heap.pageSize());
            }
        }
    }
}

test "failed preparation drains a task without opening the benchmark gate" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var first = try allocate(mib, null, .{});
    defer first.deinit();
    var second = try allocate(mib, null, .{});
    defer second.deinit();
    var workers: [2]Worker = undefined;
    var coordinator: Coordinator = .{ .config = .{ .mode = .unpinned }, .std_io = std.testing.io, .workers = &workers };
    for (&workers, [_]numa.Region{ first, second }) |*worker, region| {
        worker.* = .{
            .coordinator = &coordinator,
            .chosen = .{ .id = 0, .node = 0, .core = 0 },
            .memory_node = null,
            .region = region,
            .expected = undefined,
        };
        worker.expected = try initialize(std.testing.allocator, worker.cells(), 10);
    }
    workers[1].cells()[0].next = 0;
    var group: Group = undefined;
    try std.testing.expectError(error.IncorrectTraversal, group.init(std.testing.allocator, .{
        .count = workers.len,
        .loop = .{ .entries = 8 },
    }, &coordinator, Coordinator.entry));
    try std.testing.expectEqual(.done, workers[0].task.state);
    try std.testing.expectError(error.Canceled, workers[0].result);
}
