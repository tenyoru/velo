const std = @import("std");
const builtin = @import("builtin");
const numa = @import("root.zig");
const Topology = @This();

/// core is the lowest sibling CPU ID, unique across sockets unlike sysfs core_id.
pub const Cpu = struct { id: usize, node: usize, core: usize };
pub const Node = struct { id: usize, memory: bool, distances: []const usize };

cpus: []const Cpu,
/// All online physical nodes; memory also requires current cpuset permission.
/// Distance columns use this array's order, not node IDs as array indices.
nodes: []const Node,
arena: std.heap.ArenaAllocator,

pub fn discover(allocator: std.mem.Allocator, io: std.Io) !Topology {
    if (builtin.os.tag != .linux) return error.Unsupported;
    const affinity = try numa.allowedCpus(allocator);
    defer allocator.free(affinity);
    const system = try std.Io.Dir.openDirAbsolute(io, "/sys/devices/system", .{});
    defer system.close(io);
    // An Io backend can open files on a worker, so resolve the calling thread here.
    var status_path_buffer: [64]u8 = undefined;
    const status_path = try std.fmt.bufPrint(&status_path_buffer, "/proc/self/task/{d}/status", .{std.Thread.getCurrentId()});
    const status = try readText(allocator, io, .cwd(), status_path);
    defer allocator.free(status);
    return load(allocator, io, system, status, affinity);
}

pub fn deinit(self: *Topology) void {
    self.arena.deinit();
    self.* = undefined;
}

pub fn cpu(self: *const Topology, id: usize) ?Cpu {
    for (self.cpus) |value| if (value.id == id) return value;
    return null;
}

pub fn nodeCpus(self: *const Topology, allocator: std.mem.Allocator, node: usize) ![]usize {
    for (self.nodes) |value| {
        if (value.id == node) break;
    } else return error.InvalidNode;
    var count: usize = 0;
    for (self.cpus) |value| if (value.node == node) {
        count += 1;
    };
    if (count == 0) return error.EmptyCpuSet;
    const ids = try allocator.alloc(usize, count);
    var index: usize = 0;
    for (self.cpus) |value| if (value.node == node) {
        ids[index] = value.id;
        index += 1;
    };
    std.mem.sort(usize, ids, {}, std.sort.asc(usize));
    return ids;
}

fn load(allocator: std.mem.Allocator, io: std.Io, system: std.Io.Dir, status: []const u8, affinity: []const usize) !Topology {
    var arena: std.heap.ArenaAllocator = .init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    const allowed_memory = try allowedMemory(a, status);
    const online_cpus = try readList(a, io, system, "cpu/online");
    if (online_cpus.len == 0) return error.InvalidTopology;
    const node_dir = system.openDir(io, "node", .{}) catch |err| switch (err) {
        error.FileNotFound => return loadUma(&arena, io, system, allowed_memory, online_cpus, affinity),
        else => return err,
    };
    defer node_dir.close(io);
    const online_nodes = try readList(a, io, node_dir, "online");
    if (online_nodes.len == 0) return error.InvalidTopology;
    const memory_nodes = try readList(a, io, node_dir, "has_memory");
    const nodes = try a.alloc(Node, online_nodes.len);
    var cpus: std.ArrayList(Cpu) = .empty;
    for (online_nodes, nodes) |id, *node| {
        const distances = try readDistances(a, io, node_dir, try std.fmt.allocPrint(a, "node{d}/distance", .{id}));
        if (distances.len != nodes.len) return error.InvalidTopology;
        node.* = .{ .id = id, .memory = contains(memory_nodes, id) and contains(allowed_memory, id), .distances = distances };
        const ids = try readList(a, io, node_dir, try std.fmt.allocPrint(a, "node{d}/cpulist", .{id}));
        for (ids) |cpu_id| {
            if (!contains(online_cpus, cpu_id) or !contains(affinity, cpu_id)) continue;
            try cpus.append(a, .{ .id = cpu_id, .node = id, .core = try coreId(a, io, system, cpu_id) });
        }
    }
    std.mem.sort(Cpu, cpus.items, {}, struct {
        fn lessThan(_: void, lhs: Cpu, rhs: Cpu) bool {
            return lhs.id < rhs.id;
        }
    }.lessThan);
    var expected: usize = 0;
    for (online_cpus) |cpu_id| {
        if (contains(affinity, cpu_id)) expected += 1;
    }
    if (cpus.items.len != expected) return error.InvalidTopology;
    for (cpus.items, 0..) |value, i| {
        if (i != 0 and cpus.items[i - 1].id == value.id) return error.InvalidTopology;
    }
    const owned_cpus = try cpus.toOwnedSlice(a);
    return .{ .arena = arena, .cpus = owned_cpus, .nodes = nodes };
}

fn loadUma(arena: *std.heap.ArenaAllocator, io: std.Io, system: std.Io.Dir, allowed_memory: []const usize, online_cpus: []const usize, affinity: []const usize) !Topology {
    const a = arena.allocator();
    var cpus: std.ArrayList(Cpu) = .empty;
    for (online_cpus) |cpu_id| {
        if (!contains(affinity, cpu_id)) continue;
        try cpus.append(a, .{ .id = cpu_id, .node = 0, .core = try coreId(a, io, system, cpu_id) });
    }
    const nodes = try a.alloc(Node, 1);
    nodes[0] = .{ .id = 0, .memory = contains(allowed_memory, 0), .distances = try a.dupe(usize, &.{10}) };
    const owned_cpus = try cpus.toOwnedSlice(a);
    return .{ .arena = arena.*, .cpus = owned_cpus, .nodes = nodes };
}

fn coreId(a: std.mem.Allocator, io: std.Io, system: std.Io.Dir, cpu_id: usize) !usize {
    const siblings = try readList(a, io, system, try std.fmt.allocPrint(a, "cpu/cpu{d}/topology/thread_siblings_list", .{cpu_id}));
    if (!contains(siblings, cpu_id)) return error.InvalidTopology;
    return siblings[0];
}

fn readList(a: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8) ![]usize {
    const text = try readText(a, io, dir, path);
    defer a.free(text);
    return parseList(a, text);
}

fn readDistances(a: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8) ![]usize {
    const text = try readText(a, io, dir, path);
    defer a.free(text);
    var values: std.ArrayList(usize) = .empty;
    errdefer values.deinit(a);
    var tokens = std.mem.tokenizeAny(u8, text, " \t\r\n");
    while (tokens.next()) |token| {
        const distance = try number(token);
        if (distance == 0) return error.InvalidTopology;
        try values.append(a, distance);
    }
    return values.toOwnedSlice(a);
}

fn readText(a: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, path: []const u8) ![]u8 {
    const file = try dir.openFile(io, path, .{});
    defer file.close(io);
    var reader = file.readerStreaming(io, &.{});
    // procfs reports size zero; size-based sendFile optimizations would read nothing.
    reader.mode = .streaming_simple;
    return reader.interface.allocRemaining(a, .unlimited) catch |err| switch (err) {
        error.ReadFailed => return reader.err.?,
        else => return err,
    };
}

fn allowedMemory(a: std.mem.Allocator, status: []const u8) ![]usize {
    var lines = std.mem.splitScalar(u8, status, '\n');
    var found: ?[]usize = null;
    errdefer if (found) |ids| a.free(ids);
    while (lines.next()) |line| {
        const prefix = "Mems_allowed_list:";
        if (!std.mem.startsWith(u8, line, prefix)) continue;
        if (found != null) return error.InvalidTopology;
        found = try parseList(a, line[prefix.len..]);
    }
    return found orelse error.InvalidTopology;
}

fn parseList(a: std.mem.Allocator, text: []const u8) ![]usize {
    var values: std.ArrayList(usize) = .empty;
    errdefer values.deinit(a);
    const trimmed = std.mem.trim(u8, text, " \t\r\n");
    if (trimmed.len == 0) return values.toOwnedSlice(a);
    var ranges = std.mem.splitScalar(u8, trimmed, ',');
    while (ranges.next()) |range| {
        var bounds = std.mem.splitScalar(u8, range, '-');
        const first = try number(bounds.next().?);
        const last = if (bounds.next()) |end| try number(end) else first;
        if (bounds.next() != null or last < first) return error.InvalidTopology;
        if (values.items.len != 0 and first <= values.items[values.items.len - 1]) return error.InvalidTopology;
        const count = std.math.add(usize, last - first, 1) catch return error.InvalidTopology;
        try values.ensureUnusedCapacity(a, count);
        var id = first;
        while (true) {
            values.appendAssumeCapacity(id);
            if (id == last) break;
            id += 1;
        }
    }
    return values.toOwnedSlice(a);
}

fn number(text: []const u8) !usize {
    if (text.len == 0) return error.InvalidTopology;
    for (text) |c| if (c < '0' or c > '9') return error.InvalidTopology;
    return std.fmt.parseInt(usize, text, 10) catch error.InvalidTopology;
}

fn contains(ids: []const usize, id: usize) bool {
    return std.mem.indexOfScalar(usize, ids, id) != null;
}

fn fixtureFile(dir: std.Io.Dir, path: []const u8, text: []const u8) !void {
    const io = std.testing.io;
    if (std.fs.path.dirname(path)) |parent| try dir.createDirPath(io, parent);
    try dir.writeFile(io, .{ .sub_path = path, .data = text });
}

fn fixtureLoad(a: std.mem.Allocator, dir: std.Io.Dir, status: []const u8, affinity: []const usize) !void {
    var topology = try load(a, std.testing.io, dir, status, affinity);
    defer topology.deinit();
    const cpus = try topology.nodeCpus(a, topology.cpus[0].node);
    defer a.free(cpus);
}

test "NUMA topology sparse nodes, large CPU IDs, restricted masks and sibling keys" {
    const testing = std.testing;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try fixtureFile(tmp.dir, "cpu/online", "3,7,1025,2048\n");
    try fixtureFile(tmp.dir, "node/online", "1,4,9\n");
    try fixtureFile(tmp.dir, "node/has_memory", "1,4\n");
    try fixtureFile(tmp.dir, "node/node1/cpulist", "3,7\n");
    try fixtureFile(tmp.dir, "node/node4/cpulist", "\n");
    try fixtureFile(tmp.dir, "node/node9/cpulist", "1025,2048,3000\n");
    try fixtureFile(tmp.dir, "node/node1/distance", "10 31 42\n");
    try fixtureFile(tmp.dir, "node/node4/distance", "31 10 21\n");
    try fixtureFile(tmp.dir, "node/node9/distance", "42 21 10\n");
    try fixtureFile(tmp.dir, "cpu/cpu7/topology/thread_siblings_list", "3,7\n");
    try fixtureFile(tmp.dir, "cpu/cpu1025/topology/thread_siblings_list", "1025,2048\n");
    const status = "Name:\tfixture\nMems_allowed_list:\t4,9\n";
    var topology = try load(testing.allocator, testing.io, tmp.dir, status, &.{ 7, 1025, 3000 });
    defer topology.deinit();
    try testing.expectEqualSlices(Cpu, &.{ .{ .id = 7, .node = 1, .core = 3 }, .{ .id = 1025, .node = 9, .core = 1025 } }, topology.cpus);
    try testing.expectEqual(@as(usize, 3), topology.nodes.len);
    try testing.expectEqual(@as(usize, 1), topology.nodes[0].id);
    try testing.expect(!topology.nodes[0].memory);
    try testing.expectEqual(@as(usize, 4), topology.nodes[1].id);
    try testing.expect(topology.nodes[1].memory);
    try testing.expectEqual(@as(usize, 9), topology.nodes[2].id);
    try testing.expect(!topology.nodes[2].memory);
    try testing.expectEqualSlices(usize, &.{ 10, 31, 42 }, topology.nodes[0].distances);
    try testing.expectEqualSlices(usize, &.{ 31, 10, 21 }, topology.nodes[1].distances);
    try testing.expectEqualSlices(usize, &.{ 42, 21, 10 }, topology.nodes[2].distances);
    try testing.expectEqualDeep(topology.cpus[0], topology.cpu(7).?);
    try testing.expectEqualDeep(topology.cpus[1], topology.cpu(1025).?);
    try testing.expectEqual(null, topology.cpu(3));
    try testing.expectEqual(null, topology.cpu(2048));
    const node1 = try topology.nodeCpus(testing.allocator, 1);
    defer testing.allocator.free(node1);
    try testing.expectEqualSlices(usize, &.{7}, node1);
    const node9 = try topology.nodeCpus(testing.allocator, 9);
    defer testing.allocator.free(node9);
    try testing.expectEqualSlices(usize, &.{1025}, node9);
    try testing.expectError(error.EmptyCpuSet, topology.nodeCpus(testing.allocator, 4));
    try testing.expectError(error.InvalidNode, topology.nodeCpus(testing.allocator, 0));
    try testing.checkAllAllocationFailures(testing.allocator, fixtureLoad, .{ tmp.dir, status, &.{ 7, 1025, 3000 } });

    try fixtureFile(tmp.dir, "node/node4/distance", "31 10\n");
    try testing.expectError(error.InvalidTopology, load(testing.allocator, testing.io, tmp.dir, status, &.{7}));
    try fixtureFile(tmp.dir, "node/node4/distance", "31 10 21\n");
    try fixtureFile(tmp.dir, "cpu/cpu7/topology/thread_siblings_list", "3\n");
    try testing.expectError(error.InvalidTopology, load(testing.allocator, testing.io, tmp.dir, status, &.{7}));
    try fixtureFile(tmp.dir, "node/online", "1,,4\n");
    try testing.expectError(error.InvalidTopology, load(testing.allocator, testing.io, tmp.dir, status, &.{7}));
}

test "NUMA topology fallback requires absent node tree, not missing or corrupt files" {
    const testing = std.testing;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    try fixtureFile(tmp.dir, "cpu/online", "0-1,2048\n");
    try fixtureFile(tmp.dir, "cpu/cpu2048/topology/thread_siblings_list", "0,2048\n");
    var topology = try load(testing.allocator, testing.io, tmp.dir, "Mems_allowed_list:\t0\n", &.{2048});
    defer topology.deinit();
    try testing.expectEqualSlices(Cpu, &.{.{ .id = 2048, .node = 0, .core = 0 }}, topology.cpus);
    try testing.expect(topology.nodes[0].memory);
    try testing.expectEqualSlices(usize, &.{10}, topology.nodes[0].distances);
    try testing.checkAllAllocationFailures(testing.allocator, fixtureLoad, .{ tmp.dir, "Mems_allowed_list:\t0\n", &.{2048} });
    try testing.expectError(error.FileNotFound, load(testing.allocator, testing.io, tmp.dir, "Mems_allowed_list:\t0\n", &.{1}));
    try tmp.dir.createDir(testing.io, "node", .default_dir);
    try testing.expectError(error.FileNotFound, load(testing.allocator, testing.io, tmp.dir, "Mems_allowed_list:\t0\n", &.{2048}));
    try fixtureFile(tmp.dir, "node/online", "broken\n");
    try testing.expectError(error.InvalidTopology, load(testing.allocator, testing.io, tmp.dir, "Mems_allowed_list:\t0\n", &.{2048}));
}

test "NUMA list and status parsers reject malformed input" {
    const testing = std.testing;
    const valid = try parseList(testing.allocator, "0-2,1024,4096-4097\n");
    defer testing.allocator.free(valid);
    try testing.expectEqualSlices(usize, &.{ 0, 1, 2, 1024, 4096, 4097 }, valid);
    for ([_][]const u8{ "1,,2", "2-1", "1,1", "1-2-3", "-1", "+1", "0,", "1 2", "184467440737095516160" }) |text| {
        try testing.expectError(error.InvalidTopology, parseList(testing.allocator, text));
    }
    try testing.expectError(error.InvalidTopology, allowedMemory(testing.allocator, "Name:\ttest\n"));
    try testing.expectError(error.InvalidTopology, allowedMemory(testing.allocator, "Mems_allowed_list:\t0\nMems_allowed_list:\t1\n"));
}

test "NUMA discover includes online nodes and effective CPUs" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const testing = std.testing;
    var topology = try discover(testing.allocator, testing.io);
    defer topology.deinit();
    const affinity = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(affinity);
    try testing.expect(topology.nodes.len != 0);
    try testing.expectEqual(affinity.len, topology.cpus.len);
    for (topology.cpus, affinity) |value, id| try testing.expectEqual(id, value.id);
    for (topology.nodes) |node| try testing.expectEqual(topology.nodes.len, node.distances.len);
}
