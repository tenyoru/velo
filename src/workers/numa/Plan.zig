const std = @import("std");
const builtin = @import("builtin");
const numa = @import("root.zig");
const Topology = @import("Topology.zig");

pub const Placement = struct {
    cpu: ?usize = null,
    memory: ?numa.Policy = null,
};

pub const Options = struct {
    count: usize,
    memory: enum { none, bind, prefer } = .none,
};

/// Caller frees the plan. A topology spreads physical cores across nodes before SMT;
/// null selects sorted allowed CPUs without NUMA discovery and permits CPU-only plans.
pub fn init(allocator: std.mem.Allocator, topology: ?*const Topology, options: Options) ![]Placement {
    if (options.count == 0) return error.InvalidThreadCount;
    const snapshot = topology orelse {
        if (options.memory != .none) return error.InvalidPlacement;
        const cpus = try numa.allowedCpus(allocator);
        defer allocator.free(cpus);
        std.mem.sort(usize, cpus, {}, std.sort.asc(usize));
        if (options.count > cpus.len) return error.NotEnoughCpus;
        const placements = try allocator.alloc(Placement, options.count);
        for (placements, cpus[0..options.count]) |*placement, cpu| placement.* = .{ .cpu = cpu };
        return placements;
    };
    if (options.count > snapshot.cpus.len) return error.NotEnoughCpus;
    const storage = try allocator.alloc(Candidate, snapshot.cpus.len);
    defer allocator.free(storage);
    var count: usize = 0;
    for (snapshot.cpus) |cpu| {
        for (snapshot.nodes) |node| {
            if (node.id == cpu.node and (options.memory == .none or node.memory)) break;
        } else continue;
        storage[count] = .{ .cpu = cpu };
        count += 1;
    }
    if (options.count > count) return error.NotEnoughCpus;
    const candidates = storage[0..count];
    std.mem.sort(Candidate, candidates, {}, Candidate.byCore);
    for (candidates, 0..) |*candidate, i| {
        candidate.sibling = i != 0 and candidates[i - 1].cpu.core == candidate.cpu.core;
    }
    std.mem.sort(Candidate, candidates, {}, Candidate.byNode);
    for (candidates, 0..) |*candidate, i| {
        candidate.round = if (i != 0 and candidates[i - 1].cpu.node == candidate.cpu.node and
            candidates[i - 1].sibling == candidate.sibling) candidates[i - 1].round + 1 else 0;
    }
    std.mem.sort(Candidate, candidates, {}, Candidate.byRound);
    const placements = try allocator.alloc(Placement, options.count);
    for (placements, candidates[0..options.count]) |*placement, candidate| placement.* = .{
        .cpu = candidate.cpu.id,
        .memory = switch (options.memory) {
            .none => null,
            .bind => .{ .bind = candidate.cpu.node },
            .prefer => .{ .prefer = candidate.cpu.node },
        },
    };
    return placements;
}

const Candidate = struct {
    cpu: Topology.Cpu,
    sibling: bool = false,
    round: usize = 0,

    fn byCore(_: void, a: Candidate, b: Candidate) bool {
        if (a.cpu.core != b.cpu.core) return a.cpu.core < b.cpu.core;
        return a.cpu.id < b.cpu.id;
    }

    fn byNode(_: void, a: Candidate, b: Candidate) bool {
        if (a.sibling != b.sibling) return !a.sibling;
        if (a.cpu.node != b.cpu.node) return a.cpu.node < b.cpu.node;
        return a.cpu.id < b.cpu.id;
    }

    fn byRound(_: void, a: Candidate, b: Candidate) bool {
        if (a.sibling != b.sibling) return !a.sibling;
        if (a.round != b.round) return a.round < b.round;
        return a.cpu.node < b.cpu.node;
    }
};

fn checkAllocation(allocator: std.mem.Allocator, topology: ?*const Topology, options: Options) !void {
    const placements = try init(allocator, topology, options);
    defer allocator.free(placements);
}

test "NUMA plan round-robins sparse permitted nodes and physical cores before SMT" {
    const testing = std.testing;
    var topology: Topology = .{
        .arena = .init(testing.allocator),
        .nodes = &.{
            .{ .id = 19, .memory = false, .distances = &.{} },
            .{ .id = 9, .memory = true, .distances = &.{} },
            .{ .id = 4, .memory = true, .distances = &.{} },
            .{ .id = 1, .memory = true, .distances = &.{} },
            .{ .id = 12, .memory = false, .distances = &.{} },
        },
        .cpus = &.{
            .{ .id = 66, .node = 4, .core = 50 },
            .{ .id = 2048, .node = 19, .core = 2048 },
            .{ .id = 17, .node = 1, .core = 3 },
            .{ .id = 90, .node = 4, .core = 90 },
            .{ .id = 7, .node = 1, .core = 3 },
            .{ .id = 1025, .node = 12, .core = 1025 },
            .{ .id = 50, .node = 4, .core = 50 },
            .{ .id = 33, .node = 1, .core = 30 },
        },
    };
    defer topology.deinit();
    for ([_]@FieldType(Options, "memory"){ .none, .bind, .prefer }) |memory| {
        const expected: []const usize = if (memory == .none) &.{ 7, 50, 1025, 2048, 33, 90, 17, 66 } else &.{ 7, 50, 33, 90, 17, 66 };
        for (1..expected.len + 1) |count| {
            const options: Options = .{ .count = count, .memory = memory };
            const placements = try init(testing.allocator, &topology, options);
            defer testing.allocator.free(placements);
            try testing.expectEqual(count, placements.len);
            for (placements, expected[0..count]) |placement, id| {
                try testing.expectEqual(id, placement.cpu.?);
                const node = topology.cpu(id).?.node;
                switch (memory) {
                    .none => try testing.expectEqual(null, placement.memory),
                    .bind => try testing.expectEqualDeep(numa.Policy{ .bind = node }, placement.memory.?),
                    .prefer => try testing.expectEqualDeep(numa.Policy{ .prefer = node }, placement.memory.?),
                }
            }
        }
        try testing.expectError(error.NotEnoughCpus, init(testing.allocator, &topology, .{ .count = expected.len + 1, .memory = memory }));
        try testing.checkAllAllocationFailures(testing.allocator, checkAllocation, .{ &topology, Options{ .count = expected.len, .memory = memory } });
    }
    const node1 = try topology.nodeCpus(testing.allocator, 1);
    defer testing.allocator.free(node1);
    try testing.expectEqualSlices(usize, &.{ 7, 17, 33 }, node1);
    try testing.expectError(error.InvalidThreadCount, init(testing.allocator, &topology, .{ .count = 0 }));
    topology.cpus = &.{};
    try testing.expectError(error.NotEnoughCpus, init(testing.allocator, &topology, .{ .count = 1 }));
}

test "NUMA plan without topology uses sorted allowed CPUs and rejects memory policies" {
    const testing = std.testing;
    try testing.expectError(error.InvalidThreadCount, init(testing.allocator, null, .{ .count = 0 }));
    for ([_]@FieldType(Options, "memory"){ .bind, .prefer }) |memory| {
        try testing.expectError(error.InvalidPlacement, init(testing.allocator, null, .{ .count = 1, .memory = memory }));
    }
    if (builtin.os.tag != .linux and builtin.os.tag != .windows) {
        try testing.expectError(error.Unsupported, init(testing.allocator, null, .{ .count = 1 }));
        return;
    }
    const cpus = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(cpus);
    std.mem.sort(usize, cpus, {}, std.sort.asc(usize));
    const placements = try init(testing.allocator, null, .{ .count = cpus.len });
    defer testing.allocator.free(placements);
    for (placements, cpus) |placement, cpu| try testing.expectEqualDeep(Placement{ .cpu = cpu }, placement);
    try testing.expectError(error.NotEnoughCpus, init(testing.allocator, null, .{ .count = cpus.len + 1 }));
    try testing.checkAllAllocationFailures(testing.allocator, checkAllocation, .{ null, Options{ .count = cpus.len } });
}
