//! Explicit worker CPU and user-space memory placement.
//! Policies do not place kernel-owned I/O memory or migrate existing application allocations.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;

pub const Topology = @import("Topology.zig");
pub const Placement = @import("Plan.zig").Placement;
pub const plan = @import("Plan.zig").init;
pub const PageAllocator = @import("PageAllocator.zig");

/// Physical node IDs, never cpuset-relative. Both modes require an allowed memory
/// node; prefer permits fallback under allocation pressure, bind does not.
/// Validation is a snapshot: concurrent cpuset/hotplug changes can invalidate it.
pub const Policy = union(enum) {
    bind: usize,
    prefer: usize,
};

/// Private anonymous pages, with policy installed before any access to the mapping.
/// Writes allocate pages; reads can use the shared zero page. No migration is
/// performed, and even bind cannot guarantee future faults succeed.
/// Release all borrows, including I/O registrations, before deinit.
pub const Region = struct {
    bytes: []align(std.heap.page_size_min) u8,

    pub const Options = struct {
        alignment: usize = std.heap.page_size_min,
        /// Populates writable pages once, not mlock; reclaim and OOM killing remain possible.
        /// Requires Linux 5.14+. Failure can partially populate pages, which are then unmapped.
        prefault: bool = false,
        /// Transparent huge-page hint only; success does not guarantee huge pages.
        huge_pages: bool = false,
    };

    pub fn init(len: usize, policy: Policy) !Region {
        return initWithOptions(len, policy, .{});
    }

    pub fn initWithOptions(len: usize, policy: Policy, options: Options) !Region {
        if (builtin.os.tag != .linux) return error.Unsupported;
        if (options.alignment == 0 or !std.math.isPowerOfTwo(options.alignment)) return error.InvalidAlignment;
        const page_size = std.heap.pageSize();
        const size = try roundedLength(len, page_size);
        const alignment = @max(options.alignment, page_size);
        const reserved_size = try std.math.add(usize, size, alignment - page_size);
        const allocator = std.heap.page_allocator;
        const mask = try policyMask(allocator, policy);
        defer allocator.free(mask);

        // MCL_FUTURE can populate writable mappings before mmap returns.
        const address = try checked(linux.mmap(null, reserved_size, .{}, .{
            .TYPE = .PRIVATE,
            .ANONYMOUS = true,
        }, -1, 0));
        var bytes = @as([*]align(std.heap.page_size_min) u8, @ptrFromInt(address))[0..reserved_size];
        errdefer std.posix.munmap(bytes);
        const aligned_address = (try std.math.add(usize, address, alignment - 1)) & ~(alignment - 1);
        const prefix = aligned_address - address;
        if (prefix != 0) {
            std.posix.munmap(bytes[0..prefix]);
            bytes = @alignCast(bytes[prefix..]);
        }
        if (bytes.len > size) {
            std.posix.munmap(@alignCast(bytes[size..]));
            bytes = bytes[0..size];
        }
        _ = try checked(linux.syscall6(.mbind, aligned_address, size, mode(policy), @intFromPtr(mask.ptr), policyEnd(mask), 0));
        if (options.huge_pages) try advise(bytes, linux.MADV.HUGEPAGE);
        _ = try checked(linux.mprotect(bytes.ptr, size, .{ .READ = true, .WRITE = true }));
        if (options.prefault) try advise(bytes, madv_populate_write);
        return .{ .bytes = bytes };
    }

    pub fn deinit(self: *Region) void {
        if (builtin.os.tag == .linux) std.posix.munmap(self.bytes);
        self.* = undefined;
    }
};

/// Changes only the calling thread. Use on dedicated threads: verification or
/// allocation errors after sched_setaffinity can leave the affinity changed.
/// Windows supports only the current processor group's 64-bit process mask.
pub fn pin(cpus: []const usize) !void {
    if (builtin.os.tag == .windows) {
        if (@bitSizeOf(usize) != 64) return error.Unsupported;
        if (cpus.len == 0) return error.EmptyCpuSet;
        var requested: usize = 0;
        for (cpus) |cpu| {
            if (cpu >= word_bits) return error.InvalidCpu;
            requested |= @as(usize, 1) << @intCast(cpu);
        }
        if (requested & ~try windowsAffinityMask() != 0) return error.InvalidCpuSet;
        const win32 = @import("../../event/backends/win32.zig");
        if (win32.SetThreadAffinityMask(std.os.windows.GetCurrentThread(), requested) == 0) return error.Unexpected;
        return;
    }
    if (builtin.os.tag != .linux) return error.Unsupported;
    const allocator = std.heap.page_allocator;
    const requested = try cpuMask(allocator, cpus);
    defer allocator.free(requested);
    _ = try checked(linux.syscall3(.sched_setaffinity, 0, requested.len * @sizeOf(usize), @intFromPtr(requested.ptr)));
    const effective = try affinityMask(allocator);
    defer allocator.free(effective);
    for (0..@max(requested.len, effective.len)) |i| {
        const want = if (i < requested.len) requested[i] else 0;
        const got = if (i < effective.len) effective[i] else 0;
        if (want != got) return error.AffinityMismatch;
    }
}

/// Sorted effective thread CPU IDs on Linux, or process CPU IDs in the current
/// processor group on 64-bit Windows. Caller frees the slice.
pub fn allowedCpus(allocator: std.mem.Allocator) ![]usize {
    if (builtin.os.tag == .windows) {
        var mask = try windowsAffinityMask();
        const cpus = try allocator.alloc(usize, @popCount(mask));
        for (cpus) |*cpu| {
            cpu.* = @ctz(mask);
            mask &= mask - 1;
        }
        return cpus;
    }
    if (builtin.os.tag != .linux) return error.Unsupported;
    const mask = try affinityMask(allocator);
    defer allocator.free(mask);
    var cpus: std.ArrayList(usize) = .empty;
    errdefer cpus.deinit(allocator);
    for (mask, 0..) |word, i| {
        var remaining = word;
        while (remaining != 0) {
            const bit = @ctz(remaining);
            try cpus.append(allocator, i * word_bits + bit);
            remaining &= remaining - 1;
        }
    }
    return cpus.toOwnedSlice(allocator);
}

/// Sets the calling thread's default policy, inherited by subsequently created
/// threads. Use on dedicated threads; this API does not restore a previous policy.
pub fn setPolicy(policy: Policy) !void {
    if (builtin.os.tag != .linux) return error.Unsupported;
    const allocator = std.heap.page_allocator;
    const mask = try policyMask(allocator, policy);
    defer allocator.free(mask);
    _ = try checked(linux.syscall3(.set_mempolicy, mode(policy), @intFromPtr(mask.ptr), policyEnd(mask)));
}

/// Queries an aligned, already populated page without faulting it in or moving it.
pub fn pageNode(address: [*]const u8) !usize {
    if (builtin.os.tag != .linux) return error.Unsupported;
    if (@intFromPtr(address) % std.heap.pageSize() != 0) return error.InvalidAlignment;
    const pages = [_][*]const u8{address};
    var status: i32 = undefined;
    _ = try checked(linux.syscall6(.move_pages, 0, 1, @intFromPtr(&pages), 0, @intFromPtr(&status), 0));
    if (status < 0) return errnoError(@fromBackingInt(@intCast(-@as(i64, status))));
    return @intCast(status);
}

const word_bits = @bitSizeOf(usize);
const static_nodes = 1 << 15;
// Zig 0.17's linux.MADV omits MADV_POPULATE_WRITE (Linux 5.14).
const madv_populate_write = 23;

fn policyEnd(mask: []const usize) usize {
    // Linux get_nodes imports maxnode - 1 bits, including the final word's high bit.
    return mask.len * word_bits + 1;
}

fn mode(policy: Policy) usize {
    return static_nodes | @as(usize, switch (policy) {
        .bind => 2,
        .prefer => 1,
    });
}

fn roundedLength(len: usize, page_size: usize) !usize {
    if (len == 0) return error.InvalidLength;
    return (try std.math.add(usize, len, page_size - 1)) & ~(page_size - 1);
}

fn advise(bytes: []u8, advice: u32) !void {
    const rc = linux.madvise(bytes.ptr, bytes.len, advice);
    if (linux.errno(rc) == .INVAL) {
        // EINVAL also covers bad mappings; only the zero-length probe identifies unknown advice.
        const probe = linux.syscall3(.madvise, 0, 0, advice);
        if (linux.errno(probe) == .INVAL) return error.Unsupported;
        _ = try checked(probe);
    }
    _ = try checked(rc);
}

fn windowsAffinityMask() !usize {
    if (@bitSizeOf(usize) != 64) return error.Unsupported;
    const win32 = @import("../../event/backends/win32.zig");
    var process: usize = undefined;
    var system: usize = undefined;
    if (win32.GetProcessAffinityMask(std.os.windows.GetCurrentProcess(), &process, &system) == 0) return error.Unexpected;
    if (process == 0 or process & ~system != 0) return error.InvalidCpuSet;
    return process;
}

fn cpuMask(allocator: std.mem.Allocator, cpus: []const usize) ![]usize {
    if (cpus.len == 0) return error.EmptyCpuSet;
    const max = std.mem.max(usize, cpus);
    _ = try std.math.add(usize, max, 1);
    const words = max / word_bits + 1;
    _ = try std.math.mul(usize, words, @sizeOf(usize));
    const mask = try allocator.alloc(usize, words);
    @memset(mask, 0);
    for (cpus) |cpu| mask[cpu / word_bits] |= @as(usize, 1) << @intCast(cpu % word_bits);
    return mask;
}

fn affinityMask(allocator: std.mem.Allocator) ![]usize {
    var words: usize = 16;
    while (true) {
        const size = try std.math.mul(usize, words, @sizeOf(usize));
        const mask = try allocator.alloc(usize, words);
        @memset(mask, 0);
        const rc = linux.syscall3(.sched_getaffinity, 0, size, @intFromPtr(mask.ptr));
        switch (linux.errno(rc)) {
            .SUCCESS => return mask,
            .INVAL => {
                allocator.free(mask);
                words = try std.math.mul(usize, words, 2);
            },
            else => |err| {
                allocator.free(mask);
                return errnoError(err);
            },
        }
    }
}

fn policyMask(allocator: std.mem.Allocator, policy: Policy) ![]usize {
    var words: usize = 16;
    while (true) {
        _ = try std.math.mul(usize, words, word_bits);
        const mask = try allocator.alloc(usize, words);
        @memset(mask, 0);
        // MPOL_F_MEMS_ALLOWED is the kernel's current allowed, memory-bearing set.
        const rc = linux.syscall5(.get_mempolicy, 0, @intFromPtr(mask.ptr), words * word_bits, 0, 4);
        switch (linux.errno(rc)) {
            .SUCCESS => {
                selectNode(mask, policy) catch |err| {
                    allocator.free(mask);
                    return err;
                };
                return mask;
            },
            .INVAL => {
                allocator.free(mask);
                words = try std.math.mul(usize, words, 2);
            },
            else => |err| {
                allocator.free(mask);
                return errnoError(err);
            },
        }
    }
}

fn selectNode(mask: []usize, policy: Policy) !void {
    const node = switch (policy) {
        .bind, .prefer => |id| id,
    };
    if (node / word_bits >= mask.len or mask[node / word_bits] & (@as(usize, 1) << @intCast(node % word_bits)) == 0) return error.InvalidNode;
    @memset(mask, 0);
    mask[node / word_bits] = @as(usize, 1) << @intCast(node % word_bits);
}

fn checked(rc: usize) !usize {
    switch (linux.errno(rc)) {
        .SUCCESS => return rc,
        else => |err| return errnoError(err),
    }
}

fn errnoError(err: linux.E) error{ Unsupported, PermissionDenied, InvalidArgument, InvalidNode, InvalidAddress, PageNotPresent, PageBusy, OutOfMemory, Interrupted, Unexpected } {
    return switch (err) {
        .NOSYS, .OPNOTSUPP => error.Unsupported,
        .PERM, .ACCES => error.PermissionDenied,
        .INVAL => error.InvalidArgument,
        .NODEV => error.InvalidNode,
        .FAULT => error.InvalidAddress,
        .NOENT => error.PageNotPresent,
        .BUSY => error.PageBusy,
        .NOMEM => error.OutOfMemory,
        .INTR => error.Interrupted,
        else => error.Unexpected,
    };
}

test "NUMA masks and page rounding" {
    const testing = std.testing;
    const mask = try cpuMask(testing.allocator, &.{ 0, 63, 1024, 4097, 4097 });
    defer testing.allocator.free(mask);
    for ([_]usize{ 0, 63, 1024, 4097 }) |id| {
        try testing.expect(mask[id / word_bits] & (@as(usize, 1) << @intCast(id % word_bits)) != 0);
    }
    var count: usize = 0;
    for (mask) |word| count += @popCount(word);
    try testing.expectEqual(@as(usize, 4), count);
    try testing.expectError(error.EmptyCpuSet, cpuMask(testing.allocator, &.{}));
    try testing.expectError(error.Overflow, cpuMask(testing.allocator, &.{std.math.maxInt(usize)}));
    try testing.expectEqual(@as(usize, 8192), try roundedLength(4097, 4096));
    try testing.expectError(error.InvalidLength, roundedLength(0, 4096));
    try testing.expectError(error.Overflow, roundedLength(std.math.maxInt(usize), 4096));
    try testing.expectEqual(@as(usize, static_nodes | 2), mode(.{ .bind = 7 }));
    try testing.expectEqual(@as(usize, static_nodes | 1), mode(.{ .prefer = 7 }));
    var nodes: [1025 / word_bits + 1]usize = @splat(0);
    nodes[1025 / word_bits] |= @as(usize, 1) << @intCast(1025 % word_bits);
    try testing.expectError(error.InvalidNode, selectNode(&nodes, .{ .prefer = 1 }));
    try testing.expectError(error.InvalidNode, selectNode(&nodes, .{ .bind = 1 }));
    try testing.expectError(error.InvalidNode, selectNode(&nodes, .{ .prefer = 4097 }));
    try selectNode(&nodes, .{ .prefer = 1025 });
    try testing.expectEqual(@as(usize, 1) << @intCast(1025 % word_bits), nodes[1025 / word_bits]);
}

test "NUMA pin changes only a dedicated thread and verifies the exact mask" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const testing = std.testing;
    const before = try allowedCpus(testing.allocator);
    defer testing.allocator.free(before);
    try testing.expect(before.len != 0);
    const Runner = struct {
        fn run(cpu: usize, result: *anyerror!void) void {
            result.* = check(cpu);
        }
        fn check(cpu: usize) !void {
            try pin(&.{cpu});
            const mask = try affinityMask(std.heap.page_allocator);
            defer std.heap.page_allocator.free(mask);
            // A successful getaffinity buffer spans every kernel-supported CPU bit.
            try testing.expectError(error.AffinityMismatch, pin(&.{ cpu, mask.len * word_bits }));
            const effective = try allowedCpus(std.heap.page_allocator);
            defer std.heap.page_allocator.free(effective);
            try testing.expectEqualSlices(usize, &.{cpu}, effective);
            try testing.expectError(error.EmptyCpuSet, pin(&.{}));
            try testing.expectError(error.Overflow, pin(&.{std.math.maxInt(usize)}));
        }
    };
    var result: anyerror!void = {};
    const thread = try std.Thread.spawn(.{}, Runner.run, .{ before[0], &result });
    thread.join();
    result catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    const after = try allowedCpus(testing.allocator);
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(usize, before, after);
}

test "NUMA Region bind residency and unpopulated page query" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const testing = std.testing;
    var topology = try Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    const node = for (topology.nodes) |node| {
        if (node.memory) break node.id;
    } else return error.TestExpectedMemoryNode;
    var region = Region.init(std.heap.pageSize() + 1, .{ .bind = node }) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    defer region.deinit();
    try testing.expectEqual(@as(usize, 0), @intFromPtr(region.bytes.ptr) % std.heap.pageSize());
    try testing.expectEqual(2 * std.heap.pageSize(), region.bytes.len);
    if (pageNode(region.bytes.ptr)) |_| {
        return error.TestExpectedUnpopulatedPage;
    } else |err| switch (err) {
        error.PageNotPresent, error.InvalidAddress => {},
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    }
    var offset: usize = 0;
    while (offset < region.bytes.len) : (offset += std.heap.pageSize()) {
        @as(*volatile u8, &region.bytes[offset]).* = 1;
        try testing.expectEqual(node, try pageNode(region.bytes.ptr + offset));
    }
    try testing.expectError(error.InvalidAlignment, pageNode(region.bytes.ptr + 1));
    try testing.expectError(error.InvalidNode, Region.init(1, .{ .prefer = std.math.maxInt(usize) }));
    var preferred = try Region.init(1, .{ .prefer = node });
    defer preferred.deinit();
}

test "NUMA current-thread memory policy uses physical node IDs" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const testing = std.testing;
    var topology = try Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    const node = for (topology.nodes) |node| {
        if (node.memory) break node.id;
    } else return error.TestExpectedMemoryNode;
    const Runner = struct {
        fn run(id: usize, result: *anyerror!void) void {
            result.* = check(id);
        }
        fn check(id: usize) !void {
            for ([_]Policy{ .{ .bind = id }, .{ .prefer = id } }) |policy| {
                try setPolicy(policy);
                const mask = try policyMask(std.heap.page_allocator, policy);
                defer std.heap.page_allocator.free(mask);
                var actual_mode: i32 = 0;
                @memset(mask, 0);
                _ = try checked(linux.syscall5(.get_mempolicy, @intFromPtr(&actual_mode), @intFromPtr(mask.ptr), mask.len * word_bits, 0, 0));
                try testing.expectEqual(mode(policy), @as(usize, @intCast(actual_mode)));
                try testing.expect(mask[id / word_bits] & (@as(usize, 1) << @intCast(id % word_bits)) != 0);
            }
            try testing.expectError(error.InvalidNode, setPolicy(.{ .prefer = std.math.maxInt(usize) }));
        }
    };
    var result: anyerror!void = {};
    const thread = try std.Thread.spawn(.{}, Runner.run, .{ node, &result });
    thread.join();
    result catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
}

test "NUMA Region installs policy before MCL_FUTURE populates pages" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const testing = std.testing;
    var topology = try Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    const source = for (topology.nodes) |node| {
        if (node.memory) break node.id;
    } else return error.SkipZigTest;
    var destination = source;
    for (topology.nodes) |node| {
        if (node.memory) destination = node.id;
    }

    const Child = struct {
        fn run(from: usize, to: usize) !void {
            try setPolicy(.{ .bind = from });
            _ = try checked(linux.mlockall(.{ .FUTURE = true }));
            const size = std.heap.pageSize();
            const raw = try checked(linux.mmap(null, size, .{ .READ = true, .WRITE = true }, .{
                .TYPE = .PRIVATE,
                .ANONYMOUS = true,
            }, -1, 0));
            const control = @as([*]align(std.heap.page_size_min) u8, @ptrFromInt(raw))[0..size];
            defer std.posix.munmap(control);
            if (try pageNode(control.ptr) != from) return error.WrongControlNode;

            var region = try Region.init(size, .{ .bind = to });
            defer region.deinit();
            @as(*volatile u8, &region.bytes[0]).* = 1;
            if (try pageNode(region.bytes.ptr) != to) return error.WrongRegionNode;
        }
    };

    // mlockall changes process-wide state; keep it isolated from the test runner.
    const pid = try checked(linux.fork());
    if (pid == 0) {
        Child.run(source, destination) catch |err| linux.exit_group(switch (err) {
            error.Unsupported, error.PermissionDenied, error.OutOfMemory => 77,
            else => 1,
        });
        linux.exit_group(0);
    }
    var status: i32 = undefined;
    while (true) {
        const rc = linux.waitpid(@intCast(pid), &status, 0);
        if (linux.errno(rc) == .INTR) continue;
        _ = try checked(rc);
        break;
    }
    const code: u32 = @bitCast(status);
    try testing.expect(linux.W.IFEXITED(code));
    if (linux.W.EXITSTATUS(code) == 77) return error.SkipZigTest;
    try testing.expectEqual(0, linux.W.EXITSTATUS(code));
}

test "NUMA non-Linux API stubs" {
    if (builtin.os.tag == .linux) return;
    const testing = std.testing;
    if (builtin.os.tag != .windows or @bitSizeOf(usize) != 64) {
        try testing.expectError(error.Unsupported, allowedCpus(testing.allocator));
        try testing.expectError(error.Unsupported, pin(&.{0}));
    }
    try testing.expectError(error.Unsupported, setPolicy(.{ .bind = 0 }));
    try testing.expectError(error.Unsupported, Region.init(1, .{ .bind = 0 }));
    try testing.expectError(error.Unsupported, Region.initWithOptions(1, .{ .bind = 0 }, .{ .prefault = true, .huge_pages = true }));
    try testing.expectError(error.Unsupported, PageAllocator.init(.{ .bind = 0 }, .{}));
    var byte: u8 = 0;
    try testing.expectError(error.Unsupported, pageNode(@ptrCast(&byte)));
    try testing.expectError(error.Unsupported, Topology.discover(testing.allocator, testing.io));
    var region: Region = undefined;
    region.deinit();
}

test {
    _ = Topology;
    _ = @import("Plan.zig");
    _ = PageAllocator;
    _ = @import("memory_test.zig");
}

test "NUMA Windows CPU affinity on a dedicated thread" {
    if (builtin.os.tag != .windows or @bitSizeOf(usize) != 64) return error.SkipZigTest;
    const testing = std.testing;
    const before = try allowedCpus(testing.allocator);
    defer testing.allocator.free(before);
    try testing.expect(before.len != 0);
    const Runner = struct {
        fn run(cpu: usize, result: *anyerror!void) void {
            result.* = check(cpu);
        }
        fn check(cpu: usize) !void {
            const win32 = @import("../../event/backends/win32.zig");
            try pin(&.{cpu});
            const bit = @as(usize, 1) << @intCast(cpu);
            try testing.expectEqual(bit, win32.SetThreadAffinityMask(std.os.windows.GetCurrentThread(), bit));
            try testing.expectError(error.EmptyCpuSet, pin(&.{}));
            try testing.expectError(error.InvalidCpu, pin(&.{64}));
            try testing.expectError(error.InvalidCpu, pin(&.{std.math.maxInt(usize)}));
            const invalid = ~try windowsAffinityMask();
            if (invalid != 0) try testing.expectError(error.InvalidCpuSet, pin(&.{@ctz(invalid)}));
            try testing.expectEqual(bit, win32.SetThreadAffinityMask(std.os.windows.GetCurrentThread(), bit));
        }
    };
    var result: anyerror!void = {};
    const thread = try std.Thread.spawn(.{}, Runner.run, .{ before[0], &result });
    thread.join();
    try result;
    const after = try allowedCpus(testing.allocator);
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(usize, before, after);
}
