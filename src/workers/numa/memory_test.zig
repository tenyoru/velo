const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const numa = @import("root.zig");
const testing = std.testing;

fn memoryNode() !usize {
    var topology = try numa.Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    for (topology.nodes) |node| if (node.memory) return node.id;
    return error.TestExpectedMemoryNode;
}

fn region(len: usize, node: usize, options: numa.Region.Options) !numa.Region {
    return numa.Region.initWithOptions(len, .{ .bind = node }, options) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => error.SkipZigTest,
        else => err,
    };
}

fn backing(node: usize, options: numa.Region.Options) !numa.PageAllocator {
    return numa.PageAllocator.init(.{ .bind = node }, options) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => error.SkipZigTest,
        else => err,
    };
}

fn expectUnmapped(address: usize, len: usize) !void {
    var offset: usize = 0;
    while (offset < len) : (offset += std.heap.pageSize()) {
        var resident: [1]u8 = undefined;
        try testing.expectEqual(linux.E.NOMEM, linux.errno(linux.mincore(@ptrFromInt(address + offset), std.heap.pageSize(), &resident)));
    }
}

test "NUMA Region validates length, alignment, and reservation overflow" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const policy: numa.Policy = .{ .bind = 0 };
    try testing.expectError(error.InvalidLength, numa.Region.initWithOptions(0, policy, .{}));
    for ([_]usize{ 0, 3, std.heap.pageSize() + 1 }) |alignment| {
        try testing.expectError(error.InvalidAlignment, numa.Region.initWithOptions(1, policy, .{ .alignment = alignment }));
        try testing.expectError(error.InvalidAlignment, numa.PageAllocator.init(policy, .{ .alignment = alignment }));
    }
    try testing.expectError(error.Overflow, numa.Region.initWithOptions(std.math.maxInt(usize), policy, .{}));
    const high_bit = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    try testing.expectError(error.Overflow, numa.Region.initWithOptions(high_bit + std.heap.pageSize(), policy, .{ .alignment = high_bit }));
}

test "NUMA Region prefault is resident and bound before user writes" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const node = try memoryNode();
    const page_size = std.heap.pageSize();
    var memory = try region(2 * page_size + 1, node, .{ .alignment = 16 * page_size, .prefault = true });
    const address = @intFromPtr(memory.bytes.ptr);
    const len = memory.bytes.len;
    {
        defer memory.deinit();
        try testing.expectEqual(@as(usize, 0), address % (16 * page_size));
        try testing.expectEqual(3 * page_size, len);
        var resident: [3]u8 = undefined;
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.mincore(memory.bytes.ptr, len, &resident)));
        for (resident) |present| try testing.expect(present & 1 != 0);
        for (0..3) |page| {
            const actual = numa.pageNode(memory.bytes.ptr + page * page_size) catch |err| switch (err) {
                error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
                else => return err,
            };
            try testing.expectEqual(node, actual);
        }
        for (memory.bytes) |byte| try testing.expectEqual(@as(u8, 0), byte);
    }
    try expectUnmapped(address, len);
}

const Smaps = struct { hint: bool = false, huge_kib: usize = 0 };

fn smaps(address: usize) !Smaps {
    const file = try std.Io.Dir.openFileAbsolute(testing.io, "/proc/self/smaps", .{});
    defer file.close(testing.io);
    var reader = file.readerStreaming(testing.io, &.{});
    reader.mode = .streaming_simple;
    const text = try reader.interface.allocRemaining(testing.allocator, .limited(8 * 1024 * 1024));
    defer testing.allocator.free(text);
    var result: Smaps = .{};
    var matched = false;
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| {
        var fields = std.mem.tokenizeScalar(u8, line, ' ');
        const first = fields.next() orelse continue;
        if (std.mem.indexOfScalar(u8, first, '-')) |dash| {
            if (matched) break;
            const start = try std.fmt.parseInt(usize, first[0..dash], 16);
            const end = try std.fmt.parseInt(usize, first[dash + 1 ..], 16);
            matched = start <= address and address < end;
        } else if (matched and std.mem.eql(u8, first, "AnonHugePages:")) {
            result.huge_kib = try std.fmt.parseInt(usize, fields.next() orelse return error.InvalidSmaps, 10);
        } else if (matched and std.mem.eql(u8, first, "VmFlags:")) {
            while (fields.next()) |flag| {
                if (std.mem.eql(u8, flag, "hg")) result.hint = true;
            }
        }
    }
    if (!matched) return error.MappingNotFound;
    return result;
}

test "NUMA Region huge-page hint marks the VMA without prefaulting" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var memory = try region(2 * 1024 * 1024, try memoryNode(), .{ .alignment = 2 * 1024 * 1024, .huge_pages = true });
    defer memory.deinit();
    try testing.expect((try smaps(@intFromPtr(memory.bytes.ptr))).hint);
    var resident: [1]u8 = undefined;
    try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.mincore(memory.bytes.ptr, std.heap.pageSize(), &resident)));
    try testing.expectEqual(@as(u8, 0), resident[0] & 1);
}

test "NUMA Region transparent huge pages when granted by the kernel" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var memory = try region(2 * 1024 * 1024, try memoryNode(), .{ .alignment = 2 * 1024 * 1024, .huge_pages = true, .prefault = true });
    defer memory.deinit();
    const info = try smaps(@intFromPtr(memory.bytes.ptr));
    try testing.expect(info.hint);
    if (info.huge_kib == 0) return error.SkipZigTest;
    try testing.expect(info.huge_kib * 1024 <= memory.bytes.len);
}

test "NUMA PageAllocator zeroes, aligns, resizes, and releases rounded mappings" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const page_size = std.heap.pageSize();
    const node = try memoryNode();
    var pages = try backing(node, .{ .alignment = 16 * page_size, .prefault = true });
    const allocator = pages.allocator();
    const alignment: std.mem.Alignment = .fromByteUnits(32 * page_size);
    const len = page_size + 7;
    const ptr = allocator.rawAlloc(len, alignment, @returnAddress()) orelse return error.OutOfMemory;
    var live_len = len;
    const address = @intFromPtr(ptr);
    {
        defer allocator.rawFree(ptr[0..live_len], alignment, @returnAddress());
        try testing.expectEqual(@as(usize, 0), address % (32 * page_size));
        var resident: [2]u8 = undefined;
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.mincore(ptr, len, &resident)));
        for (resident, 0..) |present, i| {
            try testing.expect(present & 1 != 0);
            const actual = numa.pageNode(ptr + i * page_size) catch |err| switch (err) {
                error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
                else => return err,
            };
            try testing.expectEqual(node, actual);
        }
        for (ptr[0..len]) |byte| try testing.expectEqual(@as(u8, 0), byte);
        try testing.expect(allocator.rawResize(ptr[0..live_len], alignment, 2 * page_size, 0));
        live_len = 2 * page_size;
        @memset(ptr[0..live_len], 0x7b);
        try testing.expect(allocator.rawResize(ptr[0..live_len], alignment, 1, 0));
        live_len = 1;
        try expectUnmapped(address + page_size, page_size);
        try testing.expect(!allocator.rawResize(ptr[0..live_len], alignment, page_size + 1, 0));
        try testing.expect(allocator.rawRemap(ptr[0..live_len], alignment, page_size + 1, 0) == null);
        try testing.expect(!allocator.rawResize(ptr[0..live_len], alignment, std.math.maxInt(usize), 0));
        try testing.expectEqual(@as(u8, 0x7b), ptr[0]);
        try testing.expect(allocator.rawRemap(ptr[0..live_len], alignment, page_size, 0) == ptr);
        live_len = page_size;
    }
    try expectUnmapped(address, 2 * page_size);
    try testing.expect(allocator.rawAlloc(std.math.maxInt(usize), .@"1", 0) == null);
    const fresh = allocator.rawAlloc(1, .@"1", 0) orelse return error.OutOfMemory;
    defer allocator.rawFree(fresh[0..1], .@"1", 0);
    try testing.expectEqual(@as(usize, 0), @intFromPtr(fresh) % (16 * page_size));
    try testing.expectEqual(@as(u8, 0), fresh[0]);
}

test "NUMA PageAllocator supports realloc fallback, ArenaAllocator, and DebugAllocator" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const node = try memoryNode();
    var pages = try backing(node, .{});
    const allocator = pages.allocator();
    var bytes = try allocator.alloc(u8, 33);
    {
        defer allocator.free(bytes);
        const old_address = @intFromPtr(bytes.ptr);
        @memset(bytes, 0x6d);
        bytes = try allocator.realloc(bytes, 3 * std.heap.pageSize() + 7);
        for (bytes[0..33]) |byte| try testing.expectEqual(@as(u8, 0x6d), byte);
        try testing.expect(@intFromPtr(bytes.ptr) != old_address);
        try expectUnmapped(old_address, std.heap.pageSize());
    }
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for (0..32) |i| {
        const allocation = try a.alignedAlloc(u64, .@"64", 64 + i);
        @memset(allocation, i);
    }
    try testing.expect(arena.reset(.retain_capacity));
    try testing.expect(arena.reset(.free_all));
    var debug: std.heap.DebugAllocator(.{ .page_size = 16 * 1024 }) = .{ .backing_allocator = allocator };
    defer testing.expectEqual(.ok, debug.deinit()) catch @panic("NUMA debug backing leaked");
    const d = debug.allocator();
    var small: [128][]u8 = undefined;
    for (&small, 0..) |*allocation, i| {
        allocation.* = try d.alloc(u8, i + 1);
        @memset(allocation.*, @intCast(i));
    }
    const large = try d.alignedAlloc(u8, .fromByteUnits(64 * 1024), 96 * 1024);
    defer d.free(large);
    @memset(large, 0x33);
    for (small, 0..) |allocation, i| {
        try testing.expectEqual(@as(u8, @intCast(i)), allocation[0]);
        d.free(allocation);
    }
}

fn mappedBytes() !usize {
    const opened = linux.openat(linux.AT.FDCWD, "/proc/self/maps", .{ .ACCMODE = .RDONLY }, 0);
    if (linux.errno(opened) != .SUCCESS) return error.TestMapsOpenFailed;
    const fd: linux.fd_t = @intCast(opened);
    defer _ = linux.close(fd);
    var buffer: [128 * 1024]u8 = undefined;
    var len: usize = 0;
    while (true) {
        if (len == buffer.len) return error.TestMapsTooLarge;
        const count = linux.read(fd, buffer[len..].ptr, buffer.len - len);
        if (linux.errno(count) == .INTR) continue;
        if (linux.errno(count) != .SUCCESS) return error.TestMapsReadFailed;
        if (count == 0) break;
        len += count;
    }
    var size: usize = 0;
    var lines = std.mem.tokenizeScalar(u8, buffer[0..len], '\n');
    while (lines.next()) |line| {
        const dash = std.mem.indexOfScalar(u8, line, '-') orelse return error.InvalidMaps;
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse return error.InvalidMaps;
        const start = try std.fmt.parseInt(usize, line[0..dash], 16);
        const end = try std.fmt.parseInt(usize, line[dash + 1 .. space], 16);
        size = try std.math.add(usize, size, end - start);
    }
    return size;
}

test "NUMA failed advice releases aligned reservations and defaults need no advice" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const node = try memoryNode();
    var probe = try region(1, node, .{ .prefault = true });
    probe.deinit();
    const Child = struct {
        fn run(id: usize) !void {
            var pages = try numa.PageAllocator.init(.{ .bind = id }, .{ .alignment = 32 * std.heap.pageSize(), .prefault = true });
            const Instruction = extern struct { code: u16, jt: u8 = 0, jf: u8 = 0, k: u32 };
            const bpf = linux.BPF;
            const instructions = [_]Instruction{
                .{ .code = bpf.LD | bpf.W | bpf.ABS, .k = 0 },
                .{ .code = bpf.JMP | bpf.JEQ | bpf.K, .jf = 1, .k = @backingInt(linux.SYS.madvise) },
                .{ .code = bpf.RET | bpf.K, .k = linux.SECCOMP.RET.ERRNO | @as(u32, @backingInt(linux.E.INVAL)) },
                .{ .code = bpf.RET | bpf.K, .k = linux.SECCOMP.RET.ALLOW },
            };
            const program: extern struct { len: u16, filter: [*]const Instruction } = .{ .len = instructions.len, .filter = &instructions };
            if (linux.errno(linux.prctl(@backingInt(linux.PR.SET_NO_NEW_PRIVS), 1, 0, 0, 0)) != .SUCCESS or
                linux.errno(linux.seccomp(linux.SECCOMP.SET_MODE_FILTER, 0, &program)) != .SUCCESS) return error.SkipZigTest;

            const before = try mappedBytes();
            for (0..4) |_| {
                for ([_]numa.Region.Options{
                    .{ .alignment = 32 * std.heap.pageSize(), .huge_pages = true },
                    .{ .alignment = 32 * std.heap.pageSize(), .prefault = true },
                }) |options| {
                    try testing.expectError(error.Unsupported, numa.Region.initWithOptions(2 * std.heap.pageSize() + 1, .{ .bind = id }, options));
                    try testing.expectError(error.Unsupported, numa.PageAllocator.init(.{ .bind = id }, options));
                }
                try testing.expect(pages.allocator().rawAlloc(3 * std.heap.pageSize(), .@"1", 0) == null);
                var defaults = try numa.Region.init(1, .{ .bind = id });
                defaults.deinit();
            }
            try testing.expectEqual(before, try mappedBytes());
        }
    };
    // Seccomp is irreversible and fork leaves one thread, so procfs map totals stay stable.
    const pid = linux.fork();
    if (linux.errno(pid) != .SUCCESS) return error.TestForkFailed;
    if (pid == 0) {
        Child.run(node) catch |err| linux.exit_group(if (err == error.SkipZigTest) 77 else 1);
        linux.exit_group(0);
    }
    var status: i32 = undefined;
    while (true) {
        const rc = linux.waitpid(@intCast(pid), &status, 0);
        if (linux.errno(rc) == .INTR) continue;
        try testing.expectEqual(linux.E.SUCCESS, linux.errno(rc));
        break;
    }
    const code: u32 = @bitCast(status);
    try testing.expect(linux.W.IFEXITED(code));
    if (linux.W.EXITSTATUS(code) == 77) return error.SkipZigTest;
    try testing.expectEqual(0, linux.W.EXITSTATUS(code));
}
