//! NUMA-placed, zero-filled anonymous mappings for ArenaAllocator/DebugAllocator backing,
//! not small-object hot paths. Keep this object pinned while its allocator is borrowed;
//! do not mutate its policy or options while allocations are live.

const std = @import("std");
const numa = @import("root.zig");
const PageAllocator = @This();

policy: numa.Policy,
options: numa.Region.Options,

pub fn init(policy: numa.Policy, options: numa.Region.Options) !PageAllocator {
    var probe = try numa.Region.initWithOptions(1, policy, options);
    probe.deinit();
    return .{ .policy = policy, .options = options };
}

pub fn allocator(self: *PageAllocator) std.mem.Allocator {
    return .{ .ptr = self, .vtable = &.{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    } };
}

fn alloc(context: *anyopaque, len: usize, alignment: std.mem.Alignment, _: usize) ?[*]u8 {
    const self: *const PageAllocator = @ptrCast(@alignCast(context));
    var options = self.options;
    options.alignment = @max(options.alignment, alignment.toByteUnits());
    const region = numa.Region.initWithOptions(len, self.policy, options) catch return null;
    return region.bytes.ptr;
}

fn resize(_: *anyopaque, memory: []u8, _: std.mem.Alignment, new_len: usize, _: usize) bool {
    if (@import("builtin").os.tag != .linux) return false;
    const page_size = std.heap.pageSize();
    const old_size = std.mem.alignForward(usize, memory.len, page_size);
    const new_size = std.math.add(usize, new_len, page_size - 1) catch return false;
    const rounded = new_size & ~(page_size - 1);
    if (rounded > old_size) return false;
    if (rounded < old_size) std.posix.munmap(@alignCast(memory.ptr[rounded..old_size]));
    return true;
}

fn remap(context: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ra: usize) ?[*]u8 {
    if (resize(context, memory, alignment, new_len, ra)) return memory.ptr;
    return null;
}

fn free(_: *anyopaque, memory: []u8, _: std.mem.Alignment, _: usize) void {
    var region: numa.Region = .{
        .bytes = @alignCast(memory.ptr[0..std.mem.alignForward(usize, memory.len, std.heap.pageSize())]),
    };
    region.deinit();
}
