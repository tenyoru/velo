const std = @import("std");

const native = @import("native.zig");

const F_ALLOCATEALL = 0x4;
const F_PEOFPOSMODE = 3;

const PhysicalExtent = extern struct {
    flags: u32 = 0,
    contiguous: i64 align(4),
    device_offset: i64 align(4),
};

const Preallocation = extern struct {
    flags: u32 = F_ALLOCATEALL,
    position: c_int = F_PEOFPOSMODE,
    offset: i64 = 0,
    length: i64,
    bytes_allocated: i64 = 0,
};

comptime {
    std.debug.assert(@sizeOf(PhysicalExtent) == 20);
}

pub fn setDirect(file: std.Io.File) !void {
    while (true) {
        switch (std.c.errno(std.c.fcntl(file.handle, std.c.F.NOCACHE, @as(c_int, 1)))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return native.mapError(.sync, err),
        }
    }
}

pub fn allocate(io: std.Io, file: std.Io.File, start: u64, end: u64) !void {
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.UnsupportedOperation;
    if (stat.size > std.math.maxInt(i64)) return error.InvalidArgument;

    // Growth uses physical EOF, so validate the whole prefix; otherwise only the requested range matters.
    var offset: u64 = if (end > stat.size) 0 else start;
    const limit = @min(end, stat.size);
    while (offset < limit) {
        var extent: PhysicalExtent = .{
            .contiguous = @intCast(limit - offset),
            .device_offset = @intCast(offset),
        };
        const rc = std.c.fcntl(file.handle, std.c.F.LOG2PHYS_EXT, @intFromPtr(&extent));
        switch (std.c.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            .INVAL, .OPNOTSUPP, .NOSYS => return error.UnsupportedOperation,
            else => |err| return native.mapError(.write_at, err),
        }

        if (extent.device_offset < 0 or extent.contiguous <= 0) return error.UnsupportedOperation;
        offset += @min(limit - offset, @as(u64, @intCast(extent.contiguous)));
    }
    if (end <= stat.size) return;

    var store: Preallocation = .{ .length = @intCast(end - stat.size) };
    while (true) {
        switch (std.c.errno(std.c.fcntl(file.handle, std.c.F.PREALLOCATE, @intFromPtr(&store)))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return native.mapError(.write_at, err),
        }
    }
    if (store.bytes_allocated < store.length) return error.UnsupportedOperation;

    while (true) {
        switch (std.c.errno(std.c.ftruncate(file.handle, @intCast(end)))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return native.mapError(.write_at, err),
        }
    }
}
