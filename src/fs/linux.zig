const std = @import("std");

const io = @import("../event/root.zig");
const Alignment = @import("DirectFile.zig").Alignment;
const native = @import("native.zig");

const linux = std.os.linux;

pub fn setDirect(file: std.Io.File) !void {
    const flags = while (true) {
        const rc = linux.fcntl(file.handle, linux.F.GETFL, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => break rc,
            .INTR => continue,
            else => |err| return native.mapError(.sync, err),
        }
    };

    while (true) {
        const rc = linux.fcntl(
            file.handle,
            linux.F.SETFL,
            flags | @as(u32, @bitCast(linux.O{ .DIRECT = true })),
        );
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .INVAL => return error.UnsupportedOperation,
            else => |err| return native.mapError(.sync, err),
        }
    }
}

/// Null when the kernel or filesystem does not report direct I/O alignment.
pub fn directAlignment(file: std.Io.File) !?Alignment {
    var stat: linux.Statx = undefined;

    while (true) {
        const rc = linux.statx(file.handle, "", linux.AT.EMPTY_PATH, .{ .DIOALIGN = true }, &stat);
        switch (linux.errno(rc)) {
            .SUCCESS => break,
            .INTR => continue,
            .NOSYS, .INVAL => return null,
            else => |err| return native.mapError(.sync, err),
        }
    }
    if (!stat.mask.DIOALIGN) return null;

    return .{ .memory = stat.dio_mem_align, .offset = stat.dio_offset_align };
}

pub fn transfer(
    comptime kind: io.Kind,
    file: std.Io.File,
    buffer: if (kind == .read_at) []u8 else []const u8,
    offset: u64,
) !usize {
    if (buffer.len == 0) return 0;

    // The kernel further applies its page-size-dependent MAX_RW_COUNT limit.
    const len = @min(buffer.len, std.math.maxInt(i32));

    while (true) {
        const rc = if (kind == .read_at)
            linux.pread(file.handle, buffer.ptr, len, @intCast(offset))
        else
            linux.pwrite(file.handle, buffer.ptr, len, @intCast(offset));
        switch (linux.errno(rc)) {
            .SUCCESS => return rc,
            .INTR => continue,
            else => |err| return native.mapError(kind, err),
        }
    }
}

pub fn syncData(file: std.Io.File) !void {
    while (true) {
        switch (linux.errno(linux.fdatasync(file.handle))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return native.mapError(.sync, err),
        }
    }
}

pub fn allocate(file: std.Io.File, offset: u64, len: u64) !void {
    while (true) {
        const rc = linux.fallocate(file.handle, 0, @intCast(offset), @intCast(len));
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return native.mapError(.write_at, err),
        }
    }
}
