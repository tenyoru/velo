//! File opened for direct I/O, with the alignment its transfers require.
//! readAt/writeAt return InvalidArgument before submitting a misaligned request: the
//! kernel may not reject it, and a stuck direct read can be neither canceled nor killed.
//! Short transfers keep that rule, so continue them at aligned positions only.
//! Use `file` for Sync, Allocate and Close; transfers through it skip validation.

const std = @import("std");
const builtin = @import("builtin");

const Pool = @import("../workers/root.zig").Pool;
const fs = @import("root.zig");
const linux = @import("linux.zig");
const windows = @import("windows.zig");

const DirectFile = @This();

file: std.Io.File,
alignment: Alignment,

/// Powers of two: `memory` for buffer addresses, `offset` for offsets and lengths.
pub const Alignment = struct {
    memory: u32,
    offset: u32,

    fn valid(self: Alignment) bool {
        return std.mem.isValidAlign(self.memory) and std.mem.isValidAlign(self.offset);
    }
};

/// Takes ownership of `file` only on success.
pub fn init(file: std.Io.File, fallback: ?Alignment) !DirectFile {
    return .{ .file = file, .alignment = try resolve(try discover(file), fallback) };
}

/// Reported alignment wins so that `fallback` cannot weaken a limit the OS knows.
pub fn resolve(discovered: ?Alignment, fallback: ?Alignment) error{
    InvalidArgument,
    UnsupportedOperation,
}!Alignment {
    if (fallback) |alignment| if (!alignment.valid()) return error.InvalidArgument;
    if (discovered) |alignment| if (alignment.valid()) return alignment;

    return fallback orelse error.UnsupportedOperation;
}

pub fn check(self: DirectFile, buffer: []const u8, offset: u64) error{InvalidArgument}!void {
    const memory = buffer.len == 0 or std.mem.isAligned(@intFromPtr(buffer.ptr), self.alignment.memory);

    if (!memory or !std.mem.isAligned(buffer.len, self.alignment.offset) or
        !std.mem.isAlignedGeneric(u64, offset, self.alignment.offset))
        return error.InvalidArgument;
}

pub fn readAt(self: DirectFile, pool: *Pool, buffer: []u8, offset: u64) error{InvalidArgument}!fs.ReadAt {
    try self.check(buffer, offset);

    return .init(pool, .{ .file = self.file, .buffer = buffer, .offset = offset });
}

pub fn writeAt(self: DirectFile, pool: *Pool, buffer: []const u8, offset: u64) error{InvalidArgument}!fs.WriteAt {
    try self.check(buffer, offset);

    return .init(pool, .{ .file = self.file, .buffer = buffer, .offset = offset });
}

fn discover(file: std.Io.File) !?Alignment {
    return switch (builtin.os.tag) {
        .linux => linux.directAlignment(file),
        .windows => windows.directAlignment(file),
        // F_NOCACHE is a cache hint and imposes no transfer constraints.
        .macos => .{ .memory = 1, .offset = 1 },
        else => null,
    };
}
