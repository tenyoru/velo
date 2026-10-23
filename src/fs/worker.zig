//! Pool-backed file I/O; pin paths/buffers until ready, even after cancel.
//! Returned handles remain caller-owned. Close only after all I/O on the handle;
//! a canceled task's Close never runs, so use File.close in cleanup.
//! WriteAt may be short; durability needs Sync, not just WriteAt or Close.
//! Sync does not persist a new file's directory entry.

const std = @import("std");
const builtin = @import("builtin");

const workers = @import("../workers/root.zig");

pub const File = std.Io.File;

pub const Open = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions = .{},
    direct: bool = false,

    pub fn run(self: *@This(), io: std.Io) anyerror!File {
        if (self.direct and (self.flags.path_only or
            (builtin.os.tag == .windows and self.flags.lock != .none)))
            return error.UnsupportedOperation;
        const file = try self.dir.openFile(io, self.path, self.flags);
        errdefer file.close(io);
        if (builtin.os.tag == .windows and self.flags.lock == .none and !self.flags.path_only)
            return @import("windows.zig").upgrade(io, file, self.flags.isRead(), self.flags.isWrite(), self.direct);
        if (self.direct) try setDirect(file);
        return file;
    }
});

pub const Create = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions = .{},
    direct: bool = false,

    pub fn run(self: *@This(), io: std.Io) anyerror!File {
        if (self.direct and builtin.os.tag == .windows and self.flags.lock != .none)
            return error.UnsupportedOperation;
        const file = try self.dir.createFile(io, self.path, self.flags);
        errdefer file.close(io);
        if (builtin.os.tag == .windows and self.flags.lock == .none)
            return @import("windows.zig").upgrade(io, file, self.flags.read, true, self.direct);
        if (self.direct) try setDirect(file);
        return file;
    }
});

pub const ReadAt = workers.Pool.Work(struct {
    file: File,
    buffer: []u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) anyerror!usize {
        if (self.offset > std.math.maxInt(i64)) return error.InvalidArgument;
        if (builtin.os.tag == .linux)
            return linuxTransfer(.read_at, self.file, self.buffer, self.offset);
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").transfer(.read_at, self.file, self.buffer, self.offset);
        return self.file.readPositional(io, &.{self.buffer}, self.offset);
    }
});

pub const WriteAt = workers.Pool.Work(struct {
    file: File,
    buffer: []const u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) anyerror!usize {
        if (self.offset > std.math.maxInt(i64)) return error.InvalidArgument;
        if (builtin.os.tag == .linux)
            return linuxTransfer(.write_at, self.file, self.buffer, self.offset);
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").transfer(.write_at, self.file, self.buffer, self.offset);
        return self.file.writePositional(io, &.{self.buffer}, self.offset);
    }
});

pub const Sync = workers.Pool.Work(struct {
    file: File,
    data_only: bool = false,

    pub fn run(self: *@This(), io: std.Io) anyerror!void {
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").sync(self.file);
        if (builtin.os.tag == .linux and self.data_only) {
            const linux = std.os.linux;
            while (true) {
                switch (linux.errno(linux.fdatasync(self.file.handle))) {
                    .SUCCESS => return,
                    .INTR => continue,
                    else => |err| return @import("native.zig").mapError(.sync, err),
                }
            }
        }
        return self.file.sync(io);
    }
});

pub const Allocate = workers.Pool.Work(struct {
    file: File,
    offset: u64 = 0,
    len: u64,

    pub fn run(self: *@This(), io: std.Io) anyerror!void {
        if (self.offset > std.math.maxInt(i64) or self.len > std.math.maxInt(i64) - self.offset)
            return error.InvalidArgument;
        if (self.len == 0) return;
        switch (builtin.os.tag) {
            .linux => {
                const linux = std.os.linux;
                while (true) {
                    switch (linux.errno(linux.fallocate(self.file.handle, 0, @intCast(self.offset), @intCast(self.len)))) {
                        .SUCCESS => return,
                        .INTR => continue,
                        else => |err| return @import("native.zig").mapError(.write_at, err),
                    }
                }
            },
            .macos => return macosAllocate(io, self.file, self.offset + self.len),
            .windows => return @import("windows.zig").allocate(self.file, self.offset + self.len),
            else => return error.UnsupportedOperation,
        }
    }
});

pub const Close = workers.Pool.Work(struct {
    file: File,

    pub fn run(self: *@This(), io: std.Io) error{}!void {
        self.file.close(io);
    }
});

fn setDirect(file: File) !void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            const flags = while (true) {
                const rc = linux.fcntl(file.handle, linux.F.GETFL, 0);
                switch (linux.errno(rc)) {
                    .SUCCESS => break rc,
                    .INTR => continue,
                    else => |err| return @import("native.zig").mapError(.sync, err),
                }
            };
            while (true) {
                const rc = linux.fcntl(file.handle, linux.F.SETFL, flags | @as(u32, @bitCast(linux.O{ .DIRECT = true })));
                switch (linux.errno(rc)) {
                    .SUCCESS => return,
                    .INTR => continue,
                    .INVAL => return error.UnsupportedOperation,
                    else => |err| return @import("native.zig").mapError(.sync, err),
                }
            }
        },
        .macos => while (true) {
            switch (std.c.errno(std.c.fcntl(file.handle, std.c.F.NOCACHE, @as(c_int, 1)))) {
                .SUCCESS => return,
                .INTR => continue,
                else => |err| return @import("native.zig").mapError(.sync, err),
            }
        },
        else => return error.UnsupportedOperation,
    }
}

fn linuxTransfer(
    comptime kind: @import("../event/root.zig").Kind,
    file: File,
    buffer: if (kind == .read_at) []u8 else []const u8,
    offset: u64,
) !usize {
    if (buffer.len == 0) return 0;
    const linux = std.os.linux;
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
            else => |err| return @import("native.zig").mapError(kind, err),
        }
    }
}

fn macosAllocate(io: std.Io, file: File, end: u64) !void {
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.UnsupportedOperation;
    if (stat.size > std.math.maxInt(i64)) return error.InvalidArgument;

    // F_PREALLOCATE appends at physical EOF; it cannot promise to fill existing holes.
    var offset: u64 = 0;
    while (offset < stat.size) {
        var extent: extern struct {
            flags: u32 = 0,
            contiguous: i64 align(4),
            device_offset: i64 align(4),
        } = .{ .contiguous = @intCast(stat.size - offset), .device_offset = @intCast(offset) };
        comptime std.debug.assert(@sizeOf(@TypeOf(extent)) == 20);
        const rc = std.c.fcntl(file.handle, std.c.F.LOG2PHYS_EXT, @intFromPtr(&extent));
        switch (std.c.errno(rc)) {
            .SUCCESS => {},
            .INTR => continue,
            .INVAL, .OPNOTSUPP, .NOSYS => return error.UnsupportedOperation,
            else => |err| return @import("native.zig").mapError(.write_at, err),
        }
        if (extent.device_offset < 0 or extent.contiguous <= 0) return error.UnsupportedOperation;
        offset += @min(stat.size - offset, @as(u64, @intCast(extent.contiguous)));
    }
    if (end <= stat.size) return;
    const F_ALLOCATEALL = 0x4;
    const F_PEOFPOSMODE = 3;
    var store: extern struct {
        flags: u32 = F_ALLOCATEALL,
        position: c_int = F_PEOFPOSMODE,
        offset: i64 = 0,
        length: i64,
        bytes_allocated: i64 = 0,
    } = .{ .length = @intCast(end - stat.size) };
    while (true) {
        switch (std.c.errno(std.c.fcntl(file.handle, std.c.F.PREALLOCATE, @intFromPtr(&store)))) {
            .SUCCESS => break,
            .INTR => continue,
            else => |err| return @import("native.zig").mapError(.write_at, err),
        }
    }
    if (store.bytes_allocated < store.length) return error.UnsupportedOperation;
    while (true) {
        switch (std.c.errno(std.c.ftruncate(file.handle, @intCast(end)))) {
            .SUCCESS => return,
            .INTR => continue,
            else => |err| return @import("native.zig").mapError(.write_at, err),
        }
    }
}
