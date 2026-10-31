//! Pool-backed file I/O; pin paths/buffers until ready, even after cancel.
//! Returned handles remain caller-owned. Close only after all I/O on the handle;
//! a canceled task's Close never runs, so use File.close in cleanup.
//! WriteAt may be short; durability needs Sync, not just WriteAt or Close.
//! Sync does not persist a new file's directory entry.

const std = @import("std");
const builtin = @import("builtin");

const workers = @import("../workers/root.zig");
const DirectFile = @import("DirectFile.zig");
const linux = @import("linux.zig");
const macos = @import("macos.zig");
const windows = @import("windows.zig");

pub const File = std.Io.File;

pub const Open = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) anyerror!File {
        return openFile(io, self.dir, self.path, self.flags, false);
    }
});

pub const Create = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) anyerror!File {
        return createFile(io, self.dir, self.path, self.flags, false);
    }
});

pub const OpenDirect = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions = .{},
    /// Used only when the OS cannot report the alignment.
    fallback: ?DirectFile.Alignment = null,

    pub fn run(self: *@This(), io: std.Io) anyerror!DirectFile {
        const file = try openFile(io, self.dir, self.path, self.flags, true);
        errdefer file.close(io);

        return DirectFile.init(file, self.fallback);
    }
});

pub const CreateDirect = workers.Pool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions = .{},
    /// Used only when the OS cannot report the alignment.
    fallback: ?DirectFile.Alignment = null,

    pub fn run(self: *@This(), io: std.Io) anyerror!DirectFile {
        const file = try createFile(io, self.dir, self.path, self.flags, true);
        errdefer file.close(io);

        return DirectFile.init(file, self.fallback);
    }
});

pub const ReadAt = workers.Pool.Work(struct {
    file: File,
    buffer: []u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) anyerror!usize {
        if (self.offset > std.math.maxInt(i64)) return error.InvalidArgument;

        if (builtin.os.tag == .linux)
            return linux.transfer(.read_at, self.file, self.buffer, self.offset);
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return windows.transfer(.read_at, self.file, self.buffer, self.offset);

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
            return linux.transfer(.write_at, self.file, self.buffer, self.offset);
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return windows.transfer(.write_at, self.file, self.buffer, self.offset);

        return self.file.writePositional(io, &.{self.buffer}, self.offset);
    }
});

pub const Sync = workers.Pool.Work(struct {
    file: File,
    data_only: bool = false,

    pub fn run(self: *@This(), io: std.Io) anyerror!void {
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return windows.sync(self.file);
        if (builtin.os.tag == .linux and self.data_only)
            return linux.syncData(self.file);

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
            .linux => return linux.allocate(self.file, self.offset, self.len),
            .macos => return macos.allocate(io, self.file, self.offset, self.offset + self.len),
            .windows => return windows.allocate(self.file, self.offset + self.len),
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

fn openFile(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions,
    direct: bool,
) anyerror!File {
    if (direct and (flags.path_only or (builtin.os.tag == .windows and flags.lock != .none)))
        return error.UnsupportedOperation;

    const file = try dir.openFile(io, path, flags);
    errdefer file.close(io);

    if (builtin.os.tag == .windows and flags.lock == .none and !flags.path_only)
        return windows.upgrade(io, file, flags.isRead(), flags.isWrite(), direct);
    if (direct) try setDirect(file);

    return file;
}

fn createFile(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions,
    direct: bool,
) anyerror!File {
    if (direct and builtin.os.tag == .windows and flags.lock != .none)
        return error.UnsupportedOperation;

    const file = try dir.createFile(io, path, flags);
    errdefer file.close(io);

    if (builtin.os.tag == .windows and flags.lock == .none)
        return windows.upgrade(io, file, flags.read, true, direct);
    if (direct) try setDirect(file);

    return file;
}

fn setDirect(file: File) !void {
    switch (builtin.os.tag) {
        .linux => return linux.setDirect(file),
        .macos => return macos.setDirect(file),
        else => return error.UnsupportedOperation,
    }
}
