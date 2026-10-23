//! Pool-backed file I/O; pin paths/buffers until ready, even after cancel.
//! Returned handles remain caller-owned. Close only after all I/O on the handle;
//! a canceled task's Close never runs, so use File.close in cleanup.
//! WriteAt may be short; durability needs Sync, not just WriteAt or Close.
//! Sync does not persist a new file's directory entry.

const std = @import("std");
const builtin = @import("builtin");

const workers = @import("../workers/root.zig");
const linux = @import("linux.zig");
const macos = @import("macos.zig");
const windows = @import("windows.zig");

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

        if (builtin.os.tag == .windows and self.flags.lock == .none and !self.flags.path_only) {
            return windows.upgrade(
                io,
                file,
                self.flags.isRead(),
                self.flags.isWrite(),
                self.direct,
            );
        }
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
            return windows.upgrade(io, file, self.flags.read, true, self.direct);
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

fn setDirect(file: File) !void {
    switch (builtin.os.tag) {
        .linux => return linux.setDirect(file),
        .macos => return macos.setDirect(file),
        else => return error.UnsupportedOperation,
    }
}
