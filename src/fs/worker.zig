//! WorkerPool-backed file I/O; pin paths/buffers until ready, even after cancel.
//! Returned handles remain caller-owned. Close only after all I/O on the handle;
//! a canceled task's Close never runs, so use File.close in cleanup.
//! WriteAt may be short; durability needs Sync, not just WriteAt or Close.
//! Sync does not persist a new file's directory entry.

const std = @import("std");
const builtin = @import("builtin");

const WorkerPool = @import("../io/WorkerPool.zig");

pub const File = std.Io.File;

pub const Open = WorkerPool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) File.OpenError!File {
        const file = try self.dir.openFile(io, self.path, self.flags);
        if (builtin.os.tag == .windows and self.flags.lock == .none and !self.flags.path_only)
            return @import("windows.zig").upgrade(io, file, self.flags.isRead(), self.flags.isWrite());
        return file;
    }
});

pub const Create = WorkerPool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) File.OpenError!File {
        const file = try self.dir.createFile(io, self.path, self.flags);
        if (builtin.os.tag == .windows and self.flags.lock == .none)
            return @import("windows.zig").upgrade(io, file, self.flags.read, true);
        return file;
    }
});

pub const ReadAt = WorkerPool.Work(struct {
    file: File,
    buffer: []u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) anyerror!usize {
        if (self.offset > std.math.maxInt(i64)) return error.InvalidArgument;
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").transfer(.read_at, self.file, self.buffer, self.offset);
        return self.file.readPositional(io, &.{self.buffer}, self.offset);
    }
});

pub const WriteAt = WorkerPool.Work(struct {
    file: File,
    buffer: []const u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) anyerror!usize {
        if (self.offset > std.math.maxInt(i64)) return error.InvalidArgument;
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").transfer(.write_at, self.file, self.buffer, self.offset);
        return self.file.writePositional(io, &.{self.buffer}, self.offset);
    }
});

pub const Sync = WorkerPool.Work(struct {
    file: File,

    pub fn run(self: *@This(), io: std.Io) anyerror!void {
        if (builtin.os.tag == .windows and self.file.flags.nonblocking)
            return @import("windows.zig").sync(self.file);
        return self.file.sync(io);
    }
});

pub const Close = WorkerPool.Work(struct {
    file: File,

    pub fn run(self: *@This(), io: std.Io) error{}!void {
        self.file.close(io);
    }
});
