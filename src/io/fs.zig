//! WorkerPool-backed file I/O; pin paths/buffers until ready, even after cancel.
//! Returned handles remain caller-owned. Close only after all I/O on the handle;
//! a canceled task's Close never runs, so use File.close in cleanup.
//! WriteAt may be short; durability needs Sync, not just WriteAt or Close.
//! Sync does not persist a new file's directory entry.

const std = @import("std");

const WorkerPool = @import("WorkerPool.zig");

pub const File = std.Io.File;

pub const Open = WorkerPool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.OpenFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) File.OpenError!File {
        return self.dir.openFile(io, self.path, self.flags);
    }
});

pub const Create = WorkerPool.Work(struct {
    dir: std.Io.Dir,
    path: []const u8,
    flags: std.Io.Dir.CreateFileOptions = .{},

    pub fn run(self: *@This(), io: std.Io) File.OpenError!File {
        return self.dir.createFile(io, self.path, self.flags);
    }
});

pub const ReadAt = WorkerPool.Work(struct {
    file: File,
    buffer: []u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) File.ReadPositionalError!usize {
        return self.file.readPositional(io, &.{self.buffer}, self.offset);
    }
});

pub const WriteAt = WorkerPool.Work(struct {
    file: File,
    buffer: []const u8,
    offset: u64 = 0,

    pub fn run(self: *@This(), io: std.Io) File.WritePositionalError!usize {
        return self.file.writePositional(io, &.{self.buffer}, self.offset);
    }
});

pub const Sync = WorkerPool.Work(struct {
    file: File,

    pub fn run(self: *@This(), io: std.Io) File.SyncError!void {
        return self.file.sync(io);
    }
});

pub const Close = WorkerPool.Work(struct {
    file: File,

    pub fn run(self: *@This(), io: std.Io) error{}!void {
        self.file.close(io);
    }
});
