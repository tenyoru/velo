//! Positional file I/O uses the loop's native capabilities, otherwise workers.
//! Pin paths, buffers, frames and the pool until ready is consumed, including after cancel.
//! Handles remain caller-owned; close only after consuming all operations on the file.
//! Canceled tasks skip Close, so use File.close in cleanup. Transfers may be short;
//! read returning zero means EOF or an empty buffer. Offsets must fit a signed 64-bit value.
//! Consume writes before Sync for full file-data/metadata durability, not directory entries.
//! Native reads/writes use io_uring, probed Darwin AIO/kqueue, or overlapped IOCP.
//! Sync uses workers outside io_uring; Open/Create/Close always use workers.
//! Windows Open/Create request asynchronous handles for unlocked regular files;
//! supplied synchronous handles use workers. Associated handles stay on one loop.
//! Use these transfer wrappers on IOCP-associated files, not std.Io.Threaded's APC path.
//! Submitted Darwin AIO drains to its original completion even after cancellation.

const std = @import("std");
const builtin = @import("builtin");
const co = @import("../coroutines/root.zig");
const Pool = @import("../workers/root.zig").Pool;

pub const worker = @import("worker.zig");
pub const File = worker.File;
pub const Open = worker.Open;
pub const Create = worker.Create;
pub const Close = worker.Close;
pub const ReadAt = Dispatch("ReadAt", "read", usize);
pub const WriteAt = Dispatch("WriteAt", "write", usize);
pub const Sync = Dispatch("Sync", "sync", void);

fn Dispatch(comptime name: []const u8, comptime capability: []const u8, comptime T: type) type {
    const Worker = @field(worker, name);
    const Options = @FieldType(Worker, "action");
    const Native = @field(@import("native.zig"), name);

    return union(enum) {
        worker: Worker,
        native: Native,

        const Self = @This();

        pub fn init(pool: *Pool, options: Options) Self {
            if (@field(pool.loop.file_ops, capability) and
                (if (builtin.os.tag == .windows) options.file.flags.nonblocking else true))
                return .{ .native = Native.init(pool, options) };
            return .{ .worker = Worker.init(pool, options) };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(anyerror!T) {
            switch (self.*) {
                .worker => |*work| return work.poll(task),
                .native => |*work| return work.poll(task),
            }
        }

        pub fn cancel(self: *Self) void {
            switch (self.*) {
                .worker => |*work| work.cancel(),
                .native => |*work| work.cancel(),
            }
        }

        pub fn deinit(self: *Self) void {
            switch (self.*) {
                .worker => |*work| work.deinit(),
                .native => |*work| work.deinit(),
            }
        }
    };
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("test.zig");
    if (builtin.os.tag == .windows) _ = @import("test_windows.zig");
    if (builtin.os.tag == .macos) _ = @import("test_macos.zig");
}
