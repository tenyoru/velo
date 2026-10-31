//! Positional file I/O uses the loop's native capabilities, otherwise workers.
//! Pin paths, buffers, frames and the pool until ready is consumed, including after cancel.
//! Handles remain caller-owned; close only after consuming all operations on the file.
//! Canceled tasks skip Close, so use File.close in cleanup. Transfers may be short;
//! read returning zero means EOF or an empty buffer. Offsets must fit a signed 64-bit value.
//! Consume writes before Sync for full file-data/metadata durability, not directory entries.
//! `data_only` Sync (Linux fdatasync) skips metadata not needed to read the data back,
//! such as timestamps; other platforms run a full Sync.
//! Native reads/writes use io_uring, probed Darwin AIO/kqueue, or overlapped IOCP.
//! Sync uses workers outside io_uring; Open/Create/Close always use workers.
//! Windows Open/Create request asynchronous handles for unlocked regular files;
//! supplied synchronous handles use workers. Associated handles stay on one loop.
//! Use these transfer wrappers on IOCP-associated files, not std.Io.Threaded's APC path.
//! Submitted Darwin AIO drains to its original completion even after cancellation.
//! OpenDirect/CreateDirect return a DirectFile: Linux enables O_DIRECT, Windows requests
//! FILE_FLAG_NO_BUFFERING, and macOS uses the weaker F_NOCACHE cache hint, not O_DIRECT.
//! Direct setup never silently falls back; Windows locks/path-only opens and nonregular
//! handles are unsupported. Failed CreateDirect setup can leave the created file behind.
//! Direct does not imply durability.
//! Alignment comes from Linux statx(STATX_DIOALIGN) or Windows FILE_STORAGE_INFO. When
//! the OS reports none, setup uses the caller's `fallback` or fails with
//! UnsupportedOperation; 4096 is not a universal guarantee.
//! DirectFile transfers reject misalignment before submission: misaligned memory can
//! block forever in the kernel (Hyper-V storvsc retries it).
//! Allocate reserves backing storage for [offset, offset+len) and grows EOF to at
//! least offset+len without shrinking or overwriting data. It always uses workers;
//! offset+len must fit i64; len=0 does no I/O after validation. No zero-write emulation.
//! Exclude concurrent resize, hole punching or allocation changes on the file.
//! Failures may leave increased allocation or EOF behind.
//! Windows sparse/compressed files are unsupported. macOS requires queryable physical
//! extents in the requested existing range; growth requires them throughout the existing
//! file because F_PREALLOCATE extends from physical EOF. Holes/unavailable mappings are unsupported.

const std = @import("std");
const builtin = @import("builtin");

const co = @import("../coroutines/root.zig");
const Pool = @import("../workers/root.zig").Pool;

pub const worker = @import("worker.zig");
pub const File = worker.File;
pub const Open = worker.Open;
pub const Create = worker.Create;
pub const OpenDirect = worker.OpenDirect;
pub const CreateDirect = worker.CreateDirect;
pub const DirectFile = @import("DirectFile.zig");
pub const Close = worker.Close;
pub const Allocate = worker.Allocate;
pub const ReadAt = Dispatch("ReadAt", "read", usize);
pub const WriteAt = Dispatch("WriteAt", "write", usize);
pub const Sync = Dispatch("Sync", "sync", void);

fn Dispatch(comptime name: []const u8, comptime capability: []const u8, comptime T: type) type {
    const Worker = @field(worker, name);
    const Options = @FieldType(Worker, "action");
    const Native = @field(@import("native.zig"), name);

    return struct {
        tag: Tag,
        /// Pinned child storage; inspect `tag`, not the payload, while pending.
        storage: Storage,

        const Self = @This();
        const Tag = enum { worker, native };
        const Storage = union { worker: Worker, native: Native };

        pub fn init(pool: *Pool, options: Options) Self {
            if (@field(pool.loop.file_ops, capability) and
                (if (builtin.os.tag == .windows) options.file.flags.nonblocking else true))
                return .{ .tag = .native, .storage = .{ .native = Native.init(pool, options) } };

            return .{ .tag = .worker, .storage = .{ .worker = Worker.init(pool, options) } };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(anyerror!T) {
            switch (self.tag) {
                inline else => |tag| return self.active(tag).poll(task),
            }
        }

        pub fn cancel(self: *Self) void {
            switch (self.tag) {
                inline else => |tag| self.active(tag).cancel(),
            }
        }

        pub fn deinit(self: *Self) void {
            switch (self.tag) {
                inline else => |tag| self.active(tag).deinit(),
            }
        }

        fn active(self: *Self, comptime tag: Tag) *@FieldType(Storage, @tagName(tag)) {
            std.debug.assert(self.tag == tag);
            return field: {
                // Inspect only the separate tag while a worker may be publishing its result.
                @setRuntimeSafety(false);
                break :field &@field(self.storage, @tagName(tag));
            };
        }
    };
}

test {
    std.testing.refAllDecls(@This());
    _ = @import("tests/portable.zig");
    if (builtin.os.tag == .linux) _ = @import("tests/linux.zig");
    if (builtin.os.tag == .windows) _ = @import("tests/windows.zig");
    if (builtin.os.tag == .macos) _ = @import("tests/macos.zig");
}
