//! Bounded copies between regular files and TCP streams. Linux tries copy_file_range
//! (same-filesystem file/file), sendfile (file/stream), or splice through a private pipe (stream input).
//! Other platforms, unsupported operations and socket backpressure use ReadAt/WriteAt
//! and Recv/Send. Kernel-copy calls run in bounded worker jobs, never on the loop thread.
//!
//! Stops at `len` or source EOF and returns bytes written. `read` and `written` retain
//! progress on error; `buffered()` exposes consumed but unwritten input, including after
//! cancellation. File positions are unchanged. Handles remain caller-owned and must
//! outlive the transfer. File endpoints must refer to different files, including through
//! aliases; exclude source modification until a zero-copy receiver consumes the data.
//! Append-only destinations are unsupported. Direct-I/O callers must supply suitably
//! aligned buffers, lengths and offsets.
//!
//! Pin the transfer, pool and scratch buffer until ready. Cancellation drains the active
//! child; claimed worker jobs return their actual progress. Stream deadlines select the
//! buffered path and apply to socket operations, not filesystem jobs. Linux makes stream
//! sockets nonblocking before kernel copies; this does not change their ownership.

const std = @import("std");
const builtin = @import("builtin");
const co = @import("../coroutines/root.zig");
const fs = @import("../fs/root.zig");
const Pool = @import("../workers/Pool.zig");
const io = @import("root.zig");
const linux = @import("transfer_linux.zig");
const assert = std.debug.assert;
const Transfer = @This();

pub const Endpoint = union(enum) {
    file: struct { file: fs.File, offset: u64 = 0 },
    stream: io.Stream,
};

pub const Options = struct {
    source: Endpoint,
    destination: Endpoint,
    len: u64,
    /// Nonempty scratch space for fallback; chunk size is at most 64 KiB.
    buffer: []u8,
};

pub const Result = anyerror!u64;

pool: *Pool,
options: Options,
read: u64 = 0,
written: u64 = 0,
filled: usize = 0,
sent: usize = 0,
fast: bool,
canceled: bool = false,
started: bool = false,
// Inspect only the tag while a worker may be writing the child's result/post fields.
state: State = .idle,
child: Child = undefined,

const State = enum { idle, prepare, native, read_file, read_stream, write_file, write_stream, done };
const Child = union {
    prepare: Pool.Work(Prepare),
    native: Pool.Work(Native),
    read_file: fs.ReadAt,
    read_stream: io.Recv,
    write_file: fs.WriteAt,
    write_stream: io.Send,
};

/// `co.Frame(io.Transfer).init(.init(&pool, .{ .source = .{ .file = .{ .file = file } },
/// .destination = .{ .stream = stream }, .len = size, .buffer = &scratch }))`.
pub fn init(pool: *Pool, options: Options) Transfer {
    return .{
        .pool = pool,
        .options = options,
        .fast = builtin.os.tag == .linux and !hasDeadline(options.source) and !hasDeadline(options.destination),
    };
}

pub fn buffered(self: *const Transfer) []const u8 {
    return self.options.buffer[self.sent..self.filled];
}

pub fn cancel(self: *Transfer) void {
    if (self.canceled or self.state == .done) return;
    self.canceled = true;
    switch (self.state) {
        .idle, .done => {},
        inline else => |tag| self.active(tag).cancel(),
    }
}

pub fn deinit(self: *Transfer) void {
    assert(self.state == .idle or self.state == .done);
}

pub fn poll(self: *Transfer, task: *co.Task) co.Poll(Result) {
    assert(self.state != .done);
    assert(task.executor == &self.pool.loop.executor);
    if (task.cancel_requested or self.pool.loop.control.isShuttingDown()) self.cancel();

    while (true) switch (self.state) {
        .idle => {
            if (self.started and self.written == self.options.len) return self.finish(self.written);
            if (self.canceled) return self.finish(error.OperationCanceled);
            if (!self.started) {
                self.validate() catch |err| return self.finish(err);
                self.started = true;
                if (self.options.len == 0) return self.finish(0);
                self.arm(.prepare, .init(self.pool, .{ .options = self.options, .fast = self.fast }));
            } else if (self.sent < self.filled) {
                const bytes = self.options.buffer[self.sent..self.filled];
                switch (self.options.destination) {
                    .file => |f| self.arm(.write_file, .init(self.pool, .{ .file = f.file, .offset = f.offset + self.written, .buffer = bytes })),
                    .stream => |stream| self.arm(.write_stream, stream.send(bytes)),
                }
            } else {
                self.filled = 0;
                self.sent = 0;
                const count: usize = @intCast(@min(self.options.len - self.read, self.options.buffer.len, 64 * 1024));
                if (self.fast) {
                    self.arm(.native, .init(self.pool, .{
                        .source = advanced(self.options.source, self.read),
                        .destination = advanced(self.options.destination, self.written),
                        .buffer = self.options.buffer[0..count],
                    }));
                } else {
                    const bytes = self.options.buffer[0..count];
                    switch (self.options.source) {
                        .file => |f| self.arm(.read_file, .init(self.pool, .{ .file = f.file, .offset = f.offset + self.read, .buffer = bytes })),
                        .stream => |stream| self.arm(.read_stream, stream.recv(bytes)),
                    }
                }
            }
        },
        .prepare => {
            const result = self.active(.prepare).poll(task).value() orelse return .pending;
            self.state = .idle;
            self.fast = result catch |err| return self.finish(err);
        },
        .native => {
            const result = self.active(.native).poll(task).value() orelse return .pending;
            self.state = .idle;
            const progress = result catch |err| return self.finish(err);
            self.read += progress.read;
            self.written += progress.written;
            self.filled = progress.read - progress.written;
            self.fast = !progress.fallback;
            if (progress.failure) |err| return self.finish(err);
            if (progress.eof) return self.finish(self.written);
        },
        inline .read_file, .read_stream => |tag| {
            const result = self.active(tag).poll(task).value() orelse return .pending;
            self.state = .idle;
            const n = result catch |err| return self.finish(err);
            if (n == 0) return self.finish(self.written);
            self.read += n;
            self.filled = n;
        },
        inline .write_file, .write_stream => |tag| {
            const result = self.active(tag).poll(task).value() orelse return .pending;
            self.state = .idle;
            const n = result catch |err| return self.finish(err);
            if (n == 0) return self.finish(error.WriteZero);
            self.written += n;
            self.sent += n;
        },
        .done => unreachable,
    };
}

fn active(self: *Transfer, comptime tag: State) *@FieldType(Child, @tagName(tag)) {
    assert(self.state == tag);
    return field: {
        // The separate tag guards access without Zig's payload-copying union safety check.
        @setRuntimeSafety(false);
        break :field &@field(self.child, @tagName(tag));
    };
}

fn arm(self: *Transfer, comptime tag: State, child: @FieldType(Child, @tagName(tag))) void {
    self.child = @unionInit(Child, @tagName(tag), child);
    self.state = tag;
}

fn finish(self: *Transfer, result: Result) co.Poll(Result) {
    self.state = .done;
    return .{ .ready = result };
}

fn validate(self: *const Transfer) !void {
    if (self.options.len != 0 and self.options.buffer.len == 0) return error.InvalidArgument;
    for ([_]Endpoint{ self.options.source, self.options.destination }) |endpoint| switch (endpoint) {
        .file => |f| if (f.offset > std.math.maxInt(i64) or self.options.len > std.math.maxInt(i64) - f.offset)
            return error.InvalidArgument,
        .stream => |stream| if (stream.loop != self.pool.loop) return error.InvalidArgument,
    };
    if (self.options.source == .file and self.options.destination == .file) {
        const a = self.options.source.file;
        const b = self.options.destination.file;
        if (self.options.len != 0 and a.file.handle == b.file.handle)
            return error.InvalidArgument;
    }
}

fn hasDeadline(endpoint: Endpoint) bool {
    return switch (endpoint) {
        .file => false,
        .stream => |stream| stream.deadline != null,
    };
}

fn advanced(endpoint: Endpoint, count: u64) Endpoint {
    return switch (endpoint) {
        .file => |f| .{ .file = .{ .file = f.file, .offset = f.offset + count } },
        .stream => endpoint,
    };
}

const Prepare = struct {
    options: Options,
    fast: bool,

    pub fn run(self: *Prepare, threaded: std.Io) !bool {
        for ([_]Endpoint{ self.options.source, self.options.destination }) |endpoint| switch (endpoint) {
            .file => |f| if ((try f.file.stat(threaded)).kind != .file) return error.UnsupportedOperation,
            .stream => |stream| if (builtin.os.tag == .linux and self.fast) try linux.nonblocking(stream.fd),
        };
        if (builtin.os.tag == .linux and self.options.destination == .file) {
            try linux.checkDestination(self.options.destination.file.file.handle);
            if (self.options.source == .file) {
                const same_filesystem = try linux.checkFiles(self.options);
                return self.fast and same_filesystem;
            }
        }
        return self.fast;
    }
};

pub const Progress = struct {
    read: usize = 0,
    written: usize = 0,
    fallback: bool = false,
    eof: bool = false,
    failure: ?anyerror = null,
};

const Native = struct {
    source: Endpoint,
    destination: Endpoint,
    buffer: []u8,

    pub fn run(self: *Native, _: std.Io) !Progress {
        if (builtin.os.tag == .linux) return linux.copy(self.source, self.destination, self.buffer);
        return .{ .fallback = true };
    }
};
