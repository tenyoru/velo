const builtin = @import("builtin");
const co = @import("../coroutines/root.zig");
const io = @import("../io/root.zig");
const worker = @import("worker.zig");

pub const ReadAt = Coroutine(.read_at, worker.ReadAt, usize);
pub const WriteAt = Coroutine(.write_at, worker.WriteAt, usize);
pub const Sync = Coroutine(.sync, worker.Sync, void);

fn Coroutine(comptime kind: io.Kind, comptime Worker: type, comptime T: type) type {
    return struct {
        inner: io.Operation.Coroutine(kind),

        const Self = @This();

        pub fn init(pool: *io.WorkerPool, options: @FieldType(Worker, "action")) Self {
            return .{ .inner = .init(pool.loop, if (kind == .sync)
                .{ .file = options.file }
            else
                .{ .file = options.file, .buffer = options.buffer, .offset = options.offset }) };
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(anyerror!T) {
            switch (self.inner.poll(task)) {
                .pending => return .pending,
                .ready => |result| {
                    // The shared lifecycle narrows socket errors; file errno must be mapped raw.
                    if (self.inner.primaryError()) |e| return .{ .ready = mapError(kind, e) };
                    return .{ .ready = result };
                },
            }
        }

        pub fn cancel(self: *Self) void {
            self.inner.cancel();
        }

        pub fn deinit(self: *Self) void {
            self.inner.deinit();
        }
    };
}

fn mapError(comptime kind: io.Kind, e: io.NativeError) anyerror {
    if (builtin.os.tag == .windows) return @import("windows.zig").mapError(kind, e);
    return switch (e) {
        .CANCELED => error.OperationCanceled,
        .BADF => switch (kind) {
            .read_at => error.NotOpenForReading,
            .write_at => error.NotOpenForWriting,
            else => error.BadFileDescriptor,
        },
        .IO => error.InputOutput,
        .NOSPC => error.NoSpaceLeft,
        .DQUOT => error.DiskQuota,
        .FBIG => error.FileTooBig,
        .INVAL => error.InvalidArgument,
        .ISDIR => error.IsDir,
        .SPIPE, .NXIO, .OVERFLOW => error.Unseekable,
        .AGAIN => error.WouldBlock,
        .NOMEM, .NOBUFS => error.SystemResources,
        .ACCES, .ROFS => error.AccessDenied,
        .PERM => error.PermissionDenied,
        .PIPE => error.BrokenPipe,
        .BUSY => error.DeviceBusy,
        .TXTBSY => error.FileBusy,
        .NODEV => error.NoDevice,
        .OPNOTSUPP, .NOSYS => error.UnsupportedOperation,
        .INTR => error.SignalInterrupt,
        else => error.Unexpected,
    };
}
