//! Worker-only kernel-copy attempts. Socket calls are nonblocking; any bytes left in
//! the private pipe are recovered into the caller's buffer before returning.

const std = @import("std");
const linux = std.os.linux;
const Transfer = @import("transfer.zig");
const mapError = @import("../fs/native.zig").mapError;
const Endpoint = Transfer.Endpoint;
const Progress = Transfer.Progress;

pub fn nonblocking(fd: linux.fd_t) !void {
    const flags = try getFlags(fd);
    if (flags.NONBLOCK) return;
    var changed = flags;
    changed.NONBLOCK = true;
    while (true) {
        const e = linux.errno(linux.fcntl(fd, linux.F.SETFL, @as(u32, @bitCast(changed))));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return mapError(.sync, e);
        return;
    }
}

pub fn checkDestination(fd: linux.fd_t) !void {
    if ((try getFlags(fd)).APPEND) return error.InvalidArgument;
}

pub fn checkFiles(options: Transfer.Options) !bool {
    const source = options.source.file;
    const destination = options.destination.file;
    var stats: [2]linux.Statx = undefined;
    for ([_]linux.fd_t{ source.file.handle, destination.file.handle }, &stats) |fd, *stat| {
        while (true) {
            const e = linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .INO = true }, stat));
            switch (e) {
                .SUCCESS => break,
                .INTR => continue,
                .NOSYS, .INVAL, .OPNOTSUPP => return false,
                else => return failure(e),
            }
        }
        if (!stat.mask.INO) return false;
    }
    // Linux 5.3–5.18 could report successful cross-filesystem copies without copying data.
    if (stats[0].dev_major != stats[1].dev_major or stats[0].dev_minor != stats[1].dev_minor) return false;
    if (stats[0].ino == stats[1].ino) return error.InvalidArgument;
    return true;
}

fn getFlags(fd: linux.fd_t) !linux.O {
    while (true) {
        const rc = linux.fcntl(fd, linux.F.GETFL, 0);
        switch (linux.errno(rc)) {
            .SUCCESS => return @bitCast(@as(u32, @intCast(rc))),
            .INTR => continue,
            else => |e| return mapError(.sync, e),
        }
    }
}

pub fn copy(source: Endpoint, destination: Endpoint, buffer: []u8) !Progress {
    var guard = if (destination == .stream) try Sigpipe.block() else null;
    defer if (guard) |*g| g.restore();

    if (source == .file) {
        var input: i64 = @intCast(source.file.offset);
        var output: i64 = if (destination == .file) @intCast(destination.file.offset) else 0;
        while (true) {
            const rc = switch (destination) {
                .file => |f| linux.copy_file_range(source.file.file.handle, &input, f.file.handle, &output, buffer.len, 0),
                .stream => |s| linux.sendfile(s.fd, source.file.file.handle, &input, buffer.len),
            };
            const e = linux.errno(rc);
            if (guard) |*g| g.generated = e == .PIPE;
            if (e == .INTR) continue;
            if (e == .SUCCESS) {
                // Some virtual files report zero despite readable data; confirm with a read.
                return if (rc == 0) .{ .fallback = true } else .{ .read = rc, .written = rc };
            }
            if (fallback(e) or e == .INVAL) return .{ .fallback = true };
            return failure(e);
        }
    }

    var pipe: [2]linux.fd_t = undefined;
    const e = linux.errno(linux.pipe2(&pipe, .{ .CLOEXEC = true, .NONBLOCK = true }));
    if (e != .SUCCESS) return failure(e);
    defer for (pipe) |fd| {
        _ = linux.close(fd);
    };

    const incoming = splice(source.stream.fd, null, pipe[1], null, buffer.len);
    const read_error = linux.errno(incoming);
    if (read_error != .SUCCESS) {
        if (fallback(read_error) or read_error == .INVAL) return .{ .fallback = true };
        return failure(read_error);
    }
    if (incoming == 0) return .{ .eof = true };

    var output: i64 = if (destination == .file) @intCast(destination.file.offset) else 0;
    const outgoing = switch (destination) {
        .file => |f| splice(pipe[0], null, f.file.handle, &output, incoming),
        .stream => |s| splice(pipe[0], null, s.fd, null, incoming),
    };
    const write_error = linux.errno(outgoing);
    if (guard) |*g| g.generated = write_error == .PIPE;
    const written = if (write_error == .SUCCESS) outgoing else 0;
    var progress: Progress = .{ .read = incoming, .written = written };
    if (written == incoming) return progress;

    // Never discard input already consumed by splice, even when the destination fails.
    var remaining = buffer[0 .. incoming - written];
    while (remaining.len != 0) {
        const rc = linux.read(pipe[0], remaining.ptr, remaining.len);
        switch (linux.errno(rc)) {
            .SUCCESS => {
                std.debug.assert(rc != 0);
                remaining = remaining[rc..];
            },
            .INTR => continue,
            else => @panic("velo: could not recover buffered splice input"),
        }
    }
    progress.fallback = true;
    if (write_error != .SUCCESS and !fallback(write_error) and write_error != .INVAL)
        progress.failure = failure(write_error);
    return progress;
}

fn splice(input: linux.fd_t, input_offset: ?*i64, output: linux.fd_t, output_offset: ?*i64, len: usize) usize {
    while (true) {
        const rc = linux.syscall6(.splice, @as(u32, @bitCast(input)), @intFromPtr(input_offset), @as(u32, @bitCast(output)), @intFromPtr(output_offset), len, 2); // SPLICE_F_NONBLOCK
        if (linux.errno(rc) != .INTR) return rc;
    }
}

fn fallback(e: linux.E) bool {
    return switch (e) {
        .NOSYS, .OPNOTSUPP, .XDEV, .AGAIN => true,
        else => false,
    };
}

fn failure(e: linux.E) anyerror {
    return if (e == .BADF) error.BadFileDescriptor else mapError(.write_at, e);
}

// sendfile/splice have no MSG_NOSIGNAL; suppress only newly generated SIGPIPE on this worker.
const Sigpipe = struct {
    mask: linux.sigset_t,
    old: linux.sigset_t,
    pending: bool,
    generated: bool = false,

    fn block() !Sigpipe {
        var self: Sigpipe = .{ .mask = linux.sigemptyset(), .old = undefined, .pending = false };
        linux.sigaddset(&self.mask, .PIPE);
        const e = linux.errno(linux.sigprocmask(linux.SIG.BLOCK, &self.mask, &self.old));
        if (e != .SUCCESS) return failure(e);
        var pending: linux.sigset_t = undefined;
        const pe = linux.errno(linux.syscall2(.rt_sigpending, @intFromPtr(&pending), linux.NSIG / 8));
        if (pe != .SUCCESS) {
            _ = linux.sigprocmask(linux.SIG.SETMASK, &self.old, null);
            return failure(pe);
        }
        self.pending = linux.sigismember(&pending, .PIPE);
        return self;
    }

    fn restore(self: *Sigpipe) void {
        if (self.generated and !self.pending) {
            const zero: linux.timespec = .{ .sec = 0, .nsec = 0 };
            while (true) {
                const rc = linux.syscall4(.rt_sigtimedwait, @intFromPtr(&self.mask), 0, @intFromPtr(&zero), linux.NSIG / 8);
                switch (linux.errno(rc)) {
                    .SUCCESS, .AGAIN => break,
                    .INTR => continue,
                    else => @panic("velo: could not consume worker SIGPIPE"),
                }
            }
        }
        if (linux.errno(linux.sigprocmask(linux.SIG.SETMASK, &self.old, null)) != .SUCCESS)
            @panic("velo: could not restore worker signal mask");
    }
};
