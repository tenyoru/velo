const std = @import("std");
const c = std.c;

const Sigval = extern union { int: c_int, ptr: ?*anyopaque };
const Sigevent = extern struct {
    notify: c_int,
    signo: c_int,
    value: Sigval,
    function: ?*const fn (Sigval) callconv(.c) void,
    attributes: ?*c.pthread_attr_t,
};

// std.c lacks these bindings; the layout is the 64-bit Darwin sys/aio.h ABI.
pub const Request = extern struct {
    fd: c_int,
    offset: c.off_t,
    buffer: ?*volatile anyopaque,
    length: usize,
    priority: c_int,
    event: Sigevent,
    opcode: c_int,
};

extern "c" fn aio_read(request: *Request) c_int;
extern "c" fn aio_write(request: *Request) c_int;

const SIGEV_KEVENT = 4;

comptime {
    const assert = std.debug.assert;
    assert(@sizeOf(Sigval) == 8 and @alignOf(Sigval) == 8);
    assert(@sizeOf(Sigevent) == 32 and @alignOf(Sigevent) == 8);
    assert(@offsetOf(Sigevent, "notify") == 0);
    assert(@offsetOf(Sigevent, "signo") == 4);
    assert(@offsetOf(Sigevent, "value") == 8);
    assert(@offsetOf(Sigevent, "function") == 16);
    assert(@offsetOf(Sigevent, "attributes") == 24);
    assert(@sizeOf(Request) == 80 and @alignOf(Request) == 8);
    assert(@offsetOf(Request, "fd") == 0);
    assert(@offsetOf(Request, "offset") == 8);
    assert(@offsetOf(Request, "buffer") == 16);
    assert(@offsetOf(Request, "length") == 24);
    assert(@offsetOf(Request, "priority") == 32);
    assert(@offsetOf(Request, "event") == 40);
    assert(@offsetOf(Request, "opcode") == 72);
}

pub fn prepare(request: *Request, kqueue: c.fd_t, fd: c.fd_t, buffer: []const u8, offset: u64, context: *anyopaque) void {
    request.* = std.mem.zeroes(Request);
    request.fd = fd;
    request.offset = @intCast(offset);
    request.buffer = @ptrCast(@constCast(buffer.ptr));
    request.length = @min(buffer.len, std.math.maxInt(i32));
    request.event.notify = SIGEV_KEVENT;
    request.event.signo = kqueue;
    request.event.value.ptr = context;
}

pub fn submit(request: *Request, write: bool) c.E {
    while (true) {
        const e = c.errno(if (write) aio_write(request) else aio_read(request));
        if (e != .INTR) return e;
    }
}

pub fn rejection(request: *const Request, write: bool) c.E {
    // XNU folds descriptor/access/type validation failures into submission EAGAIN.
    const flags: c.O = @bitCast(while (true) {
        const rc = c.fcntl(request.fd, c.F.GETFL);
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break rc;
    });
    if ((write and flags.ACCMODE == .RDONLY) or (!write and flags.ACCMODE == .WRONLY))
        return .BADF;

    var stat: c.Stat = undefined;
    while (true) {
        const e = c.errno(c.fstat(request.fd, &stat));
        if (e == .INTR) continue;
        if (e != .SUCCESS) return e;
        break;
    }
    return switch (stat.mode & c.S.IFMT) {
        c.S.IFIFO, c.S.IFSOCK => .SPIPE,
        else => .AGAIN,
    };
}

pub fn probe(kqueue: c.fd_t) bool {
    if (kqueue <= 0) return false;

    const fd = while (true) {
        const rc = c.open("/dev/null", .{ .CLOEXEC = true });
        const e = c.errno(rc);
        if (e == .INTR) continue;
        if (e != .SUCCESS) return false;
        break rc;
    };
    defer _ = c.close(fd);

    var byte: [1]u8 = undefined;
    var request: Request = undefined;
    prepare(&request, kqueue, fd, byte[0..0], 0, &request);
    if (submit(&request, false) != .SUCCESS) return false;

    // SIGEV_KEVENT reaps on delivery, not aio_return; the accepted stack request must drain.
    while (true) {
        var events: [1]c.kevent64_s = undefined;
        const count = c.kevent64(kqueue, &.{}, 0, &events, events.len, .{}, null);
        const e = c.errno(count);
        if (e == .INTR) continue;
        if (e != .SUCCESS) std.debug.panic("AIO probe drain failed: {t}", .{e});
        if (count == 0) continue;
        const event = events[0];
        if (event.filter != c.EVFILT.AIO or event.ident != @intFromPtr(&request) or
            event.udata != @intFromPtr(&request))
            std.debug.panic("unexpected AIO probe completion", .{});
        return event.flags & c.EV.ERROR == 0 and event.ext[0] == 0 and event.ext[1] == 0;
    }
}
