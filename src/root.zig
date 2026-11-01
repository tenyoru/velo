//! velo: stackless coroutines and I/O.

pub const coroutines = @import("coroutines/root.zig");
pub const event = @import("event/root.zig");
pub const net = @import("net/root.zig");
pub const workers = @import("workers/root.zig");
pub const fs = @import("fs/root.zig");

test {
    const testing = @import("std").testing;

    testing.refAllDecls(@This());

    inline for (.{
        event.Loop,
        event.Connect,
        event.Accept,
        event.Send,
        event.Recv,
        event.SendTo,
        event.RecvFrom,
        event.Close,
        event.Timeout,
    }) |T| testing.refAllDecls(T);
}
