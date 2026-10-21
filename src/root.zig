//! velo: stackless coroutines and I/O.

pub const coroutines = @import("coroutines/root.zig");
pub const io = @import("io/root.zig");
pub const fs = @import("fs/root.zig");

test {
    const testing = @import("std").testing;

    testing.refAllDecls(@This());

    inline for (.{
        io.Loop,
        io.Connect,
        io.Accept,
        io.Send,
        io.Recv,
        io.SendTo,
        io.RecvFrom,
        io.Close,
        io.Timeout,
    }) |T| testing.refAllDecls(T);
}
