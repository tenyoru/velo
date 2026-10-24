//! `Server(connect)`: a TCP server coroutine over a listener, an acceptor `Machine` and a
//! `Scope` of connections in caller-owned slots.
//!
//! `connect: fn (Context, io.Stream) Handler` builds each connection's coroutine, which
//! returns `void` or `E!void`. The server closes the stream after its handler finishes;
//! handlers must not close it. Handler errors end only that connection and are counted.
//! A slot is reserved before each accept, so a full server stops accepting instead of
//! holding sockets. Transient accept failures (e.g. descriptor exhaustion) back off from
//! 5 ms to 1 s; invalid-listener and backoff-timer errors are returned after draining.
//! Cancellation, including loop shutdown, stops accepting, cancels connections and drains.
//! Pin the server and slots from spawn until ready is consumed; `deinit` afterwards.

const std = @import("std");
const io = @import("root.zig");
const co = @import("../coroutines/root.zig");

pub fn Server(comptime connect: anytype) type {
    const signature = "Server needs `fn (Context, io.Stream) Handler`, found " ++ @typeName(@TypeOf(connect));
    const info = switch (@typeInfo(@TypeOf(connect))) {
        .@"fn" => |info| info,
        else => @compileError(signature),
    };
    if (info.param_types.len != 2 or info.param_types[0] == null or
        info.param_types[1] != io.Stream or info.return_type == null)
        @compileError(signature);

    const Context = info.param_types[0].?;
    const Handler = info.return_type.?;
    const R = co.ResultOf(Handler);
    if (!@hasDecl(Handler, "cancel")) @compileError("Server handlers need `cancel`: " ++ @typeName(Handler));
    switch (@typeInfo(R)) {
        .void => {},
        .error_union => |eu| if (eu.payload != void)
            @compileError("Server handlers return void or E!void: " ++ @typeName(Handler)),
        else => @compileError("Server handlers return void or E!void: " ++ @typeName(Handler)),
    }

    return struct {
        loop: *io.Loop,
        listener: io.Listener,
        context: Context,
        connections: Connections,
        acceptor: co.Machine(Acceptor),
        stats: Stats = .{},
        /// Most recent handler or transient accept error.
        last_error: ?anyerror = null,

        const Self = @This();
        const Connections = co.Scope(Connection);
        pub const Slot = Connections.Slot;
        pub const Result = io.Error!void;

        pub const Options = io.Listener.Options;

        pub const Stats = struct {
            accepted: usize = 0,
            /// Connections whose handler returned an error.
            failed: usize = 0,
            /// Transient accept failures that triggered a backoff.
            accept_errors: usize = 0,
        };

        const Connection = struct {
            handler: Handler,
            stream: io.Stream,
            server: *Self,

            pub fn poll(self: *Connection, task: *co.Task) co.Poll(void) {
                const result = self.handler.poll(task).value() orelse return .pending;

                // The finished handler no longer borrows the stream.
                self.stream.close();

                if (R != void) {
                    result catch |err| {
                        self.server.stats.failed += 1;
                        self.server.last_error = err;
                    };
                }

                return .{ .ready = {} };
            }

            pub fn cancel(self: *Connection) void {
                self.handler.cancel();
            }
        };

        const Acceptor = struct {
            server: *Self,
            slot: ?*Slot = null,
            backoff_ms: u32 = 0,
            fatal: ?io.Error = null,

            pub const Result = io.Error!void;
            pub const State = union(enum) {
                reserve: Connections.Reserve,
                accept: io.Accept,
                backoff: io.Timeout,
                drain: Connections.Wait,
            };
            const Next = co.Next(Acceptor);

            pub fn start(self: *Acceptor) Next {
                return .{ .go = .{ .reserve = self.server.connections.reserve() } };
            }

            pub fn reserve(self: *Acceptor, result: error{Canceled}!*Slot) Next {
                self.slot = result catch return self.stop();

                return .{ .go = .{ .accept = self.server.listener.accept(.{}) } };
            }

            pub fn accept(self: *Acceptor, result: io.Error!io.Socket) Next {
                const server = self.server;
                const slot = self.slot.?;
                self.slot = null;

                const fd = result catch |err| {
                    server.connections.release(slot);

                    switch (err) {
                        error.OperationCanceled => return self.stop(),
                        error.BadFileDescriptor, error.InvalidArgument, error.UnsupportedOperation => {
                            self.fatal = err;
                            return self.stop();
                        },
                        else => {
                            server.stats.accept_errors += 1;
                            server.last_error = err;
                            self.backoff_ms = std.math.clamp(self.backoff_ms * 2, 5, 1000);

                            return .{ .go = .{ .backoff = .init(server.loop, .{
                                .duration = io.milliseconds(self.backoff_ms),
                            }) } };
                        },
                    }
                };

                self.backoff_ms = 0;
                server.stats.accepted += 1;

                const stream: io.Stream = .init(server.loop, fd);
                server.connections.start(slot, .{
                    .handler = connect(server.context, stream),
                    .stream = stream,
                    .server = server,
                });

                return self.start();
            }

            pub fn backoff(self: *Acceptor, result: io.Error!void) Next {
                result catch |err| {
                    if (err != error.OperationCanceled) self.fatal = err;
                    return self.stop();
                };
                return self.start();
            }

            pub fn drain(self: *Acceptor, result: Connections.Error!void) Next {
                // Connections report through stats; Canceled is the expected outcome.
                result catch {};
                return .{ .done = if (self.fatal) |err| err else {} };
            }

            fn stop(self: *Acceptor) Next {
                self.server.connections.cancel();
                return .{ .go = .{ .drain = self.server.connections.wait() } };
            }
        };

        /// Binds and listens; `listener.address` reports the bound port.
        /// At least one slot is required; empty storage returns InvalidArgument.
        pub fn init(loop: *io.Loop, address: io.Address, context: Context, slots: []Slot, options: Options) io.Error!Self {
            if (slots.len == 0) return error.InvalidArgument;
            return .{
                .loop = loop,
                .listener = try .init(loop, address, .{ .backlog = options.backlog }),
                .context = context,
                .connections = .init(&loop.executor, slots),
                .acceptor = .init(.{ .server = undefined }),
            };
        }

        /// Requires the server to have finished, or never to have been spawned.
        pub fn deinit(self: *Self) void {
            self.connections.deinit();
            self.listener.deinit();
        }

        pub fn poll(self: *Self, task: *co.Task) co.Poll(Result) {
            // Pinned from the first poll on; children borrow this address.
            self.acceptor.ctx.server = self;
            return self.acceptor.poll(task);
        }

        pub fn cancel(self: *Self) void {
            self.acceptor.cancel();
        }
    };
}
