//! Three client loops send typed requests to one server loop and await typed replies.
//! Mailboxes provide backpressure and wakeups; request/reply keeps borrowed reply
//! mailbox pointers alive until the server is finished with them.

const std = @import("std");
const builtin = @import("builtin");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const Group = velo.workers.Group;

const clients = 3;
const requests = 1000;
const Replies = io.Mailbox(u64);
const Request = struct { sequence: u64, replies: *Replies };
const Inbox = io.Mailbox(Request);

const Client = struct {
    pub const Result = anyerror!void;
    pub const State = union(enum) { send: Inbox.Send, receive: Replies.Recv };
    shared: *Shared,
    loop: *io.Loop,
    replies: *Replies,
    sequence: u64 = 0,

    pub fn start(self: *Client) co.Next(Client) {
        // Startup rollback can cancel this client before the server mailbox exists.
        if (self.loop.control.isShuttingDown()) {
            self.replies.close();
            return .{ .done = error.Canceled };
        }
        return .{ .go = .{ .send = self.shared.inbox.send(self.loop, .{
            .sequence = self.sequence,
            .replies = self.replies,
        }) } };
    }

    pub fn send(self: *Client, result: Inbox.Error!void) co.Next(Client) {
        result catch |err| return self.fail(err);
        return .{ .go = .{ .receive = self.replies.recv() } };
    }

    pub fn receive(self: *Client, result: Replies.Error!u64) co.Next(Client) {
        const value = result catch |err| return self.fail(err);
        if (value != self.sequence + 1) return self.fail(error.BadReply);
        self.sequence += 1;
        if (self.sequence != requests) return self.start();
        self.replies.close();
        if (self.shared.remaining.fetchSub(1, .acq_rel) == 1) self.shared.inbox.close();
        return .{ .done = {} };
    }

    fn fail(self: *Client, err: anyerror) co.Next(Client) {
        self.replies.close();
        self.shared.inbox.close();
        while (self.replies.tryRecv()) |_| {} else |_| {}
        return .{ .done = err };
    }
};

const Server = struct {
    pub const Result = anyerror!void;
    pub const State = union(enum) { receive: Inbox.Recv, reply: Replies.Send };
    shared: *Shared,
    loop: *io.Loop,
    current: ?Request = null,
    count: usize = 0,

    pub fn start(self: *Server) co.Next(Server) {
        return .{ .go = .{ .receive = self.shared.inbox.recv() } };
    }

    pub fn receive(self: *Server, result: Inbox.Error!Request) co.Next(Server) {
        const request = result catch |err| return if (err == error.Closed) .{ .done = {} } else self.fail(err);
        self.current = request;
        return .{ .go = .{ .reply = request.replies.send(self.loop, request.sequence + 1) } };
    }

    pub fn reply(self: *Server, result: Replies.Error!void) co.Next(Server) {
        result catch |err| return self.fail(err);
        self.current = null;
        self.count += 1;
        return self.start();
    }

    fn fail(self: *Server, err: anyerror) co.Next(Server) {
        self.shared.inbox.close();
        if (self.current) |request| request.replies.close();
        while (self.shared.inbox.tryRecv()) |request| {
            request.replies.close();
        } else |_| {}
        return .{ .done = err };
    }
};

const Shared = struct {
    inbox: Inbox = undefined,
    buffer: [2]Request = undefined,
    server: co.Frame(co.Machine(Server)) = undefined,
    slots: [clients]struct {
        replies: Replies = undefined,
        buffer: [1]u64 = undefined,
        frame: co.Frame(co.Machine(Client)) = undefined,
    } = @splat(.{}),
    remaining: std.atomic.Value(usize) = .init(clients),

    fn entry(self: *Shared, loop: *io.Loop, index: usize) !void {
        if (index == 0) {
            try self.inbox.init(loop, &self.buffer);
            self.server = .init(.init(.{ .shared = self, .loop = loop }));
            loop.spawn(&self.server);
        } else {
            const slot = &self.slots[index - 1];
            try slot.replies.init(loop, &slot.buffer);
            slot.frame = .init(.init(.{ .shared = self, .loop = loop, .replies = &slot.replies }));
            loop.spawn(&slot.frame);
        }
    }
};

fn run(allocator: std.mem.Allocator, backend: Group.Backend) !void {
    var shared: Shared = .{};
    var group: Group = undefined;
    try group.init(allocator, .{ .count = clients + 1, .backend = backend }, &shared, Shared.entry);
    group.deinit();
    defer {
        shared.inbox.deinit();
        for (&shared.slots) |*slot| slot.replies.deinit();
    }
    try shared.server.getResult();
    for (&shared.slots) |*slot| {
        try slot.frame.getResult();
        if (slot.frame.coroutine.ctx.sequence != requests) return error.MissingReplies;
    }
    if (shared.server.coroutine.ctx.count != clients * requests) return error.MissingRequests;
}

pub fn main() !void {
    try run(std.heap.page_allocator, .auto);
    std.debug.print("PASS: {d} typed request/reply exchanges across four loops\n", .{clients * requests});
}

test "mailbox typed request/reply across loop threads" {
    try run(std.testing.allocator, .auto);
    if (builtin.os.tag == .linux) try run(std.testing.allocator, .epoll);
}

test "mailbox group shutdown drains requests and replies" {
    var shared: Shared = .{};
    var group: Group = undefined;
    try group.init(std.testing.allocator, .{ .count = clients + 1 }, &shared, Shared.entry);
    group.shutdown();
    group.deinit();
    defer {
        shared.inbox.deinit();
        for (&shared.slots) |*slot| slot.replies.deinit();
    }
    shared.server.getResult() catch |err| switch (err) {
        error.Canceled, error.Closed => {},
        else => return err,
    };
    for (&shared.slots) |*slot| slot.frame.getResult() catch |err| switch (err) {
        error.Canceled, error.Closed => {},
        else => return err,
    };
}

test "mailbox startup failure drains initialized clients without touching an uninitialized server" {
    const Failing = struct {
        fn entry(shared: *Shared, loop: *io.Loop, index: usize) !void {
            if (index == 0) return error.Injected;
            try Shared.entry(shared, loop, index);
        }
    };
    var shared: Shared = .{};
    var group: Group = undefined;
    try std.testing.expectError(error.Injected, group.init(
        std.testing.allocator,
        .{ .count = clients + 1 },
        &shared,
        Failing.entry,
    ));
    for (&shared.slots) |*slot| {
        slot.replies.deinit();
        try std.testing.expectError(error.Canceled, slot.frame.getResult());
    }
}
