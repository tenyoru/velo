const std = @import("std");
const builtin = @import("builtin");
const Group = @import("Group.zig");
const numa = @import("numa/root.zig");
const event = @import("../event/root.zig");
const co = @import("../coroutines/root.zig");
const support = @import("../event/tests/support.zig");
const testing = std.testing;
const linux = std.os.linux;

test "group placement copies explicit CPUs and planned affinity precedes hooks" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const allowed = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(allowed);
    const Context = struct {
        expected: []const Group.Placement,
        prepared: [2]?std.Thread.Id = @splat(null),
        cleaned: [2]usize = @splat(0),

        fn prepare(self: *@This(), index: usize, base: event.LoopOptions) !event.LoopOptions {
            const effective = try numa.allowedCpus(std.heap.page_allocator);
            defer std.heap.page_allocator.free(effective);
            try testing.expectEqualSlices(usize, &.{self.expected[index].cpu.?}, effective);
            var name: [std.Thread.max_name_len + 1]u8 = undefined;
            _ = try std.posix.prctl(.GET_NAME, .{@intFromPtr(&name)});
            var buffer: [32]u8 = undefined;
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buffer, "placed-{d}", .{index}), std.mem.sliceTo(&name, 0));
            self.prepared[index] = std.Thread.getCurrentId();
            return base;
        }

        fn entry(self: *@This(), _: *event.Loop, index: usize) !void {
            try testing.expectEqual(self.prepared[index].?, std.Thread.getCurrentId());
        }

        fn cleanup(self: *@This(), index: usize) void {
            std.debug.assert(self.prepared[index].? == std.Thread.getCurrentId());
            self.cleaned[index] += 1;
        }
    };

    var requested = [_]Group.Placement{ .{ .cpu = allowed[allowed.len - 1] }, .{ .cpu = allowed[0] } };
    const expected = requested;
    var context: Context = .{ .expected = &expected };
    var group: Group = undefined;
    try group.initWithHooks(testing.allocator, .{
        .placement = &requested,
        .name = "placed",
        .backend = .epoll,
    }, &context, Context.entry, .{ .prepare = Context.prepare, .cleanup = Context.cleanup });
    requested = @splat(.{});
    const actual = [_]Group.Placement{ group.placement(0), group.placement(1) };
    group.deinit();
    try testing.expectEqualDeep(expected, actual);
    try testing.expectEqualDeep([_]usize{ 1, 1 }, context.cleaned);

    const count = @min(2, allowed.len);
    var topology = try numa.Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    const planned = try numa.plan(testing.allocator, &topology, .{ .count = count });
    defer testing.allocator.free(planned);
    context = .{ .expected = planned };
    try group.initWithHooks(testing.allocator, .{
        .count = count,
        .placement = planned,
        .name = "placed",
        .backend = .epoll,
    }, &context, Context.entry, .{ .prepare = Context.prepare, .cleanup = Context.cleanup });
    group.deinit();
    for (context.cleaned[0..count]) |calls| try testing.expectEqual(1, calls);
    const after = try numa.allowedCpus(testing.allocator);
    defer testing.allocator.free(after);
    try testing.expectEqualSlices(usize, allowed, after);
}

test "group placement validates counts and rejects unsupported CPU and memory requests" {
    const Noop = struct {
        fn entry(_: void, _: *event.Loop, _: usize) void {}
    };
    var group: Group = undefined;
    const placements = [_]Group.Placement{.{}};
    try testing.expectError(error.InvalidPlacement, group.init(testing.allocator, .{
        .placement = &placements,
        .count = 2,
    }, {}, Noop.entry));
    try testing.expectError(error.InvalidPlacement, group.init(testing.allocator, .{
        .placement = &placements,
        .count = 0,
    }, {}, Noop.entry));
    if (builtin.os.tag == .linux) {
        try testing.expectError(error.Overflow, group.init(testing.allocator, .{
            .placement = &.{.{ .cpu = std.math.maxInt(usize) }},
            .backend = .epoll,
        }, {}, Noop.entry));
    } else {
        try testing.expectError(error.Unsupported, group.init(testing.allocator, .{
            .placement = &.{.{ .memory = .{ .bind = 0 } }},
        }, {}, Noop.entry));
        if (builtin.os.tag != .windows) try testing.expectError(error.Unsupported, group.init(testing.allocator, .{
            .placement = &.{.{ .cpu = 0 }},
        }, {}, Noop.entry));
    }
    try group.init(testing.allocator, .{ .placement = &placements, .count = 1, .backend = if (builtin.os.tag == .linux) .epoll else .auto }, {}, Noop.entry);
    const actual = group.placement(0);
    group.deinit();
    try testing.expectEqualDeep(placements[0], actual);
}

test "group memory placement binds loop pages and sets policy before prepare" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var topology = try numa.Topology.discover(testing.allocator, testing.io);
    defer topology.deinit();
    const node = for (topology.nodes) |node| {
        if (node.memory) break node.id;
    } else return error.TestExpectedMemoryNode;
    const other = for (topology.nodes) |candidate| {
        if (candidate.memory and candidate.id != node) break candidate.id;
    } else node;
    var probe = numa.Region.init(std.heap.pageSize(), .{ .bind = node }) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    defer probe.deinit();
    probe.bytes[0] = 1;
    _ = numa.pageNode(probe.bytes.ptr) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };

    const Context = struct {
        placements: []const Group.Placement,
        mask_words: usize,
        fresh: [3]?[]u8 = @splat(null),
        thread: [3]?std.Thread.Id = @splat(null),
        loop_address: [3]usize = @splat(0),
        wake_fd: [3]linux.fd_t = @splat(-1),
        cleanups: [3]usize = @splat(0),
        cleanup_result: [3]anyerror!void = .{ {}, {}, {} },

        fn prepare(self: *@This(), index: usize, base: event.LoopOptions) !event.LoopOptions {
            const policy = self.placements[index].memory.?;
            const id = switch (policy) {
                .bind, .prefer => |n| n,
            };
            const mask = try std.heap.page_allocator.alloc(usize, self.mask_words);
            defer std.heap.page_allocator.free(mask);
            @memset(mask, 0);
            var mode: i32 = 0;
            try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.syscall5(.get_mempolicy, @intFromPtr(&mode), @intFromPtr(mask.ptr), mask.len * @bitSizeOf(usize), 0, 0)));
            try testing.expectEqual(@as(i32, (1 << 15) | @as(i32, if (policy == .bind) 2 else 1)), mode);
            for (mask, 0..) |word, i| try testing.expectEqual(
                if (i == id / @bitSizeOf(usize)) @as(usize, 1) << @intCast(id % @bitSizeOf(usize)) else 0,
                word,
            );
            const bytes = try std.heap.page_allocator.alloc(u8, std.heap.pageSize());
            errdefer std.heap.page_allocator.free(bytes);
            @memset(bytes, 17);
            if (policy == .bind) try testing.expectEqual(id, try numa.pageNode(bytes.ptr));
            self.fresh[index] = bytes;
            self.thread[index] = std.Thread.getCurrentId();
            return base;
        }

        fn entry(self: *@This(), loop: *event.Loop, index: usize) !void {
            self.loop_address[index] = @intFromPtr(loop);
            self.wake_fd[index] = loop.wake_fd;
            try testing.expectEqual(self.thread[index].?, std.Thread.getCurrentId());
            try testing.expectEqual(@as(usize, 0), @intFromPtr(loop) % std.heap.pageSize());
            if (self.placements[index].memory.? == .bind) {
                const id = self.placements[index].memory.?.bind;
                var offset: usize = 0;
                while (offset < @sizeOf(event.Loop)) : (offset += std.heap.pageSize()) {
                    const page: [*]const u8 = @ptrFromInt(@intFromPtr(loop) + offset);
                    try testing.expectEqual(id, try numa.pageNode(page));
                }
            }
        }

        fn cleanup(self: *@This(), index: usize) void {
            defer std.heap.page_allocator.free(self.fresh[index].?);
            self.cleanups[index] += 1;
            self.cleanup_result[index] = self.checkCleanup(index);
        }

        fn checkCleanup(self: *@This(), index: usize) !void {
            try testing.expectEqual(self.thread[index].?, std.Thread.getCurrentId());
            if (self.loop_address[index] != 0)
                try expectLoopReleased(self.loop_address[index], self.wake_fd[index], null);
        }
    };
    const placements = [_]Group.Placement{
        .{ .memory = .{ .bind = node } },
        .{ .memory = .{ .bind = other } },
        .{ .memory = .{ .prefer = node } },
    };
    var context: Context = .{
        .placements = &placements,
        .mask_words = @max(16, topology.nodes[topology.nodes.len - 1].id / @bitSizeOf(usize) + 1),
    };
    var group: Group = undefined;
    try testing.expectError(error.InvalidNode, group.init(testing.allocator, .{
        .placement = &.{.{ .memory = .{ .bind = std.math.maxInt(usize) } }},
        .backend = .epoll,
    }, &context, Context.entry));
    group.initWithHooks(testing.allocator, .{ .placement = &placements, .backend = .epoll }, &context, Context.entry, .{
        .prepare = Context.prepare,
        .cleanup = Context.cleanup,
    }) catch |err| switch (err) {
        error.Unsupported, error.PermissionDenied => return error.SkipZigTest,
        else => return err,
    };
    const copied = [_]Group.Placement{ group.placement(0), group.placement(1), group.placement(2) };
    const addresses = [_]usize{ @intFromPtr(group.loop(0)), @intFromPtr(group.loop(1)), @intFromPtr(group.loop(2)) };
    group.deinit();
    try testing.expectEqualDeep(placements, copied);
    try testing.expectEqualDeep(context.loop_address, addresses);
    for (context.cleanups) |calls| try testing.expectEqual(1, calls);
    for (context.cleanup_result) |result| try result;
}

fn expectLoopReleased(address: usize, wake_fd: linux.fd_t, ring_fd: ?linux.fd_t) !void {
    var resident: [1]u8 = undefined;
    try testing.expectEqual(linux.E.NOMEM, linux.errno(linux.mincore(@ptrFromInt(address), std.heap.pageSize(), &resident)));
    try testing.expectEqual(linux.E.BADF, linux.errno(linux.fcntl(wake_fd, linux.F.GETFD, 0)));
    if (ring_fd) |fd| try testing.expectEqual(linux.E.BADF, linux.errno(linux.fcntl(fd, linux.F.GETFD, 0)));
}

const Rollback = struct {
    failure: enum { none, prepare, loop_init, entry },
    peers: [3]Peer = @splat(.{}),

    const Peer = struct {
        prepares: usize = 0,
        cleanups: usize = 0,
        entries: usize = 0,
        thread: ?std.Thread.Id = null,
        bytes: ?[]u8 = null,
        loop_address: usize = 0,
        wake_fd: linux.fd_t = -1,
        ring_fd: ?linux.fd_t = null,
        gate: co.Event = .{},
        frame: co.Frame(co.Event.Wait) = undefined,
        cleanup_result: anyerror!void = {},
    };

    fn prepare(self: *@This(), index: usize, base: event.LoopOptions) !event.LoopOptions {
        const peer = &self.peers[index];
        peer.prepares += 1;
        peer.thread = std.Thread.getCurrentId();
        const bytes = try std.heap.page_allocator.alloc(u8, std.heap.pageSize());
        @memset(bytes, 71);
        if (index == 1 and self.failure == .prepare) {
            std.heap.page_allocator.free(bytes);
            return error.InjectedPrepare;
        }
        peer.bytes = bytes;
        var options = base;
        if (index == 1 and self.failure == .loop_init) options.entries = 0;
        return options;
    }

    fn entry(self: *@This(), loop: *event.Loop, index: usize) !void {
        const peer = &self.peers[index];
        peer.entries += 1;
        peer.loop_address = @intFromPtr(loop);
        if (builtin.os.tag == .linux) {
            peer.wake_fd = loop.wake_fd;
            if (loop.epoll == null) peer.ring_fd = loop.ring.fd;
        }
        peer.frame = .{ .coroutine = peer.gate.wait() };
        loop.executor.spawn(&peer.frame.task);
        if (index == 1 and self.failure == .entry) return error.InjectedEntry;
    }

    fn cleanup(self: *@This(), index: usize) void {
        const peer = &self.peers[index];
        defer std.heap.page_allocator.free(peer.bytes.?);
        peer.cleanups += 1;
        peer.cleanup_result = check(peer);
    }

    fn check(peer: *Peer) !void {
        try testing.expectEqual(peer.thread.?, std.Thread.getCurrentId());
        try testing.expectEqual(@as(u8, 71), peer.bytes.?[0]);
        if (peer.entries == 0) return;
        try testing.expectError(error.Canceled, peer.frame.getResult());
        try testing.expectEqual(0, peer.frame.task.outstanding_io);
        try testing.expect(!peer.frame.task.registered);
        peer.gate.deinit();
        if (builtin.os.tag == .linux) try expectLoopReleased(peer.loop_address, peer.wake_fd, peer.ring_fd);
    }
};

test "group hooks clean up once after teardown on success prepare loop init and entry rollback" {
    for (support.modes) |mode| {
        for ([_]@FieldType(Rollback, "failure"){ .none, .prepare, .loop_init, .entry }) |failure| {
            var context: Rollback = .{ .failure = failure };
            var group: Group = undefined;
            const result = group.initWithHooks(testing.allocator, .{
                .count = context.peers.len,
                .loop = .{ .entries = 8 },
                .backend = if (builtin.os.tag == .linux and mode == .epoll) .epoll else .auto,
            }, &context, Rollback.entry, .{ .prepare = Rollback.prepare, .cleanup = Rollback.cleanup });
            switch (failure) {
                .none => {
                    try result;
                    group.shutdown();
                    group.deinit();
                },
                .prepare => try testing.expectError(error.InjectedPrepare, result),
                .loop_init => try testing.expectError(if (builtin.os.tag == .windows) error.InvalidArgument else error.RingTooSmall, result),
                .entry => try testing.expectError(error.InjectedEntry, result),
            }
            for (&context.peers, 0..) |*peer, i| {
                try testing.expectEqual(1, peer.prepares);
                try testing.expectEqual(@as(usize, if (i == 1 and failure == .prepare) 0 else 1), peer.cleanups);
                try testing.expectEqual(@as(usize, if (i == 1 and (failure == .prepare or failure == .loop_init)) 0 else 1), peer.entries);
                try peer.cleanup_result;
            }
        }
    }
}

test "group hooks register separate shard buffers and retain them through cancellation" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var available = event.Loop.initWithBackend(.{ .entries = 8 }, .uring) catch |err| switch (err) {
        error.PermissionDenied, error.SystemOutdated => return error.SkipZigTest,
        else => return err,
    };
    const probe = try available.ring.get_probe();
    const can_read = available.file_ops.read and probe.is_supported(.READ_FIXED) and probe.is_supported(.WRITE_FIXED);
    available.deinit();
    if (!can_read) return error.SkipZigTest;

    const Context = struct {
        peers: [2]Peer = @splat(.{}),

        const Read = event.Operation.Coroutine(.read_at);
        const Peer = struct {
            bytes: []u8 = undefined,
            buffers: [1][]u8 = undefined,
            thread: std.Thread.Id = undefined,
            loop_address: usize = 0,
            wake_fd: linux.fd_t = -1,
            ring_fd: linux.fd_t = -1,
            pipe: [2]linux.fd_t = .{ -1, -1 },
            entered: bool = false,
            read: co.Frame(Read) = undefined,
            notify: co.Frame(Notify) = undefined,
            started: std.Io.Event = .unset,
            cleanups: usize = 0,
            cleanup_result: anyerror!void = {},
        };
        const Notify = struct {
            timer: event.Timeout,
            started: *std.Io.Event,

            pub fn poll(self: *@This(), task: *co.Task) co.Poll(anyerror!void) {
                const result = switch (self.timer.poll(task)) {
                    .pending => return .pending,
                    .ready => |result| result,
                };
                self.started.set(testing.io);
                return .{ .ready = result };
            }

            pub fn cancel(self: *@This()) void {
                self.timer.cancel();
            }
        };

        fn prepare(self: *@This(), index: usize, base: event.LoopOptions) !event.LoopOptions {
            const peer = &self.peers[index];
            peer.thread = std.Thread.getCurrentId();
            peer.bytes = try std.heap.page_allocator.alloc(u8, std.heap.pageSize());
            @memset(peer.bytes, @intCast(index + 41));
            peer.buffers = .{peer.bytes};
            var options = base;
            options.buffers = &peer.buffers;
            return options;
        }

        fn entry(self: *@This(), loop: *event.Loop, index: usize) !void {
            const peer = &self.peers[index];
            try testing.expectEqual(@as(usize, 1), loop.buffers.len);
            try testing.expectEqual(peer.buffers[0].ptr, loop.buffers[0].ptr);
            try testing.expectEqual(@as([*]const []u8, &peer.buffers), loop.buffers.ptr);
            peer.loop_address = @intFromPtr(loop);
            peer.wake_fd = loop.wake_fd;
            peer.ring_fd = loop.ring.fd;
            try testing.expectEqual(linux.E.SUCCESS, linux.errno(linux.pipe2(&peer.pipe, .{ .CLOEXEC = true })));
            peer.read = .{ .coroutine = Read.init(loop, .{
                .file = .{ .handle = peer.pipe[0], .flags = .{ .nonblocking = false } },
                .buffer = peer.bytes[0..8],
            }) };
            peer.notify = .{ .coroutine = .{
                .timer = event.Timeout.init(loop, .{ .duration = .{ .sec = 0, .nsec = 2_000_000 } }),
                .started = &peer.started,
            } };
            loop.executor.spawn(&peer.read.task);
            loop.executor.spawn(&peer.notify.task);
            peer.entered = true;
        }

        fn cleanup(self: *@This(), index: usize) void {
            const peer = &self.peers[index];
            defer std.heap.page_allocator.free(peer.bytes);
            defer for (peer.pipe) |fd| {
                if (fd >= 0) _ = linux.close(fd);
            };
            peer.cleanups += 1;
            peer.cleanup_result = check(peer, index);
        }

        fn check(peer: *Peer, index: usize) !void {
            try testing.expectEqual(peer.thread, std.Thread.getCurrentId());
            if (peer.loop_address == 0) return;
            try expectLoopReleased(peer.loop_address, peer.wake_fd, peer.ring_fd);
            if (!peer.entered) return;
            try testing.expectEqual(@as(u8, @intCast(index + 41)), peer.bytes[0]);
            try testing.expectError(error.OperationCanceled, peer.read.getResult());
            try peer.notify.getResult();
            try testing.expectEqual(0, peer.read.task.outstanding_io);
            try testing.expect(!peer.read.coroutine.operation.registration.registered);
            peer.read.coroutine.deinit();
            peer.notify.coroutine.timer.deinit();
        }
    };
    var context: Context = .{};
    var group: Group = undefined;
    try group.initWithHooks(testing.allocator, .{ .count = 2, .loop = .{ .entries = 8 }, .backend = .uring }, &context, Context.entry, .{
        .prepare = Context.prepare,
        .cleanup = Context.cleanup,
    });
    for (&context.peers) |*peer| peer.started.waitUncancelable(testing.io);
    group.shutdown();
    group.deinit();
    try testing.expect(context.peers[0].bytes.ptr != context.peers[1].bytes.ptr);
    for (context.peers) |peer| {
        try testing.expectEqual(1, peer.cleanups);
        try peer.cleanup_result;
    }
}
