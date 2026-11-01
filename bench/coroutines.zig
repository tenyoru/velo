const std = @import("std");
const builtin = @import("builtin");
const co = @import("velo").coroutines;
const linux = std.os.linux;
const Channel = co.Channel(u64);
const Workload = enum { handoff, pipeline, parked };

const Config = struct {
    workload: Workload,
    tasks: usize,
    iterations: usize,
    capacity: usize,
    work: usize,
    payload: usize,
    active: usize,
    warmup: usize,

    fn parse(args: []const []const u8) !Config {
        if (args.len != 8) return error.Usage;
        const workload = std.meta.stringToEnum(Workload, args[0]) orelse return error.Usage;
        var nums: [7]usize = undefined;
        for (&nums, args[1..]) |*n, arg| n.* = try std.fmt.parseInt(usize, arg, 10);
        const c: Config = .{ .workload = workload, .tasks = nums[0], .iterations = nums[1], .capacity = nums[2], .work = nums[3], .payload = nums[4], .active = nums[5], .warmup = nums[6] };
        if (c.tasks < 2 or c.tasks > 100_000 or c.iterations == 0 or c.iterations > 10_000_000 or
            c.capacity == 0 or c.capacity > 65536 or c.work > 10000 or c.payload > 65536 or
            c.active == 0 or c.active > c.tasks or c.warmup > 1_000_000) return error.Usage;
        if (c.tasks * c.payload > 256 * 1024 * 1024 or c.tasks * c.capacity > 10_000_000) return error.Usage;
        if (workload == .pipeline and c.tasks < 3) return error.Usage;
        if (workload == .parked and (c.iterations != 1 or c.warmup != 0)) return error.Usage;
        if (c.tasks * @max(c.iterations, c.warmup) * @max(c.work, 1) > 100_000_000) return error.Usage;
        return c;
    }
};

fn clock(id: linux.clockid_t) u64 {
    var ts: linux.timespec = undefined;
    if (linux.errno(linux.clock_gettime(id, &ts)) != .SUCCESS) @panic("clock_gettime");
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn emit(gpa: std.mem.Allocator, value: anytype) !void {
    const text = try std.json.Stringify.valueAlloc(gpa, value, .{});
    defer gpa.free(text);
    for ([_][]const u8{ text, "\n" }) |part| {
        var offset: usize = 0;
        while (offset < part.len) {
            const n = linux.write(1, part.ptr + offset, part.len - offset);
            if (linux.errno(n) == .INTR) continue;
            if (linux.errno(n) != .SUCCESS or n == 0) return error.Output;
            offset += n;
        }
    }
}

fn command() !void {
    var byte: [1]u8 = undefined;
    while (true) {
        const n = linux.read(0, &byte, 1);
        if (linux.errno(n) == .INTR) continue;
        if (n != 1 or byte[0] != '\n') return error.Protocol;
        return;
    }
}

fn affine(steps: usize) struct { a: u64, b: u64 } {
    var a: u64 = 1;
    var b: u64 = 0;
    for (0..steps) |_| {
        a *%= 1664525;
        b = b *% 1664525 +% 1013904223;
    }
    return .{ .a = a, .b = b };
}

const Worker = struct {
    c: Config,
    id: usize,
    channels: []Channel,
    start_gate: *co.Event,
    index: usize = 0,
    checksum: u64 = 0,
    expected: u64 = 0,
    stride: u64 = 1,

    pub const Result = anyerror!void;
    pub const State = union(enum) { gate: co.Event.Wait, recv: Channel.Recv, send: Channel.Send };
    const Next = co.Next(@This());

    pub fn start(self: *Worker) Next {
        return if (self.c.workload == .pipeline and self.id == 0)
            .{ .go = .{ .gate = self.start_gate.wait() } }
        else
            self.receive();
    }

    fn receive(self: *Worker) Next {
        const input = if (self.c.workload == .handoff) self.id else self.id - 1;
        return .{ .go = .{ .recv = self.channels[input].recv() } };
    }

    pub fn gate(self: *Worker, result: error{Canceled}!void) Next {
        result catch |err| return .{ .done = err };
        return .{ .go = .{ .send = self.channels[0].send(0) } };
    }

    pub fn recv(self: *Worker, result: Channel.Error!u64) Next {
        var value = result catch |err| return .{ .done = err };
        if (self.c.workload == .handoff) {
            if (value != self.index * self.c.tasks + self.id) return .{ .done = error.BadToken };
            value += 1;
            if (self.id == self.c.tasks - 1 and self.index == self.c.iterations - 1) {
                self.checksum = value;
                return .{ .done = {} };
            }
            return .{ .go = .{ .send = self.channels[(self.id + 1) % self.c.tasks].send(value) } };
        }
        if (self.id == self.c.tasks - 1) {
            if (value != self.expected) return .{ .done = error.BadValue };
            self.expected +%= self.stride;
            self.checksum +%= value;
            self.index += 1;
            return if (self.index == self.c.iterations) .{ .done = {} } else self.receive();
        }
        for (0..self.c.work) |_| value = value *% 1664525 +% 1013904223;
        return .{ .go = .{ .send = self.channels[self.id].send(value) } };
    }

    pub fn send(self: *Worker, result: Channel.Error!void) Next {
        result catch |err| return .{ .done = err };
        self.index += 1;
        if (self.index == self.c.iterations) return .{ .done = {} };
        if (self.c.workload == .pipeline and self.id == 0)
            return .{ .go = .{ .send = self.channels[0].send(self.index) } };
        return self.receive();
    }
};

const Sleeper = struct {
    recv: Channel.Recv,
    payload: []const u8,
    expected: u64,
    checksum: u64 = 0,

    pub fn poll(self: *Sleeper, task: *co.Task) co.Poll(anyerror!void) {
        const value = self.recv.poll(task).value() orelse return .pending;
        _ = value catch |err| return .{ .ready = err };
        for (self.payload) |byte| self.checksum += byte;
        if (self.checksum != self.expected) return .{ .ready = error.BadPayload };
        return .{ .ready = {} };
    }
};

const Result = struct {
    phase: []const u8 = "result",
    operations: usize,
    checksum: u64,
    elapsed_ns: u64,
    cpu_ns: u64,
    setup_ns: u64,
    drain_ns: u64,
    completed: usize,
};

fn execute(gpa: std.mem.Allocator, c: Config, protocol: bool) !Result {
    const setup = clock(.MONOTONIC);
    var ex: co.Executor = .{};
    const count = if (c.workload == .pipeline) c.tasks - 1 else c.tasks;
    const capacity = if (c.workload == .pipeline) c.capacity else 1;
    const buffers = try gpa.alloc(u64, count * capacity);
    defer gpa.free(buffers);
    @memset(buffers, 0);
    const channels = try gpa.alloc(Channel, count);
    defer gpa.free(channels);
    for (channels, 0..) |*ch, i| ch.* = .init(buffers[i * capacity ..][0..capacity]);
    defer for (channels) |*ch| ch.deinit();

    if (c.workload == .parked) {
        const payload = try gpa.alloc(u8, c.tasks * c.payload);
        defer gpa.free(payload);
        const frames = try gpa.alloc(co.Frame(Sleeper), c.tasks);
        defer gpa.free(frames);
        for (frames, 0..) |*frame, i| {
            const bytes = payload[i * c.payload ..][0..c.payload];
            @memset(bytes, @intCast(i % 251));
            frame.* = .init(.{ .recv = channels[i].recv(), .payload = bytes, .expected = (i % 251) * c.payload });
            ex.spawn(&frame.task);
        }
        ex.run();
        const setup_ns = clock(.MONOTONIC) - setup;
        if (protocol) {
            try emit(gpa, .{ .phase = "ready", .tasks = c.tasks });
            try command();
        }
        const cpu = clock(.PROCESS_CPUTIME_ID);
        const begin = clock(.MONOTONIC);
        for (0..c.active) |i| {
            // Evenly spaced targets keep the active set fixed as the parked population grows.
            const id = i * c.tasks / c.active;
            var send: co.Frame(Channel.Send) = .init(channels[id].send(0));
            ex.spawn(&send.task);
            ex.run();
            try send.getResult();
            try frames[id].getResult();
        }
        const elapsed = clock(.MONOTONIC) - begin;
        const cpu_ns = clock(.PROCESS_CPUTIME_ID) - cpu;
        var checksum: u64 = 0;
        for (0..c.active) |i| checksum +%= frames[i * c.tasks / c.active].coroutine.checksum;
        const drain = clock(.MONOTONIC);
        for (frames, 0..) |*frame, i| {
            if (frame.result != null) continue;
            var send: co.Frame(Channel.Send) = .init(channels[i].send(0));
            ex.spawn(&send.task);
            ex.run();
            try send.getResult();
            try frame.getResult();
        }
        const drain_ns = clock(.MONOTONIC) - drain;
        if (ex.all_tasks.first != null) return error.Undrained;
        return .{ .operations = c.active, .checksum = checksum, .elapsed_ns = elapsed, .cpu_ns = cpu_ns, .setup_ns = setup_ns, .drain_ns = drain_ns, .completed = c.tasks };
    }

    var gate: co.Event = .{};
    defer gate.deinit();
    const transform = affine(if (c.workload == .pipeline) (c.tasks - 2) * c.work else 0);
    const frames = try gpa.alloc(co.Frame(co.Machine(Worker)), c.tasks);
    defer gpa.free(frames);
    for (frames, 0..) |*frame, i| {
        frame.* = .init(.init(.{ .c = c, .id = i, .channels = channels, .start_gate = &gate, .expected = transform.b, .stride = transform.a }));
        ex.spawn(&frame.task);
    }
    ex.run();
    const setup_ns = clock(.MONOTONIC) - setup;
    if (protocol) {
        try emit(gpa, .{ .phase = "ready", .tasks = c.tasks });
        try command();
    }
    const cpu = clock(.PROCESS_CPUTIME_ID);
    const begin = clock(.MONOTONIC);
    var inject: co.Frame(Channel.Send) = .init(channels[0].send(0));
    if (c.workload == .handoff) ex.spawn(&inject.task) else gate.set();
    ex.run();
    const elapsed = clock(.MONOTONIC) - begin;
    const cpu_ns = clock(.PROCESS_CPUTIME_ID) - cpu;
    if (c.workload == .handoff) try inject.getResult();
    for (frames) |*frame| {
        if (frame.result == null) return error.Incomplete;
        try frame.getResult();
    }
    if (ex.all_tasks.first != null) return error.Undrained;
    return .{ .operations = if (c.workload == .handoff) c.tasks * c.iterations else c.iterations, .checksum = frames[c.tasks - 1].coroutine.ctx.checksum, .elapsed_ns = elapsed, .cpu_ns = cpu_ns, .setup_ns = setup_ns, .drain_ns = 0, .completed = c.tasks };
}

pub fn main(init: std.process.Init) !void {
    if (builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const c = try Config.parse(args[1..]);
    if (c.workload != .parked and c.warmup != 0) {
        var warm = c;
        warm.iterations = c.warmup;
        _ = try execute(init.gpa, warm, false);
    }
    try emit(init.gpa, .{ .phase = "baseline" });
    try command();
    const result = try execute(init.gpa, c, true);
    try emit(init.gpa, result);
}

test "coroutine benchmark validates ring, pipeline and sparse wakes" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const base: Config = .{ .workload = .handoff, .tasks = 7, .iterations = 11, .capacity = 1, .work = 3, .payload = 257, .active = 3, .warmup = 0 };
    const ring = try execute(std.testing.allocator, base, false);
    try std.testing.expectEqual(77, ring.checksum);
    var c = base;
    c.workload = .pipeline;
    for ([_]usize{ 1, 16 }) |capacity| {
        c.capacity = capacity;
        const pipeline = try execute(std.testing.allocator, c, false);
        var expected: u64 = 0;
        for (0..c.iterations) |i| {
            var value: u64 = i;
            for (0..(c.tasks - 2) * c.work) |_| value = value *% 1664525 +% 1013904223;
            expected +%= value;
        }
        try std.testing.expectEqual(expected, pipeline.checksum);
    }
    c.workload = .parked;
    c.iterations = 1;
    const parked = try execute(std.testing.allocator, c, false);
    try std.testing.expectEqual(3, parked.operations);
    try std.testing.expectEqual((0 + 2 + 4) * 257, parked.checksum);
}
