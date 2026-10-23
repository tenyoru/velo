//! Event loops with one thread each. Initialize in place; keep pinned through deinit.
//! Loops are created, run and destroyed on their own threads. `entry(context, loop, i)`
//! runs on thread i before any loop runs and may touch only its own loop; no loop runs
//! until every entry succeeds, so tasks may post to any group loop from their first poll.
//! The group holds one reference per loop until deinit; posts that may happen after
//! deinit begins need a reference taken before it. Non-group threads may only
//! post/retain/release/shutdown group loops and must finish those calls before deinit.
//! Call deinit outside the group: it waits for every loop to drain, so shutdown first to
//! cancel live work. A loop run error panics rather than unwinding borrowed storage.

const std = @import("std");
const builtin = @import("builtin");
const event = @import("../event/root.zig");
const win32 = @import("../event/backends/win32.zig");
const Loop = event.Loop;
const Group = @This();

pub const Backend = if (builtin.os.tag == .linux) Loop.Backend else enum { auto };
pub const Options = struct {
    count: ?usize = null,
    loop: event.LoopOptions = .{},
    backend: Backend = .auto,
    name: ?[]const u8 = "velo",
    pin: bool = false,
};

allocator: std.mem.Allocator,
slots: []Slot,
threaded: std.Io.Threaded = .init_single_threaded,
go: std.Io.Event = .unset,
closing: std.Io.Event = .unset,
pending: std.atomic.Value(usize) = .init(0),

const Slot = struct {
    index: usize,
    cpu: ?usize,
    thread: std.Thread = undefined,
    loop: Loop = undefined,
    created: bool = false,
    result: anyerror!void = {},
    ready: std.Io.Event = .unset,
};

pub fn init(group: *Group, allocator: std.mem.Allocator, options: Options, context: anytype, comptime entry: anytype) !void {
    var cpus: [if (builtin.os.tag == .linux) @bitSizeOf(std.os.linux.cpu_set_t) else @bitSizeOf(usize)]usize = undefined;
    var allowed: usize = 0;

    if (options.pin) switch (builtin.os.tag) {
        .linux => {
            const set = try std.posix.sched_getaffinity(0);
            for (set, 0..) |word, i| {
                var bits = word;
                while (bits != 0) : (bits &= bits - 1) {
                    cpus[allowed] = i * @bitSizeOf(usize) + @ctz(bits);
                    allowed += 1;
                }
            }
        },
        .windows => {
            var process: usize = undefined;
            var system: usize = undefined;
            if (win32.GetProcessAffinityMask(std.os.windows.GetCurrentProcess(), &process, &system) == 0)
                return error.Unexpected;

            while (process != 0) : (process &= process - 1) {
                cpus[allowed] = @ctz(process);
                allowed += 1;
            }
        },
        else => return error.Unsupported,
    };

    const count = options.count orelse if (options.pin) allowed else try std.Thread.getCpuCount();
    if (count == 0) return error.InvalidThreadCount;
    if (options.name) |name| {
        const digits = std.fmt.count("{d}", .{count - 1});
        if (name.len + 1 + digits > std.Thread.max_name_len)
            return error.NameTooLong;
    }
    if (options.pin and count > allowed) return error.NotEnoughCpus;

    const slots = try allocator.alloc(Slot, count);
    group.* = .{ .allocator = allocator, .slots = slots };
    errdefer {
        group.threaded.deinit();
        allocator.free(slots);
        group.* = undefined;
    }

    for (slots, 0..) |*slot, i| slot.* = .{
        .index = i,
        .cpu = if (options.pin) cpus[i] else null,
    };

    var spawned: usize = 0;
    var failure: ?anyerror = null;
    for (slots) |*slot| {
        slot.thread = std.Thread.spawn(.{}, ThreadMain(@TypeOf(context), entry).run, .{ group, slot, options, context }) catch |err| {
            failure = err;
            break;
        };
        spawned += 1;
    }

    const io = group.threaded.io();
    var entry_failure: ?anyerror = null;
    var created: usize = 0;
    for (slots[0..spawned]) |*slot| {
        slot.ready.waitUncancelable(io);
        slot.result catch |err| {
            if (entry_failure == null) entry_failure = err;
        };
        created += @intFromBool(slot.created);
    }

    if (entry_failure != null) failure = entry_failure;
    if (failure) |err| {
        for (slots[0..spawned]) |*slot| if (slot.created) {
            slot.loop.shutdown();
            slot.loop.release();
        };

        group.pending.store(created + 1, .monotonic);
        group.go.set(io);
        group.arrive();
        for (slots[0..spawned]) |*slot| slot.thread.join();

        return err;
    }

    group.pending.store(count + 1, .monotonic);
    group.go.set(io);
}

pub fn loop(group: *Group, index: usize) *Loop {
    return &group.slots[index].loop;
}

pub fn shutdown(group: *Group) void {
    for (group.slots) |*slot| slot.loop.shutdown();
}

pub fn deinit(group: *Group) void {
    for (group.slots) |*slot| slot.loop.release();
    group.arrive();
    for (group.slots) |*slot| slot.thread.join();

    group.threaded.deinit();
    group.allocator.free(group.slots);
    group.* = undefined;
}

fn arrive(group: *Group) void {
    // A final release can still be signaling after its destination loop drains.
    if (group.pending.fetchSub(1, .acq_rel) == 1) group.closing.set(group.threaded.io());
}

fn ThreadMain(comptime Context: type, comptime entry: anytype) type {
    return struct {
        fn run(group: *Group, slot: *Slot, options: Options, context: Context) void {
            const io = group.threaded.io();
            slot.result = setup(slot, options, context);
            const created = slot.created;
            slot.ready.set(io);
            if (!created) return;

            group.go.waitUncancelable(io);
            while (true) {
                slot.loop.run() catch |err| std.debug.panic("velo: group loop {d} failed: {s}", .{ slot.index, @errorName(err) });
                if (!slot.loop.control.hasWork()) break;
            }

            group.arrive();
            group.closing.waitUncancelable(io);
            slot.loop.deinit();
        }

        fn setup(slot: *Slot, options: Options, context: Context) anyerror!void {
            if (slot.cpu) |cpu| switch (builtin.os.tag) {
                .linux => {
                    var set: std.os.linux.cpu_set_t = @splat(0);
                    set[cpu / @bitSizeOf(usize)] = @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));
                    try std.os.linux.sched_setaffinity(0, &set);
                },
                .windows => {
                    if (win32.SetThreadAffinityMask(std.os.windows.GetCurrentThread(), @as(usize, 1) << @intCast(cpu)) == 0)
                        return error.Unexpected;
                },
                else => unreachable,
            };

            if (options.name) |prefix| {
                var buffer: [std.Thread.max_name_len + 1]u8 = undefined;
                const name = try std.mem.print(buffer[0..std.Thread.max_name_len], "{s}-{d}", .{ prefix, slot.index });
                buffer[name.len] = 0;

                switch (builtin.os.tag) {
                    .linux => _ = try std.posix.prctl(.SET_NAME, .{@intFromPtr(name.ptr)}),
                    .windows => {
                        const windows = std.os.windows;
                        var wide: [std.Thread.max_name_len]u16 = undefined;
                        const len = try std.unicode.wtf8ToWtf16Le(&wide, name);
                        if (windows.ntdll.NtSetInformationThread(
                            windows.GetCurrentThread(),
                            .NameInformation,
                            &windows.UNICODE_STRING.init(wide[0..len]),
                            @sizeOf(windows.UNICODE_STRING),
                        ) != .SUCCESS) return error.Unexpected;
                    },
                    .macos => {
                        if (std.c.pthread_setname_np(@ptrCast(name.ptr)) != 0) return error.Unexpected;
                    },
                    else => unreachable,
                }
            }

            slot.loop = if (builtin.os.tag == .linux)
                try Loop.initWithBackend(options.loop, options.backend)
            else
                try Loop.init(options.loop);
            slot.created = true;
            slot.loop.retain();

            const result = entry(context, &slot.loop, slot.index);
            if (@TypeOf(result) != void) _ = try result;
        }
    };
}
