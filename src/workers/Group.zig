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
const numa = @import("numa/root.zig");
const Loop = event.Loop;
const Group = @This();

pub const Backend = if (builtin.os.tag == .linux) Loop.Backend else enum { auto };
pub const Placement = numa.Placement;

pub const Options = struct {
    count: ?usize = null,
    loop: event.LoopOptions = .{},
    backend: Backend = .auto,
    name: ?[]const u8 = "velo",
    /// CPU affinity is Linux/Windows; memory policy is Linux-only. A nonempty list determines count.
    placement: []const Placement = &.{},
};

allocator: std.mem.Allocator,
slots: []Slot,
default_storage: ?[]align(storage_alignment) u8 = null,
threaded: std.Io.Threaded = .init_single_threaded,
go: std.Io.Event = .unset,
closing: std.Io.Event = .unset,
pending: std.atomic.Value(usize) = .init(0),

const Slot = struct {
    index: usize,
    placement: Placement,
    thread: std.Thread = undefined,
    loop: ?*Loop = null,
    created: bool = false,
    result: anyerror!void = {},
    ready: std.Io.Event = .unset,
};

const storage_alignment = @max(@alignOf(Slot), @alignOf(Loop));

pub fn init(group: *Group, allocator: std.mem.Allocator, options: Options, context: anytype, comptime entry: anytype) !void {
    return group.initWithHooks(allocator, options, context, entry, .{});
}

/// Hooks are a comptime pair: `prepare(context, index, base) !LoopOptions` and
/// `cleanup(context, index) void`. A successful prepare owns resources until cleanup
/// on the same thread, after loop deinit and storage release, even if loop init fails.
/// A failed prepare must release its own partial resources. Memory policy affects
/// fresh faults on that thread, not preexisting heap pages; loop backing uses mbind.
pub fn initWithHooks(group: *Group, allocator: std.mem.Allocator, options: Options, context: anytype, comptime entry: anytype, comptime hooks: anytype) !void {
    comptime {
        if (@hasField(@TypeOf(hooks), "prepare") != @hasField(@TypeOf(hooks), "cleanup"))
            @compileError("Group hooks require both prepare and cleanup");
    }
    if (options.placement.len != 0) {
        if (options.count) |count| {
            if (count != options.placement.len) return error.InvalidPlacement;
        }
        for (options.placement) |requested| {
            if (requested.cpu != null and builtin.os.tag != .linux and builtin.os.tag != .windows)
                return error.Unsupported;
            if (requested.memory != null and builtin.os.tag != .linux) return error.Unsupported;
        }
    }

    const count = options.count orelse if (options.placement.len != 0)
        options.placement.len
    else
        try std.Thread.getCpuCount();
    if (count == 0) return error.InvalidThreadCount;
    if (options.name) |name| {
        const digits = std.fmt.count("{d}", .{count - 1});
        if (name.len + 1 + digits > std.Thread.max_name_len)
            return error.NameTooLong;
    }

    const shared_storage = options.placement.len == 0 and !@hasField(@TypeOf(hooks), "prepare");
    var storage: ?[]align(storage_alignment) u8 = null;
    var loop_offset: usize = 0;
    const slots = if (shared_storage) blk: {
        const slot_bytes = try std.math.mul(usize, @sizeOf(Slot), count);
        loop_offset = (try std.math.add(usize, slot_bytes, @alignOf(Loop) - 1)) & ~@as(usize, @alignOf(Loop) - 1);
        const bytes = try std.math.add(usize, loop_offset, try std.math.mul(usize, @sizeOf(Loop), count));
        storage = try allocator.alignedAlloc(u8, .fromByteUnits(storage_alignment), bytes);
        break :blk @as([*]Slot, @ptrCast(storage.?.ptr))[0..count];
    } else try allocator.alloc(Slot, count);
    group.* = .{ .allocator = allocator, .slots = slots, .default_storage = storage };
    errdefer {
        group.threaded.deinit();
        if (storage) |bytes| allocator.free(bytes) else allocator.free(slots);
        group.* = undefined;
    }

    // Placed loops need separate backing; cleanup hooks require in-thread storage release.
    for (slots, 0..) |*slot, i| slot.* = .{
        .index = i,
        .placement = if (options.placement.len != 0) options.placement[i] else .{},
        .loop = if (storage) |bytes| @ptrCast(@alignCast(bytes.ptr + loop_offset + i * @sizeOf(Loop))) else null,
    };

    var spawned: usize = 0;
    var failure: ?anyerror = null;
    for (slots) |*slot| {
        slot.thread = std.Thread.spawn(.{}, ThreadMain(@TypeOf(context), entry, hooks).run, .{ group, slot, options, context }) catch |err| {
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
            slot.loop.?.shutdown();
            slot.loop.?.release();
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
    return group.slots[index].loop.?;
}

pub fn placement(group: *const Group, index: usize) Placement {
    return group.slots[index].placement;
}

pub fn shutdown(group: *Group) void {
    for (group.slots) |*slot| slot.loop.?.shutdown();
}

pub fn deinit(group: *Group) void {
    for (group.slots) |*slot| slot.loop.?.release();
    group.arrive();
    for (group.slots) |*slot| slot.thread.join();

    group.threaded.deinit();
    if (group.default_storage) |bytes| group.allocator.free(bytes) else group.allocator.free(group.slots);
    group.* = undefined;
}

fn arrive(group: *Group) void {
    // A final release can still be signaling after its destination loop drains.
    if (group.pending.fetchSub(1, .acq_rel) == 1) group.closing.set(group.threaded.io());
}

fn ThreadMain(comptime Context: type, comptime entry: anytype, comptime hooks: anytype) type {
    return struct {
        fn run(group: *Group, slot: *Slot, options: Options, context: Context) void {
            var prepared = false;
            defer if (@hasField(@TypeOf(hooks), "cleanup")) {
                if (prepared) hooks.cleanup(context, slot.index);
            };
            var region: ?numa.Region = null;
            defer if (region) |*backing| {
                backing.deinit();
            } else if (group.default_storage == null) {
                if (slot.loop) |backing| std.heap.page_allocator.destroy(backing);
            };

            const io = group.threaded.io();
            slot.result = setup(slot, options, context, &prepared, &region);
            const created = slot.created;
            slot.ready.set(io);
            if (!created) return;

            group.go.waitUncancelable(io);
            while (true) {
                slot.loop.?.run() catch |err| std.debug.panic("velo: group loop {d} failed: {s}", .{ slot.index, @errorName(err) });
                if (!slot.loop.?.control.hasWork()) break;
            }

            group.arrive();
            group.closing.waitUncancelable(io);
            slot.loop.?.deinit();
        }

        fn setup(slot: *Slot, options: Options, context: Context, prepared: *bool, region: *?numa.Region) anyerror!void {
            if (slot.placement.cpu) |cpu| try numa.pin(&.{cpu});
            if (slot.placement.memory) |policy| try numa.setPolicy(policy);

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

            const loop_options = if (@hasField(@TypeOf(hooks), "prepare")) blk: {
                const result = try hooks.prepare(context, slot.index, options.loop);
                prepared.* = true;
                break :blk result;
            } else options.loop;

            if (slot.loop == null) slot.loop = if (slot.placement.memory) |policy| blk: {
                region.* = try numa.Region.init(@sizeOf(Loop), policy);
                break :blk @ptrCast(@alignCast(region.*.?.bytes.ptr));
            } else try std.heap.page_allocator.create(Loop);
            slot.loop.?.* = if (builtin.os.tag == .linux)
                try Loop.initWithBackend(loop_options, options.backend)
            else
                try Loop.init(loop_options);
            slot.created = true;
            slot.loop.?.retain();

            const result = entry(context, slot.loop.?, slot.index);
            if (@TypeOf(result) != void) _ = try result;
        }
    };
}

test {
    _ = @import("group_placement_test.zig");
}
