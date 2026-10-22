const std = @import("std");
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;
const fs = velo.fs;

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (args.len != 2) {
        std.debug.print("Usage: {s} <new-output-path>\n", .{args[0]});
        return error.ExpectedOutputPath;
    }

    const bytes = try writeAndVerify(init.gpa, init.io, args[1]);
    std.debug.print(
        "Verified {d} synced bytes in {s}; file kept for inspection.\n",
        .{ bytes, args[1] },
    );
}

fn writeAndVerify(allocator: std.mem.Allocator, std_io: std.Io, path: []const u8) !usize {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    var pool: velo.workers.Pool = undefined;
    try pool.init(allocator, &loop, 2);
    // Join workers before destroying the loop they post results to.
    defer pool.deinit();

    // Exclusive creation prevents this example from overwriting an existing file.
    const file = try run(&loop, fs.Create.init(&pool, .{
        .dir = .cwd(),
        .path = path,
        .flags = .{ .read = true, .exclusive = true },
    }));

    // run() consumes each result before returning, so error cleanup can close safely.
    errdefer file.close(std_io);

    const payload = "velo: bounded workers, explicit state, durable files.\n";
    var offset: usize = 0;

    // Successful writes can be short; advance only by the bytes actually written.
    while (offset < payload.len) {
        const n = try run(&loop, fs.WriteAt.init(&pool, .{
            .file = file,
            .buffer = payload[offset..],
            .offset = offset,
        }));

        if (n == 0) return error.WriteZero;

        offset += n;
    }

    // Write/Close alone do not guarantee file-data durability.
    try run(&loop, fs.Sync.init(&pool, .{ .file = file }));

    var buffer: [payload.len]u8 = undefined;
    offset = 0;

    // Reads can also be short; zero before filling the buffer means premature EOF.
    while (offset < buffer.len) {
        const n = try run(&loop, fs.ReadAt.init(&pool, .{
            .file = file,
            .buffer = buffer[offset..],
            .offset = offset,
        }));

        if (n == 0) return error.UnexpectedEndOfFile;

        offset += n;
    }

    if (!std.mem.eql(u8, payload, &buffer)) return error.ReadbackMismatch;

    try run(&loop, fs.Close.init(&pool, .{ .file = file }));

    return payload.len;
}

fn run(loop: *io.Loop, work: anytype) @typeInfo(@TypeOf(co.Frame(@TypeOf(work)).getResult)).@"fn".return_type.? {
    // This example waits for each step; the local frame stays pinned until completion.
    var frame: co.Frame(@TypeOf(work)) = .{ .coroutine = work };

    loop.executor.spawn(&frame.task);

    // A fatal run error cannot unwind buffers still borrowed by I/O.
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    frame.coroutine.deinit();
    return frame.getResult();
}
