# velo

Stackless coroutines and async I/O for Zig 0.17. Coroutines are explicit state
machines: nothing switches stacks.

- **Coroutines:** `Machine` for sequential operations, `Join`/`Race` for composition,
  `Scope` for child tasks, plus channels and synchronization primitives.
- **I/O:** TCP, UDP, timers, files, and file/stream transfers with cancellation;
  socket deadlines and timeouts.
- **Workers:** a blocking pool, one-loop-per-thread groups, and bounded cross-thread
  mailboxes, with optional CPU affinity and Linux NUMA placement.
- **Backends:** Linux io_uring with epoll fallback, macOS kqueue, Windows IOCP.

## Usage

```sh
zig fetch --save git+https://github.com/tenyoru/velo
```

Add the module in `build.zig`:

```zig
const velo = b.dependency("velo", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("velo", velo.module("velo"));
```

```zig
const velo = @import("velo");
const co = velo.coroutines;
const io = velo.event;

pub fn main() !void {
    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    var timer: co.Frame(io.Timeout) = .init(.init(&loop, .{
        .duration = io.milliseconds(1),
    }));
    try loop.block(&timer);
}
```

`block` spawns the frame and runs until **all work drains**, returning its result.
It panics on loop errors or an incomplete drain to avoid freeing borrowed storage.
Use `loop.spawn(&frame)` and `loop.run()` for stop/resume or retrying loop errors.

## Architecture

```mermaid
flowchart LR
    subgraph loop["Loop (owning thread)"]
        executor["Executor"] -->|poll| frames["Coroutine frames"]
        frames -->|submit I/O| backend["I/O backend"]
        backend -->|wake| executor
        control["Control"] -->|wake tasks| executor
    end

    frames -->|blocking work| workers["workers.Pool"]
    workers -->|completion post| control
    threads["Other threads"] -->|post| control
```

`Frame(C)` holds a coroutine's state, task, and result. Polling returns `.pending`
or `.ready`; waking a task schedules another poll. The executor also works without
I/O. Scale with independent loops, one per thread.

## Rules

- Pin loops, frames, buffers, and addresses until results are consumed.
- Cancellation is a request: drain to ready before freeing storage. A late cancel
  can still succeed; clean up successful results, including losing race results.
- A loop belongs to one thread. Other threads may call only `post`, `retain`,
  `release`, `stop`, and `shutdown`.
- Close sockets after their operations finish; drain all work before loop deinit.

API contracts live in doc comments: [coroutines](src/coroutines/root.zig),
[I/O](src/event/root.zig), [workers](src/workers/root.zig), and [files](src/fs/root.zig).
See [Machine](src/coroutines/machine.zig) for sequential coroutines,
[Mailbox](src/event/Mailbox.zig) for cross-loop messaging, and
[NUMA](src/workers/numa/root.zig) for explicit placement.

## Examples

```sh
zig build run-counters                 # two coroutines taking turns
zig build run-wakeup                   # cross-thread post, stop, and resume
zig build run-pipeline                 # Channel and Join over a bounded buffer
zig build run-echo                     # TCP server using Machine and Server
zig build run-files -- /tmp/velo-demo   # create, write, sync, and read a file
zig build run-flusher                  # coalesced deadlines and graceful drain
zig build run-shards -- 4 10000         # bounded requests across four loops
zig build run-mailbox                   # typed request/reply with backpressure
```

Browse [examples/](examples/) for complete programs. `echo` stops on Enter or stdin
EOF; `shards` accepts `auto numa` after its arguments for Linux NUMA placement.

## Development

```sh
nix develop
just test
just check -Dtarget=x86_64-windows
just fmt
```

ThreadSanitizer: `zig test src/root.zig -fllvm -fsanitize-thread -lc -j2`.

Build benchmarks with `zig build bench -Doptimize=ReleaseFast`:
[scalability](bench/scalability.zig) measures TCP/timer workloads;
[numa](bench/numa.zig) measures Linux memory locality;
[crossloop](bench/crossloop.zig) measures cross-loop posts. Run any of them in
`zig-out/bin/` without arguments for usage.
For the Linux TCP echo comparison with C/libuv and Rust/Tokio, see
[benchmarks](bench/README.md).
