# velo

Stackless coroutines and async I/O for Zig 0.17. Coroutines are explicit state
machines: nothing switches stacks.

- **Coroutines:** executor, `Join`, `Race`, `Yield`, `Event`, `Semaphore`, `Channel`
- **Network:** TCP and UDP with deadlines, timeouts, and cancellation
- **Threads:** cross-thread posts into a loop; `workers.Group` runs one loop per thread
  with optional CPU pinning and thread names; fixed-size `workers.Pool` for blocking work
- **Files:** positional reads/writes through io_uring (Linux), probed AIO/kqueue
  (macOS), or overlapped IOCP (Windows), with worker fallback; sync uses io_uring
  or workers, and open/create/close/preallocation use workers; opt-in direct I/O
  and io_uring registered buffers
- **Backends:** io_uring with epoll fallback (Linux), kqueue (macOS), IOCP (Windows)

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

- `Frame(C)` holds a coroutine's state, task handle, and result. Its coroutine
  returns `.pending` to yield or `.ready` to finish; `Task.wake()` schedules another poll.
- `Loop` drives the executor and native socket, timer, and file operations.
  I/O completions wake waiting tasks.
- `Control` runs posted callbacks on the loop thread. `workers.Pool` executes blocking
  actions on worker threads and posts completions back to that loop.
- The executor also works without I/O. Independent loops can run on separate threads,
  each owning its tasks and communicating through posts; `workers.Group` creates, runs,
  and drains such loops, each on its own thread.

Public modules: `velo.coroutines`, `velo.event`, `velo.workers`, and `velo.fs`.
`velo.fs.worker` exposes explicitly worker-backed file operations.

## Usage

```sh
zig fetch --save git+https://github.com/tenyoru/velo
```

```zig
// build.zig
const velo = b.dependency("velo", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("velo", velo.module("velo"));
```

```zig
const std = @import("std");
const velo = @import("velo");

pub fn main() !void {
    var loop = try velo.event.Loop.init(.{});
    defer loop.deinit();

    var timer: velo.coroutines.Frame(velo.event.Timeout) = .{
        .coroutine = velo.event.Timeout.init(&loop, .{
            .duration = .{ .sec = 0, .nsec = 1_000_000 },
        }),
    };

    loop.executor.spawn(&timer.task);

    // A loop error cannot safely unwind storage the kernel still references.
    loop.run() catch |err| std.debug.panic("I/O loop failed: {s}", .{@errorName(err)});

    try timer.getResult();
}
```

## Rules

- Pin the loop, frames, buffers, and addresses until the result is consumed,
  including after cancellation.
- `cancel()` is a request: keep polling until ready. A late cancel can still
  succeed, so clean up whatever the result owns.
- A loop belongs to one thread. Other threads may call only `post`, `retain`,
  `release`, `stop`, and `shutdown`.
- The full contracts are doc comments; start with `src/coroutines/root.zig`
  and `src/event/root.zig`.

## Examples

```sh
zig build run-counters                 # two coroutines taking turns
zig build run-wakeup                   # cross-thread post, stop, and resume
zig build run-pipeline                 # Channel and Join over a bounded buffer
zig build run-echo                     # TCP echo server; press Enter to stop
zig build run-files -- /tmp/velo-demo  # create, write, sync, and read a file
zig build run-flusher                  # coalesced deadlines and graceful worker drain
zig build run-shards -- 4 10000         # bounded request/reply across a four-loop Group
```

`flusher` closes admission and finishes pending generations before global shutdown;
shutdown alone may skip queued worker jobs. `shards` keeps requests pinned through
their replies, including cancellation. Both carry lifecycle tests.
`shards` reports throughput, mean request/reply latency, and shutdown/join time;
its timed exchange includes routing and result validation.

## Development

```sh
nix develop
just test                           # zig build test --summary all
just check -Dtarget=x86_64-windows  # link tests for another target
just fmt
```

ThreadSanitizer on Linux: `zig test src/root.zig -fllvm -fsanitize-thread -lc -j2`.

## Scalability measurements

```sh
zig build bench -Doptimize=ReleaseFast
./zig-out/bin/scalability auto idle-deadline 1000
./zig-out/bin/scalability epoll expire-all 1000  # Linux-only forced backend
```

Arguments: `backend workload count [steps]`. Backends are `auto`, `uring`, and
`epoll`; forced backends require Linux. Workloads are `idle-recv`, `idle-deadline`,
`idle-timeout`, `same-fd`, `expire-one`, and `expire-all`; count is 1–10000.
Optional steps (default 10000) apply only to non-expiration workloads.

JSON output separates admission, fixed ready-task work, observed expiration,
cancellation requests, and shutdown/drain. Socket setup and expiration waiting are
excluded from these timings. The ready-work phase measures scheduler/loop work,
not network latency. TCP workloads need roughly two descriptors per connection;
`same-fd` uses one connection for all waiters. Repeat runs on the same host and
backend; native macOS/Windows measurements are required for their scaling claims.
Epoll measurements use a fixed deadline-tree seed, reported as `deadline_seed` in
JSON, so ASLR does not change the tree shape between runs.
