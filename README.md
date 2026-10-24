# velo

Stackless coroutines and async I/O for Zig 0.17. Coroutines are explicit state
machines: nothing switches stacks.

- **Coroutines:** executor, `Machine`, `Join`, `Race`, `Scope`, `Yield`, `Event`, `Semaphore`,
  `Channel`
- **Network:** TCP and UDP with deadlines, timeouts, and cancellation; `WithTimeout` bounds
  any cancelable coroutine
- **Threads:** cross-thread posts into a loop; `workers.Group` runs one loop per thread
  with optional CPU pinning and thread names; fixed-size `workers.Pool` for blocking work
- **NUMA (Linux):** topology discovery, explicit loop/worker placement, bound or preferred
  memory and allocator backing, optional prefault/huge-page hints, and io-wq affinity
- **Files:** positional reads/writes through io_uring (Linux), probed AIO/kqueue
  (macOS), or overlapped IOCP (Windows), with worker fallback; full or data-only
  (Linux `fdatasync`) sync uses io_uring or workers, and open/create/close/preallocation
  use workers; opt-in direct I/O
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
Placement APIs live in `velo.workers.numa`.
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

    var timer: velo.coroutines.Frame(velo.event.Timeout) = .init(velo.event.Timeout.init(&loop, .{
        .duration = .{ .sec = 0, .nsec = 1_000_000 },
    }));

    // Spawns, runs until the loop drains, and returns the result. Loop errors panic:
    // storage the kernel still references cannot be unwound safely.
    try loop.block(&timer);
}
```

Use `loop.spawn(&frame)` for additional frames, and `loop.run()` directly to stop/resume
or retry run errors. Inside a coroutine, `child.poll(task).value() orelse return .pending`
forwards a child's pending state.

Most coroutines run child operations in sequence. `Machine` runs one child at a time
and calls the handler named after its state with that child's result:

```zig
const co = velo.coroutines;
const io = velo.event;

const Echo = struct {
    loop: *io.Loop,
    fd: io.Socket,
    buffer: [4096]u8 = undefined,

    pub const Result = anyerror!void;
    pub const State = union(enum) { recv: io.Recv, send: io.SendAll };

    // Runs on the first poll, once pinned, so children may borrow `buffer`.
    pub fn start(self: *Echo) co.Next(Echo) {
        return .{ .go = .{ .recv = io.Recv.init(self.loop, .{ .fd = self.fd, .buffer = &self.buffer }) } };
    }

    pub fn recv(self: *Echo, result: io.Error!usize) co.Next(Echo) {
        const n = result catch |err| return .{ .done = err };
        if (n == 0) return .{ .done = {} };

        return .{ .go = .{ .send = io.SendAll.init(self.loop, .{ .fd = self.fd, .buffer = self.buffer[0..n] }) } };
    }

    pub fn send(self: *Echo, result: co.ResultOf(io.SendAll)) co.Next(Echo) {
        result catch |err| return .{ .done = err };
        return self.start();
    }
};

var frame: co.Frame(co.Machine(Echo)) = .init(.init(.{ .loop = &loop, .fd = fd }));
```

Cancellation reaches the active child and every later child; handlers still choose the
result. A missing or mistyped handler fails to compile with its expected signature.
Write `poll` by hand for custom wake sources.

Concurrency composes the same way:

- `Join(.{ .user = LoadUser, .orders = LoadOrders })` polls children on one task and
  returns every result under the same names; positional `.{ A, B }` gives a tuple.
- `Race(.{ .recv = io.Recv, .timeout = io.Timeout })` reports `winner` and still returns
  every result, so a losing success can be cleaned up.
- `Scope(C)` runs a dynamic number of children as separate tasks in caller-owned slots.
  The first child error cancels the rest; `scope.wait()` completes after all drain, and
  `scope.reserve()` waits for a free slot before work such as an accept.
- `io.WithTimeout(C)` bounds a child by a timer. A success always wins, even after
  expiry; an error after expiry becomes `error.Timeout`.

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
zig build run-shards -- 4 10000 auto numa  # Linux: placed state and per-shard buffers
```

`flusher` closes admission and finishes pending generations before global shutdown;
shutdown alone may skip queued worker jobs. `shards` keeps requests pinned through
their replies, including cancellation. Both carry lifecycle tests.
`shards` reports throughput, mean request/reply latency, and shutdown/join time;
its timed exchange includes routing and result validation.

### Explicit NUMA placement

Use `const numa = velo.workers.numa`. Placement is opt-in: default loops and pools
do no topology discovery or affinity/policy changes. `Topology.discover` reports
permitted CPUs, memory nodes, core siblings, and distances. `numa.plan(allocator,
&topology, .{ .count = 4, .memory = .bind })` spreads cores across nodes before SMT;
free the returned slice after use. Pass it as `Group.Options.placement`.

`Group.Options.pin` has been replaced by the planner. With a null topology and
the default `.memory = .none`, planning selects sorted allowed CPUs, including the
existing 64-bit Windows current-processor-group affinity support. NUMA memory
policies and topology discovery remain Linux-only.

`Region.init(bytes, .{ .bind = node })` creates fresh placed memory; `.prefer` allows
fallback under pressure. `Region.initWithOptions` adds alignment, `.prefault`, and
`.huge_pages` options. Prefaulting requires Linux 5.14+; huge pages are a hint, not a
guarantee. `PageAllocator.init(policy, options)` provides zero-filled page backing
for standard arenas/debug allocators; use those for small objects.

`Group.initWithHooks` prepares per-shard buffers before registration and cleans them
up after loop teardown. Pools must drain and join before that teardown.
`Pool.initWithOptions` accepts `.count` and a Linux `.cpus` set;
`Topology.nodeCpus` supplies a caller-owned node CPU list. `LoopOptions.iowq_cpus`
configures Linux io_uring workers, with registration errors propagated. Some kernels
intersect overlapping masks with inherited user affinity; the shard example excludes
the loop CPU from its worker set when another node CPU is available.

Thread policies affect fresh faults, not
existing heap pages; kernel-owned I/O memory is outside the placement guarantee.
Topology is a snapshot, and binding does not guarantee future allocations succeed.
The NUMA shard example uses placed arenas, prefaulted buffers, and a node-restricted
blocking pool per shard. Warmup jobs verify worker affinity before exchange starts.
It reports same-node and cross-node requests; registered buffers demonstrate setup,
not fixed-buffer file-transfer performance.

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

### NUMA measurements (Linux)

```sh
zig build bench -Doptimize=ReleaseFast
./zig-out/bin/numa unpinned 2 64 1000000
./zig-out/bin/numa pinned   2 64 1000000
./zig-out/bin/numa local    2 64 1000000
./zig-out/bin/numa remote   2 64 1000000
./zig-out/bin/numa local    2 64 1000000 prefault
./zig-out/bin/numa local    2 64 1000000 huge
```

Arguments: `mode [threads] [MiB-per-thread] [iterations] [lazy|prefault|huge]`.
Preparation defaults to `lazy`; `prefault` and `huge` require local/remote mode.
The same dependent-memory workload runs in every mode; allocation, initialization,
and warmup are measured separately from steady traversal. Initialization already
writes every page, so prefaulting primarily moves work into the allocation phase.
The first two modes use coordinator-first-touched pages with inherited memory policy;
`local` and `remote` bind pages before touching them. Output reports CPU placement,
post-run residency samples, throughput, elapsed nanoseconds per access, and actual
huge-page bytes from overlapping VMAs (which can include neighboring mappings).
`remote` explicitly skips without a different permitted memory node. Use a real
multi-node host and repeated runs for locality claims; single-node runs are smoke tests.
