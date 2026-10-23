# velo

Stackless coroutines and I/O in Zig for concurrent programs and backend services,
such as databases.
The name is short for "velocity".

Zig has no language-level stackless coroutines (the evented `std.Io` backends use
stackful fibers), so coroutines here are explicit state machines.

## Toolchain

- Zig 0.17.0 (`minimum_zig_version` in `build.zig.zon`)
- I/O backends: Linux io_uring with epoll fallback, macOS kqueue, Windows IOCP
  (64-bit Windows only)
- The Nix shell targets x86_64-linux.
- `nix develop` provides `zig_0_17` (nixos-unstable), `zls` (zigtools/zls flake input,
  because nixpkgs has no `zls_0_17` yet) and `just`
- Run recipes and the editor inside `nix develop`; the global zls is 0.16 and does not
  match Zig 0.17

## Commands

- `just build`: `zig build`
- `just check -Dtarget=<target>`: compile and link tests for a target without running
- `just test`: `zig build test --summary all`
- `just fmt`: `zig fmt build.zig build.zig.zon src examples bench`
- `zig test src/root.zig -fllvm -fsanitize-thread -lc -j2`: Linux ThreadSanitizer tests
- `zig build run-<counters|wakeup|echo|files|pipeline|flusher|shards> [-- args]`: run an example
- `zig build bench -Doptimize=ReleaseFast`: install `zig-out/bin/scalability`
- `zig build run-scalability -- <auto|uring|epoll> <workload> <count> [steps]`: phase-separated benchmark
- `zig test --dep velo -Mroot=examples/threadguard.zig -Mvelo=src/root.zig`: must panic
  in Debug/ReleaseSafe
- Release: bump `.version` in `build.zig.zon`, commit, push tag `v<version>`;
  `release.yml` rejects a mismatched tag

## Layout

- `src/root.zig`: public API root, exported as module `velo`
- `src/state.zig`: internal lifecycle enums for waiters, channels, tasks, I/O, and jobs
- `src/ThreadGuard.zig`: test-only active-executor ownership checks; zero-sized
  in normal executables and in unsafe or single-threaded test builds
- `src/coroutines/root.zig`: `velo.coroutines`; `Poll`, `Frame`, `Yield`, `Join`,
  `Race`; its doc comment is the coroutine contract, read it before writing a
  coroutine
- `src/coroutines/test.zig`: scheduling and composition tests; primitive-specific
  tests stay beside their implementations
- `src/coroutines/Executor.zig`: single-threaded intrusive run queue, `Task` (the
  waker), task registry and cancellation
- `src/coroutines/Event.zig`, `Semaphore.zig`, `Channel.zig`: single-threaded FIFO
  primitives

- `src/event/root.zig`: `velo.event`; loop, network operations, timers, backend
  selection, and lifetime contract
- `src/event/Operation.zig`: shared operation state, typed coroutine wrappers,
  cancellation, and completion accounting
- `src/event/loop.zig`: `Shared(Loop, hooks)`: guarded `run` skeleton, `deinit`, and
  control forwarders; comptime check of backend fields and the
  `turn`/`inflight`/`park`/`close` hooks
- `src/event/backends/Linux.zig`, `Uring.zig`: Linux loop/selection, io_uring
  submissions and cancellation
- `src/event/backends/Epoll.zig`: Linux readiness fallback, per-direction FIFO over
  fd buckets, run queue, deadline treap
- `src/event/backends/Kqueue.zig`: macOS readiness, serialized per-direction waiters,
  software timers, kevent64 and capability-probed native AIO reads/writes
- `src/event/backends/DarwinAio.zig`: Darwin AIO ABI bindings, zero-length completion
  probe, submission and rejection diagnostics; kevent delivery owns reaping
- `src/event/backends/Iocp.zig`, `win32.zig`: Windows completion loop and missing
  WinAPI bindings (also used by `workers.Group` affinity)
- `src/event/Control.zig`: cross-thread posts, retain/release, stop, shutdown,
  operation registry
- `src/event/stream.zig`: `SendAll`/`ReadExact` short-transfer loops

- `src/workers/root.zig`: `velo.workers`; exports `Pool` and `Group`
- `src/workers/Pool.zig`: fixed-size blocking pool, private jobs, generic `Work(C)`;
  completions and cancellation remain connected to the owning event loop
- `src/workers/Group.zig`: one loop per thread; pin/name/create in-thread, `entry`
  seeding, all-or-nothing start with cancel-and-drain rollback, close barrier
  before loop deinit
- `src/workers/test.zig`: worker pool and group tests

- `src/fs/root.zig`: `velo.fs`; operations select from
  `loop.file_ops` at init, with worker fallback; Open/Create/Close always workers;
  `worker` exposes explicitly worker-backed operations
- `src/fs/native.zig`: native file wrappers over shared `event.Operation`, filesystem errno mapping
- `src/fs/worker.zig`: worker-backed file implementation using `workers.Pool.Work`,
  exposed through `velo.fs.worker`
- `src/fs/windows.zig`: compatible file handles reopened for overlapped IOCP;
  worker transfers use private event-tagged OVERLAPPEDs, never Threaded APCs
- `src/fs/test.zig`: forced uring/epoll/fallback, errno, cancellation, pressure, drain tests
- `src/fs/test_windows.zig`, `test_macos.zig`: native platform file tests;
  macOS native cases explicitly skip when the AIO-to-kqueue probe fails

- `src/event/tests/support.zig`: shared socket setup, short-transfer echo, deadlines,
  loop driving, and drain checks
- `src/event/tests/uring.zig`: real-kernel timer, timeout, queue-pressure, and TCP echo tests
- `src/event/tests/portable.zig`: native socket/timer integration tests on all three OSes
- `src/event/tests/lifecycle.zig`: Accept/Recv/timer cancellation and real Join/Race tests
- `src/event/tests/runtime.zig`: cross-thread control, shutdown, datagram, and
  stream-helper tests

- `examples/`: runnable `counters`, `wakeup`, `echo`, `files`, `pipeline`, `flusher`, `shards`; all except
  files carry tests
- `examples/counters.zig`: two independent counters print alternating steps by
  requeueing themselves, without timers or delays
- `examples/wakeup.zig`: cross-thread post/stop/resume example; tests use automatic
  selection and forced epoll on Linux
- `examples/flusher.zig`: synchronized deadline mailbox, generation snapshots,
  timer rearm and graceful/canceled worker drain
- `examples/shards.zig`: bounded cross-loop requests over `workers.Group`, pinned
  replies and coordinated shutdown; optional shard count, requests per shard and
  Linux epoll selection
- `bench/scalability.zig`: portable TCP/timer workloads with separate admission,
  fixed ready work, expiration, cancellation and drain timings; included in test/check
- `examples/threadguard.zig`: deliberately panicking wrong-thread wake; not in
  `build.zig`, run by hand

- `.github/workflows/test.yml`: native three-platform CI, ReleaseSafe,
  foreign-target links; reusable
- `.github/workflows/release.yml`: `v*` tag → version check, tests, GitHub release
  with fetch hash
- `build.zig`: the `velo` module, examples, and the `test`/`check`/`run-*` steps
- `README.md`: minimal user-facing overview, usage, rules, examples; API contracts
  stay in doc comments, references in `Code sources` below
- `flake.nix`, `shell.nix`: development shell
- `.gitignore`: deny-by-default; every new tracked path needs an allowlist entry

## Code sources

Check these before writing I/O or scheduler code; add new references here.
Entries were checked on 2026-10-16 (Tokio, Go, and Ochi later).

- **Zig 0.17 std:** the only source that matches the compiler. Read it locally:
  `zig env` prints `lib_dir`, and the std sources are under `<lib_dir>/std`.
  - [`std/os/linux/IoUring.zig`](https://codeberg.org/ziglang/zig/src/tag/0.17.0/lib/std/os/linux/IoUring.zig):
    raw ring, SQE preparation helpers
  - [`std/Io/Uring.zig`](https://codeberg.org/ziglang/zig/src/tag/0.17.0/lib/std/Io/Uring.zig):
    evented `std.Io` over io_uring (stackful fibers)
  - [Language reference 0.17.0](https://ziglang.org/documentation/0.17.0/): labeled
    `switch` with `continue :label`, the basis of hand-written state machines
- **TigerBeetle** (Zig 0.16, Apache-2.0):
  [`src/io`](https://github.com/tigerbeetle/tigerbeetle/tree/main/src/io) completion-based
  io_uring/epoll/kqueue layer;
  [design post](https://tigerbeetle.com/blog/2022-11-23-a-friendly-abstraction-over-iouring-and-kqueue)
  (predates the code);
  [`darwin.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/io/darwin.zig)
  and [`windows.zig`](https://github.com/tigerbeetle/tigerbeetle/blob/main/src/io/windows.zig)
- **libxev** (Zig 0.16, MIT):
  [`io_uring.zig`](https://github.com/mitchellh/libxev/blob/main/src/backend/io_uring.zig),
  [`kqueue.zig`](https://github.com/mitchellh/libxev/blob/main/src/backend/kqueue.zig):
  submission pressure, completion ownership, nonblocking connect, Darwin socket flags
- **zio** (Zig 0.17, MIT, inspected `4e4e1ed10da974f15b119207d346a78cc6c363e4`):
  [`src/ev`](https://github.com/lalinsky/zio/tree/main/src/ev) callback loop under
  stackful coroutines;
  [`linux.zig`](https://github.com/lalinsky/zio/blob/4e4e1ed10da974f15b119207d346a78cc6c363e4/src/ev/backends/linux.zig)
  and [`epoll.zig`](https://github.com/lalinsky/zio/blob/4e4e1ed10da974f15b119207d346a78cc6c363e4/src/ev/backends/linux/epoll.zig)
  fallback only for unavailable ring setup, readiness registration, cancellation;
  [`group.zig`](https://github.com/lalinsky/zio/blob/4e4e1ed10da974f15b119207d346a78cc6c363e4/src/group.zig)
  and [`select.zig`](https://github.com/lalinsky/zio/blob/4e4e1ed10da974f15b119207d346a78cc6c363e4/src/select.zig)
  cancel-and-drain; [`iocp.zig`](https://github.com/lalinsky/zio/blob/main/src/ev/backends/iocp.zig)
  overlapped ownership, ConnectEx; `src/ev/loop.zig` for the loop itself
- **Tokio** (Rust, MIT): [repo](https://github.com/tokio-rs/tokio),
  [`runtime/scheduler/`](https://github.com/tokio-rs/tokio/tree/master/tokio/src/runtime/scheduler),
  [`runtime/io/driver.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/runtime/io/driver.rs):
  stackless futures, task scheduling, readiness-driven I/O
- **Go runtime** (BSD-3-Clause): [repo](https://github.com/golang/go),
  [`HACKING.md`](https://github.com/golang/go/blob/master/src/runtime/HACKING.md),
  [`netpoll.go`](https://github.com/golang/go/blob/master/src/runtime/netpoll.go):
  scheduler parking/wakeup, network polling (stackful goroutines)
- **Seastar** (C++, Apache-2.0): [repo](https://github.com/scylladb/seastar),
  [`tutorial.md`](https://github.com/scylladb/seastar/blob/master/doc/tutorial.md),
  [`reactor_backend.cc`](https://github.com/scylladb/seastar/blob/master/src/core/reactor_backend.cc):
  shared-nothing reactor, futures/continuations
- **Silk** (C++, Apache-2.0): [repo](https://github.com/ClickHouse/silk),
  [`coroutines.md`](https://github.com/ClickHouse/silk/blob/main/docs/coroutines.md):
  stackful fibers over io_uring, stackless vs stackful benchmarks
- **PhotonLibOS** (C++, Apache-2.0): [repo](https://github.com/alibaba/PhotonLibOS),
  [`io/`](https://github.com/alibaba/PhotonLibOS/tree/main/io),
  [Stackful Coroutine Made Fast](https://photonlibos.github.io/blog/stackful-coroutine-made-fast)
- **Ochi** (Zig 0.16, Apache-2.0, inspected `ffa7366a0b9e0ff1cc6fc8ea84c6e9758a932d95`):
  [`docs/internal/fibers.md`](https://github.com/ochi-team/ochi/blob/ffa7366a0b9e0ff1cc6fc8ea84c6e9758a932d95/docs/internal/fibers.md)
  consumer workload: per-object flush timers, merge pool, shutdown drain
- **io_uring:** [Lord of the io_uring](https://unixism.net/loti/);
  [io_uring_setup(2)](https://man7.org/linux/man-pages/man2/io_uring_setup.2.html)
  feature flags, NODROP completion retention, overflow handling
- **Darwin AIO** (checked 2026-10-21):
  [Apple XNU kern_aio.c](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/kern_aio.c),
  `SIGEV_KEVENT`, `filt_aioprocess`: completion errno/result in kevent64 `ext`,
  delivery reaps the request. Probe runtime support; do not infer it from headers.
  Submitted requests drain normally rather than relying on canceled-kevent delivery.
- **Windows file I/O** (checked 2026-10-21):
  [ReadFile](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-readfile),
  [ReOpenFile](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-reopenfile),
  [GetQueuedCompletionStatus](https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-getqueuedcompletionstatus).
  Native success still awaits a packet. Worker transfers alone use a private
  event's low bit to suppress IOCP packets; APC-based std.Io.Threaded transfers
  cannot be used on associated handles. Windows Sync remains synchronous/worker-backed.

Rules:

- When delegating I/O work, name relevant reference files and the invariants to
  inspect (e.g. TigerBeetle/libxev submission pressure and completion ownership).
- macOS: libxev kqueue and TigerBeetle `darwin.zig`. Windows: zio IOCP and TigerBeetle
  `windows.zig`. Do not mix skip-on-success and always-await-IOCP-packet ownership policies.
- Never free a losing race's frame before completion; preserve both results so
  successful losing resources can be closed.
- TigerBeetle, libxev, and Ochi target Zig 0.16: port their ideas, do not paste their code.
- Seastar, Silk, PhotonLibOS, Go, and zio are design references. Except for Seastar's
  futures and Tokio, they use stackful coroutines; velo must not rely on stack switching.
- velo has no license yet. Ask before copying upstream code, and keep the upstream
  notice when you do (MIT: copyright line; Apache-2.0: license, NOTICE, change note).
- A reference does not justify a dependency.
