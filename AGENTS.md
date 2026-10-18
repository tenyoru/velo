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
- Runtime-tested on x86_64-linux; macOS/Windows x86_64 and aarch64 are cross-linked,
  not runtime-verified. The Nix shell currently targets x86_64-linux.
- `nix develop` provides `zig_0_17` (nixos-unstable), `zls` (zigtools/zls flake input,
  because nixpkgs has no `zls_0_17` yet) and `just`
- Run recipes and the editor inside `nix develop`; the global zls is 0.16 and does not
  match Zig 0.17
- No runtime dependencies. GitHub Actions configures native Linux/macOS/Windows tests;
  hosted results remain unverified until the workflow is pushed and run.

## Commands

- `just build`: `zig build`
- `just check -Dtarget=<target>`: compile and link tests for a target without running
- `just test`: `zig build test --summary all`
- `just fmt`: `zig fmt build.zig build.zig.zon src examples`
- `zig test src/root.zig -fllvm -fsanitize-thread -lc -j2`: Linux ThreadSanitizer tests
- `zig build run-<counters|wakeup|echo|files|pipeline> [-- args]`: run an example
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

- `src/io/root.zig`: public aliases, backend selection, and lifetime contract
- `src/io/Operation.zig`: shared operation state, typed coroutine wrappers,
  cancellation, and completion accounting
- `src/io/backends/Linux.zig`, `Uring.zig`: Linux loop/selection, io_uring
  submissions and cancellation
- `src/io/backends/Epoll.zig`: Linux readiness fallback, per-direction FIFO,
  software timers
- `src/io/backends/Kqueue.zig`: macOS readiness, serialized per-direction waiters,
  software timers
- `src/io/backends/Iocp.zig`, `win32.zig`: Windows completion loop and missing
  WinAPI bindings
- `src/io/Control.zig`: cross-thread posts, retain/release, stop, shutdown,
  operation registry
- `src/io/WorkerPool.zig`, `src/io/fs.zig`: bounded blocking workers and pooled
  file operations
- `src/io/stream.zig`: `SendAll`/`ReadExact` short-transfer loops

- `src/io/tests/support.zig`: shared socket setup, short-transfer echo, deadlines,
  loop driving, and drain checks
- `src/io/tests/uring.zig`: real-kernel timer, timeout, queue-pressure, and TCP echo tests
- `src/io/tests/portable.zig`: native socket/timer integration tests on all three OSes
- `src/io/tests/lifecycle.zig`: Accept/Recv/timer cancellation and real Join/Race tests
- `src/io/tests/runtime.zig`: cross-thread control, shutdown, datagram, and
  stream-helper tests
- `src/io/tests/worker_pool.zig`, `fs.zig`: worker pool and file-operation tests

- `examples/`: runnable `counters`, `wakeup`, `echo`, `files`, `pipeline`; all except
  files carry tests
- `examples/counters.zig`: two independent counters print alternating steps by
  requeueing themselves, without timers or delays
- `examples/wakeup.zig`: cross-thread post/stop/resume example; tests use automatic
  selection and forced epoll on Linux
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
