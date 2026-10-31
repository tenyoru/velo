# velo

Stackless coroutines and I/O in Zig for concurrent programs and backend services,
such as databases.
The name is short for "velocity".

Zig has no language-level stackless coroutines (the evented `std.Io` backends use
stackful fibers), so coroutines here are explicit state machines.

## Toolchain

- Zig 0.17.0 (`minimum_zig_version` in `build.zig.zon`)
- I/O backends: Linux io_uring (5.11+, for timed waits) with epoll fallback, macOS kqueue, Windows IOCP
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
- `zig build run-<counters|wakeup|echo|files|pipeline|flusher|shards|mailbox> [-- args]`: run an example
- `zig build bench -Doptimize=ReleaseFast`: install `zig-out/bin/scalability`, `numa`, and `crossloop`
- `zig build run-scalability -- <auto|uring|epoll> <workload> <count> [steps]`: phase-separated benchmark
- `zig build run-crossloop -Doptimize=ReleaseFast -- <auto|uring|epoll> <ping-pong|busy|paced> <messages> [loops=N] [rate=N] [work=N] [spin=NS] [pinned]`: cross-loop post cost, CPU and latency
- `zig build run-numa -Doptimize=ReleaseFast -- <unpinned|pinned|local|remote> [threads] [MiB-per-thread] [iterations] [lazy|prefault|huge]`: Linux memory-locality benchmark
- `zig build run-shards -- 4 10000 auto numa`: Linux placed shards with per-loop registered buffers
- `zig test --dep velo -Mroot=examples/threadguard.zig -Mvelo=src/root.zig`: must panic
  in Debug/ReleaseSafe
- Release: bump `.version` in `build.zig.zon`, commit, push tag `v<version>`;
  `release.yml` rejects a mismatched tag

## Layout

- `src/root.zig`: public API root, exported as module `velo`
- `src/state.zig`: internal lifecycle enums for waiters, channels, tasks, I/O, and jobs
- `src/ThreadGuard.zig`: test-only active-executor ownership checks; zero-sized
  in normal executables and in unsafe or single-threaded test builds
- `src/coroutines/root.zig`: `velo.coroutines`; `Poll`, `ResultOf`, `Frame`, `Yield`,
  N-ary named/positional `Join`, named `Race`; its doc comment is the coroutine
  contract, read it before writing a coroutine
- `src/coroutines/machine.zig`: `Machine(Ctx)`/`Next`: one active child per state,
  handlers named after states, `start` after pinning, cancellation forwarded to later
  children, comptime handler-signature diagnostics; separate active tag avoids copying
  worker-owned payloads during polling/cancellation
- `src/coroutines/scope.zig`: `Scope(C)`: children as separate tasks in caller slots,
  void/E!void results, first error cancels, FIFO reservations win cancellation, `wait`
  drains reserved and running slots
- `src/coroutines/test.zig`: scheduling and composition tests; primitive-specific
  tests stay beside their implementations
- `src/coroutines/Executor.zig`: single-threaded intrusive run queue, `Task` (the
  waker), task registry and cancellation; safe-build task names, `writeTasks`/`dumpTasks`;
  `Stats` counters; share-weighted `Class` queues, charged per slice (`vruntime`)
- `src/coroutines/Event.zig`, `Semaphore.zig`, `Channel.zig`: single-threaded FIFO
  primitives

- `src/event/root.zig`: `velo.event`; loop, network operations, timers, backend
  selection, and lifetime contract
- `src/event/Operation.zig`: shared operation state, typed coroutine wrappers,
  cancellation, and completion accounting
- `src/event/loop.zig`: `Shared(Loop, hooks)`: guarded `run` skeleton, slices of 64
  polls capped by a 500 us quota with an adaptive clock stride, `shouldYield`, safe-build
  stall reports, the `enterPark` wrapper around `park`, `deinit`,
  `spawn`/`block` frame helpers, control forwarders, and safe-build live-task reports
  on undrained deinit or stalled `block`; comptime check of backend fields and the
  `turn`/`inflight`/`park`/`close` hooks
- `src/event/backends/Linux.zig`, `Uring.zig`: Linux loop/selection, io_uring
  submissions and cancellation; explicit `LoopOptions.iowq_cpus` registration;
  `Timeout` as a user-space treap bounding `timedEnter` (EXT_ARG), `idle_spin_ns`
- `src/event/backends/Epoll.zig`: Linux readiness fallback, per-direction FIFO over
  fd buckets, run queue, deadline treap
- `src/event/backends/Kqueue.zig`: macOS readiness, serialized per-direction waiters,
  software timers, kevent64 and capability-probed native AIO reads/writes
- `src/event/backends/DarwinAio.zig`: Darwin AIO ABI bindings, zero-length completion
  probe, submission and rejection diagnostics; kevent delivery owns reaping
- `src/event/backends/Iocp.zig`, `win32.zig`: Windows completion loop and missing
  WinAPI bindings (also used by `workers.Group` affinity)
- `src/event/Control.zig`: cross-thread posts, retain/release, stop, shutdown,
  operation registry; `enterPark` seq_cst recheck lets posts skip the wake of an awake loop
- `src/event/Mailbox.zig`: bounded typed MPSC mailbox; native mutex, FIFO Send/Recv,
  thread-safe trySend/close, post-drained cancellation, receiving-loop retention until
  close, shutdown registration and buffered-value cleanup with tryRecv
- `src/event/stream.zig`: `SendAll`/`ReadExact` short-transfer loops
- `src/event/transfer.zig`, `transfer_linux.zig`: `Transfer` between distinct regular files
  and streams; bounded worker kernel copies (copy_file_range/sendfile/splice), async buffered
  fallback, explicit offsets, retained progress/unwritten input, worker-local SIGPIPE handling
- `src/event/net.zig`: `Address` (std `IpAddress`), `NativeAddress`, `Listener`,
  `Stream` with a shared deadline, `Dial` closing its socket unless connected
- `src/event/server.zig`: `Server(connect)`: acceptor `Machine` (reserve, accept, backoff,
  drain) over a `Scope` of connections; nonempty slots, server-owned stream close,
  isolated handler errors and propagated backoff-timer errors
- `src/event/timeout.zig`: `WithTimeout(C)`: lazy timer start, success beats expiry,
  error after expiry maps to `Timeout`

- `src/workers/root.zig`: `velo.workers`; exports `Pool`, `Group`, and `numa`
- `src/workers/Pool.zig`: fixed-size blocking pool, private jobs, generic `Work(C)`;
  completions and cancellation remain connected to the owning event loop
- `src/workers/Group.zig`: one loop per thread; pin/name/create in-thread, `entry`
  seeding, all-or-nothing start with cancel-and-drain rollback, close barrier
  before loop deinit; Linux explicit placement and `initWithHooks` preparation before
  buffer registration, cleanup after loop storage release; `Options.pin` replaced by `numa.plan`
- `src/workers/test.zig`: worker pool and group tests
- `src/workers/group_placement_test.zig`, `pool_placement_test.zig`: NUMA/affinity,
  preparation rollback, per-shard buffer lifetime, worker startup and completion tests
- `src/workers/numa/root.zig`: dynamic Linux affinity and existing Windows CPU-only
  affinity; Linux memory policy, aligned bound/preferred `Region`, prefault/THP hints
  and nonfaulting residency queries; memory APIs unsupported elsewhere
- `src/workers/numa/Topology.zig`: effective CPUs, physical node IDs, memory permissions,
  unique core keys and node distances; sysfs/procfs discovery and sparse/restricted fixtures
- `src/workers/numa/Plan.zig`: shared core-first node-round-robin placement; null
  topology gives a CPU-only plan without NUMA discovery
- `src/workers/numa/PageAllocator.zig`, `memory_test.zig`: zero-filled placed page
  backing for standard allocators, over-alignment, prefault, THP and cleanup tests
- `src/event/tests/iowq.zig`: forced async pipe reads, observed kernel-worker affinity,
  default/epoll behavior and registration-error tests

- `src/fs/root.zig`: `velo.fs`; operations select from
  `loop.file_ops` at init, with worker fallback and separate tag/child storage;
  Open/Create/Close always workers;
  `worker` exposes explicitly worker-backed operations
- `src/fs/native.zig`: native file wrappers over shared `event.Operation`, filesystem errno mapping
- `src/fs/worker.zig`: worker-backed file implementation using `workers.Pool.Work`,
  exposed through `velo.fs.worker`; OpenDirect/CreateDirect, positional Linux errno
  handling, and grow-only `Allocate` (native platform reservation, no zero-fill emulation)
- `src/fs/DirectFile.zig`: direct file with its OS-reported alignment; readAt/writeAt
  reject misaligned requests before submission, `fallback` only applies when alignment
  is unknown
- `src/fs/linux.zig`, `macos.zig`: platform syscall helpers for direct I/O,
  positional transfers, data-only sync, and allocation; Darwin ABI definitions
- `src/fs/windows.zig`: compatible file handles reopened for overlapped IOCP;
  worker transfers use private event-tagged OVERLAPPEDs, never Threaded APCs
- `src/fs/tests/portable.zig`, `linux.zig`: portable contracts and Linux backend tests;
  `support.zig` shares frame completion and the positional roundtrip
- `src/fs/tests/windows.zig`, `macos.zig`: native platform file tests;
  macOS native cases explicitly skip when the AIO-to-kqueue probe fails

- `src/event/tests/support.zig`: shared socket setup, short-transfer echo, deadlines,
  loop driving, and drain checks
- `src/event/tests/uring.zig`: real-kernel timer, timeout, queue-pressure, and TCP echo tests
- `src/event/tests/portable.zig`: native socket/timer integration tests on all three OSes
- `src/event/tests/lifecycle.zig`: Accept/Recv/timer cancellation and real Join/Race tests
- `src/event/tests/transfer.zig`, `transfer_linux.zig`: file/stream combinations, offsets,
  native and buffered paths, EOF, failures, cancellation/deadlines, SIGPIPE and pipe recovery
- `src/event/tests/net.zig`: address conversion, listener/dial/stream exchange,
  refused and pre-canceled dials
- `src/event/tests/server.zig`: slot backpressure, handler errors, live/pre-poll
  cancellation, empty-slot rejection and backoff errors; deadline-bounded client scripts
- `src/event/tests/runtime.zig`: cross-thread control, shutdown, datagram, and
  stream-helper tests
- `src/event/tests/mailbox.zig`: bounded FIFO, ownership, delayed notifications,
  cancellation/shutdown and concurrent producer ordering

- `examples/`: runnable `counters`, `wakeup`, `echo`, `files`, `pipeline`, `flusher`, `shards`, `mailbox`; all except
  files carry tests
- `examples/counters.zig`: two independent counters print alternating steps by
  requeueing themselves, without timers or delays
- `examples/wakeup.zig`: cross-thread post/stop/resume example; tests use automatic
  selection and forced epoll on Linux
- `examples/flusher.zig`: synchronized deadline mailbox, generation snapshots,
  timer rearm and graceful/canceled worker drain
- `examples/shards.zig`: bounded cross-loop requests over `workers.Group`, pinned
  replies and coordinated shutdown; optional shard count, requests per shard and
  Linux epoll selection; optional `numa` uses placed arenas, prefaulted buffers,
  node-restricted pools/io-wq, worker warmup and cross-node post counts
- `bench/scalability.zig`: portable TCP/timer workloads with separate admission,
  fixed ready work, expiration, cancellation and drain timings; included in test/check
- `bench/numa.zig`: Linux dependent-memory benchmark, unpinned/pinned/local/remote;
  separately measured setup/warmup, CPU/residency/THP observation; included in test/check
- `bench/crossloop.zig`: cross-loop ping-pong, busy receiver and paced traffic over
  `workers.Group`; CPU per message, latency percentiles and `Loop.stats`
- `examples/mailbox.zig`: three client loops and one server exchange typed requests
  and replies over bounded Mailboxes; graceful closure and error-path drain, tested
  with automatic selection and forced epoll, including ThreadSanitizer
- `examples/threadguard.zig`: deliberately panicking wrong-thread wake; not in
  `build.zig`, run by hand

- `.github/workflows/test.yml`: native three-platform CI, ReleaseSafe,
  foreign-target links; reusable
- `.github/workflows/release.yml`: `v*` tag → version check, tests, GitHub release
  with fetch hash
- `build.zig`: the `velo` module, examples, and the `test`/`check`/`run-*` steps
- `README.md`: keep concise—overview, quick start, architecture, essential rules, and
  example commands. Link to source docs instead of adding API inventories or long guides;
  contracts stay in doc comments, references in `Code sources` below
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
  stackless futures, task scheduling, readiness-driven I/O;
  [`sync/mpsc/bounded.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/mpsc/bounded.rs)
  and [`sync/mpsc/chan.rs`](https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/mpsc/chan.rs):
  bounded admission, close-then-drain and wake registration. Velo keeps unsuccessful
  send values caller-owned and drains posts before freeing canceled frames; native
  mutex operations use Zig 0.17 `std/Io/Threaded.zig`, without a stackful I/O backend.
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
  feature flags, NODROP completion retention, overflow handling;
  [io_uring_enter(2)](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html)
  `IORING_ENTER_EXT_ARG` timed waits need a raw syscall: std's wrapper fixes the size argument.
  COOP/DEFER_TASKRUN hide completions from a spinning loop until it enters the kernel;
  neither beat plain flags on loopback echo (checked 2026-10-31)
- **Direct I/O and allocation:** [Linux O_DIRECT](https://man7.org/linux/man-pages/man2/creat.2.html)
  and [F_SETFL](https://man7.org/linux/man-pages/man2/F_GETFL.2const.html):
  per-filesystem alignment and flag setup;
  [fallocate](https://man7.org/linux/man-pages/man2/fallocate.2.html): reserve/grow semantics;
  [Windows buffering](https://learn.microsoft.com/en-us/windows/win32/fileio/file-buffering):
  logical-sector lengths/offsets and physical-sector buffer alignment.
  [statx](https://man7.org/linux/man-pages/man2/statx.2.html) `STATX_DIOALIGN` and
  [FILE_STORAGE_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_storage_info)
  report them. Never submit misaligned direct memory, even in tests: on GitHub's Azure
  runners, Linux 7.0 hv_storvsc retried such a read forever in unkillable D state,
  while NVMe runners completed the same test (checked 2026-10-31).
- **Kernel copies:** [copy_file_range](https://man7.org/linux/man-pages/man2/copy_file_range.2.html),
  [sendfile](https://man7.org/linux/man-pages/man2/sendfile.2.html),
  [splice](https://man7.org/linux/man-pages/man2/splice.2.html): short progress, explicit offsets,
  unsupported-operation fallback; cross-filesystem copies use read/write to avoid Linux
  5.3–5.18 false successes. Splice NONBLOCK alone does not make sockets nonblocking.
  Recover pending pipe bytes before closing; zero-copy source data must stay unmodified
  until consumed by the receiver. sendfile/splice lack MSG_NOSIGNAL: block/consume only
  newly generated SIGPIPE on the worker, preserving its old mask and pending signal.
- **Registered buffers:** [io_uring_register](https://man7.org/linux/man-pages/man2/io_uring_register.2.html):
  memlock accounting, containment and teardown. `Loop.Options.buffers` borrows at
  most 64 regions until deinit; `Uring.prepare` selects fixed opcodes transparently.
- **Darwin AIO** (checked 2026-10-21):
  [Apple XNU kern_aio.c](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/kern/kern_aio.c),
  `SIGEV_KEVENT`, `filt_aioprocess`: completion errno/result in kevent64 `ext`,
  delivery reaps the request. Probe runtime support; do not infer it from headers.
  Submitted requests drain normally rather than relying on canceled-kevent delivery.
- **Darwin TCP** (checked 2026-10-24):
  [XNU tcp_input.c](https://github.com/apple-oss-distributions/xnu/blob/f6217f891ac0bb64f3d375211650a4c1ff8ca1ea/bsd/netinet/tcp_input.c):
  a bound, non-listening socket is `TCPS_CLOSED`; incoming packets are dropped without
  a reset. `tests/net.zig` keeps the port bound to avoid reassignment races and expects
  a Dial timeout on macOS versus connection refusal on Linux/Windows.
- **Windows file I/O** (checked 2026-10-21):
  [ReadFile](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-readfile),
  [NtReadFile](https://learn.microsoft.com/en-us/windows-hardware/drivers/ddi/ntifs/nf-ntifs-ntreadfile),
  [ReOpenFile](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-reopenfile),
  [GetQueuedCompletionStatus](https://learn.microsoft.com/en-us/windows/win32/api/ioapiset/nf-ioapiset-getqueuedcompletionstatus).
  Native success still awaits a packet. Worker transfers alone use a private
  event's low bit to suppress IOCP packets; APC-based std.Io.Threaded transfers
  cannot be used on associated handles. Windows Sync remains synchronous/worker-backed.
  Explicit offsets still update the file pointer on synchronous Windows handles; only
  overlapped handles preserve it. Transfer tests check positions before verification reads.
- **Linux NUMA** (checked 2026-10-23):
  [mbind](https://man7.org/linux/man-pages/man2/mbind.2.html),
  [set_mempolicy](https://man7.org/linux/man-pages/man2/set_mempolicy.2.html),
  [get_mempolicy](https://man7.org/linux/man-pages/man2/get_mempolicy.2.html),
  [move_pages](https://man7.org/linux/man-pages/man2/move_pages.2.html),
  [sched_setaffinity](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html),
  [pthread_create](https://man7.org/linux/man-pages/man3/pthread_create.3.html),
  [sysfs node ABI](https://www.kernel.org/doc/Documentation/ABI/stable/sysfs-devices-node):
  bind before first writes; policy is not migration or a future-fault success guarantee.
  Thread affinity/policy inheritance matters for pools; explicit requests never silently
  degrade. Topology is a snapshot; kernel-owned I/O memory is outside user-region guarantees.
  Linux [`get_nodes`](https://github.com/torvalds/linux/blob/v6.12/mm/mempolicy.c)
  imports `maxnode - 1` bits. Map regions inaccessible until mbind completes:
  [`populate_vma_page_range`](https://github.com/torvalds/linux/blob/v6.12/mm/gup.c)
  skips PROT_NONE, preventing MCL_FUTURE from allocating pages under the old policy.
- **NUMA preparation and io-wq** (checked 2026-10-23):
  [madvise](https://man7.org/linux/man-pages/man2/madvise.2.html):
  `MADV_POPULATE_WRITE` is 23, populates writable pages once, can fail partially and
  does not prevent reclaim/OOM; `MADV_HUGEPAGE` only advises. Align to the host's
  `hpage_pmd_size` for THP experiments; inspect actual mappings.
  [io_uring_register](https://man7.org/linux/man-pages/man2/io_uring_register.2.html):
  `IORING_REGISTER_IOWQ_AFF` takes the mask size in bytes; empty options issue no
  affinity syscall. Worker masks can be shared between a thread's rings.
  [scheduler affinity](https://github.com/torvalds/linux/blob/master/kernel/sched/core.c)
  and [io-wq worker creation](https://github.com/torvalds/linux/blob/master/io_uring/io-wq.c):
  inherited `user_cpus_ptr` can narrow overlapping masks in `set_cpus_allowed_ptr`.
  Disjoint worker/loop CPU sets were verified on Linux 7.2.6; inspect actual worker
  affinity rather than treating registration success as an effective-mask guarantee.

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
