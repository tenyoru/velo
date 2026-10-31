# Benchmarks

The existing `scalability.zig` measures TCP/timer admission, ready work, expiration,
cancellation and drain. `numa.zig` measures Linux memory locality. Build both with
`zig build bench -Doptimize=ReleaseFast`; run without arguments for usage.
Space repeated 10000-connection `scalability` runs at least a minute apart: each leaves
sockets in TIME_WAIT, and exhausting ephemeral ports fails its setup with `Timeout`.

`crossloop.zig` measures cross-loop posts, as JSON Lines with `Loop.stats` totals:

```sh
crossloop auto ping-pong 100000 pinned
crossloop auto busy 200000 loops=4 work=2048
crossloop auto paced 100000 rate=100000 spin=20000
```

`ping-pong` always finds its target parked, the best case for `spin=`. `busy` has senders
outrun one receiver. `paced` sends at a fixed rate from a busy-waiting thread, so read
`receiver_cpu_ns_per_message` and the latency percentiles, not whole-process CPU.

## TCP echo comparison (Linux)

```sh
nix develop .#bench
just bench-check
just bench-compare > results.jsonl
just bench-compare --connections 128 --size 4096 --iterations 2000 --repetitions 7 > results-128.jsonl
```

The runner builds standalone velo, C/libuv and Rust/Tokio servers in release mode.
Each uses one event-loop thread, a 4096-byte buffer per connection, TCP_NODELAY,
and sequential read/echo operations. Tokio uses its current-thread runtime.
The default comparison forces velo's epoll backend; libuv and Tokio use epoll on
Linux. Include a separate forced io_uring run with:

```sh
just bench-compare --implementations velo-epoll velo-uring libuv tokio > backends.jsonl
```

Unavailable io_uring fails the run. It does not silently substitute epoll.
For CPU placement, pass `--server-cpu N --client-cpu M`, choosing CPUs in your
allowed affinity mask, preferably on separate physical cores. The results record
the effective masks. Default runs inherit the shell's affinity.

Each connection exchanges `--warmup` requests, then waits at a common start barrier.
The measured phase sends `--iterations` requests per connection with one request
in flight per connection. Every reply must match its payload. Connections remain
open until all measurements finish; the runner then half-closes them, checks EOF
and a successful server exit. Startup and the whole workload have time limits.
Failures exit nonzero and terminate/reap the child; they produce no successful
record for that run. Each repetition starts fresh processes in a seeded shuffled
order. Compilation, connection setup, warmup and shutdown are excluded from the
throughput interval; shutdown is reported separately.

JSON Lines output contains:

- Metadata: arguments, OS/kernel, Git revision/status, source and executable
  SHA-256 hashes, compiler/library versions and build commands.
- Individual runs: validated request count, requests/s, payload bytes/s (one
  direction), nearest-rank p50/p99 round-trip latency, client/server CPU seconds,
  warmed server RSS, server peak RSS, thread count, affinity and drain duration.
- Summaries: median/min/max requests/s and the median of each run's p99.

The load generator is a single Python asyncio process on loopback. Its scheduling,
payload checking and sample collection affect throughput and observed latency;
check client CPU use before interpreting a plateau as a server limit. Latency is
client-observed under this closed-loop workload, with no coordinated-omission
correction, and does not describe latency under a fixed external arrival rate.
Server CPU uses `/proc` ticks, so short runs have coarse resolution. Peak RSS is
sampled before shutdown and includes startup/warmup; it excludes kernel socket
and io_uring memory and is not a per-coroutine allocation measurement.

The benchmark Nix shell pins the C/Rust tools and libuv through `flake.lock`;
Tokio and its transitive dependencies are pinned in `compare/tokio/Cargo.lock`.
The first build needs network access to fetch Cargo packages. The ordinary velo
development shell and library dependencies do not require these tools.

Server sources are `echo.zig`, `compare/libuv.c`, and
`compare/tokio/src/main.rs`. `compare/test.py` checks fragmented transfers larger
than the server buffer, half-close, empty peers, measurement counts, corruption
detection and startup/workload timeout cleanup. This suite compares complete echo programs;
the existing timer/cancellation benchmark remains specific to velo.
