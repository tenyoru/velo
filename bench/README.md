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

## Coroutine comparison (64-bit Linux)

Velo, Tokio and Go run the same in-memory work, with no sockets or Python in
that work's hot path. Use the pinned tools in `nix develop .#bench`:

```sh
just bench-coroutines-check
just bench-coroutines --workload handoff --tasks 64 --iterations 100000 > handoff.jsonl
just bench-coroutines --workload pipeline --tasks 4 --capacity 16 --work 16 --iterations 100000 > pipeline.jsonl
just bench-coroutines --workload parked --tasks 10000 --active 16 --payload 64 > parked.jsonl
```

`--implementations velo tokio go` is the default; a subset builds only those
implementations. `--cpu N` selects one allowed logical CPU (default: the lowest
allowed). Affinity is applied **before exec**, inherited by runtime threads, and
checked at the ready barrier. Velo uses a bare `Executor`, Tokio a current-thread
runtime, Go `GOMAXPROCS=1`. Go can still create multiple OS threads. The runner
sets `GOGC=100`, clears `GODEBUG`/`GOMEMLIMIT`/`GOFLAGS`, and records these choices;
GC remains enabled and its CPU cost is included. It uses the Nix-provided Go
compiler with `GOTOOLCHAIN=local` rather than downloading a compiler.

| Workload | Contract | Primary measurements |
|---|---|---|
| `handoff` | `tasks` workers pass one u64 token around capacity-one channels for `iterations` laps; every receiver checks the token sequence. The final hop terminates without sending another token. | CPU ns/hop and hops/s; `operations = tasks * iterations` |
| `pipeline` | `tasks` includes one producer, intermediate workers and one sink; `tasks - 1` bounded channels. Each intermediate worker applies `work` wrapping-u64 multiply/add steps per item. The sink checks every value in order. | CPU ns/item and items/s; `operations = iterations` |
| `parked` | Each task waits on its own capacity-one channel with `payload` bytes of live data. After the RSS snapshot, wake `active` evenly spaced tasks one at a time and await their completion; then wake/drain the remainder. All payload checksums are checked. | Ready RSS and incremental RSS/task; setup/drain durations for context |

For `parked`, target `i` is `floor(i * tasks / active)`; keep `active` fixed when
varying the parked population. The payload is a shared slab with a disjoint,
initialized region per task, held live across suspension. `--payload 0` isolates
more of the runtime/primitive overhead; it is not a measurement of the frame
alone. Footprints include channels, completion handles, task storage, allocator
retention and stacks. Report both total ready RSS and the delta from baseline;
a delta can be negative after reclamation and small populations are dominated
by fixed costs. Kernel memory is excluded. No forced collection is performed.

A useful sweep is handoff tasks 2/64/1024, pipeline capacities 1/16/256 with
work 0/16/256, and parked tasks 1000/10000/100000 with a fixed active subset.
Use the same settings across implementations. Capacity zero is excluded because
Tokio's bounded channel does not support rendezvous. Channels carry u64 values;
these are idiomatic runtime workloads, not identical primitive implementations
or a count of context switches. A token dependency does not guarantee one
scheduler switch per hop. Tokio's cooperative budget and Go's scheduling remain
at their normal settings.

The native programs speak `baseline → ready → result` JSON Lines, with a newline
from the runner releasing each of the first two barriers. The performance cases
first run `--warmup` iterations (default 100) with disposable workers, then create
fresh workers outside the measured interval. The parked case uses a fresh process
without warmup (`iterations=1`, `warmup=0`). `setup_ns` includes allocation,
initialization and reaching the initial waits; it is separate from `elapsed_ns`.
Timed work includes activation, result checking and completion coordination.
`drain_ns` is only the parked case's remaining wake-and-complete work, not memory
reclamation or cancellation. Every process exits only after all workers finish.

The runner checks each result against an independent checksum/count oracle,
requires successful exit with no extra output, and kills/reaps children on timeout
or protocol failure. The default is five fresh processes per implementation in
seeded shuffled order. Metadata includes configuration, compiler versions,
Git state, source/binary hashes and build commands. Each run includes
baseline/ready/peak-at-ready RSS, thread count, and validation status; summaries
retain median/min/max values. Handoff and pipeline also report native wall/process
CPU time. CPU uses the process clock in Zig/Rust and microsecond-resolution
`getrusage` in Go. Samples shorter than 10 ms carry `short_sample: true`; increase
iterations for throughput/CPU claims. In output schema 2, parked runs and summaries
omit sparse-wake wall/CPU time, throughput, CPU per wake, and short-sample flags:
the native interval is too short to rank reliably. Sparse wakes still validate
liveness and checksums. No per-hop latency
percentiles are claimed: clocking every handoff would distort it. These results
exclude event-loop/backend costs and do not establish application/server speed.

Sources: `coroutines.zig`, `compare/coroutines.go`,
`compare/tokio/src/coroutines.rs`, runner `compare/coroutines.py` and integration
checks `compare/test_coroutines.py`. `just bench-coroutines-check` covers all
implementations, queue capacities, sparse/all wakes, checksums and runner failure
cleanup. The Zig test also runs under ordinary `just test`; foreign targets
compile it but skip the Linux-only execution test.

## Shard balance (64-bit Linux)

```sh
nix develop .#bench -c just bench-balance-check
nix develop .#bench -c just bench-balance --cores 1 2 4 --repetitions 5 > balance.jsonl
```

This Velo-only experiment measures how explicit placement affects CPU utilization.
It runs the same finite batch of independent computations with three placements:

- `balanced/local`: equal query ownership across loops, computing on each owner.
- `skewed/local`: 90% of queries belong to loop 0; computation stays on the owner.
- `skewed/spread`: the same skewed ownership, but computation is distributed round
  robin. Each result is posted back to its owner for the accumulator update.

Each query starts with a unique u64 and performs `--rounds` wrapping multiply/add
operations. It yields after every 1024 rounds. Only the owner updates its accumulator;
per-job results, exactly-once commits, executing threads, per-shard counts and sums
are checked. The Python runner also uses an independent affine-composition oracle.
This represents distributable computation followed by a short, order-independent
update. It does not model a computation that must hold exclusive access to mutable
shard state, nor an ordered transaction or expensive serialized commit.

`--jobs` defaults to 4096 and `--rounds` to 262144. All jobs are admitted as one batch;
the measured interval includes dispatch, cooperative polling, result posts and owner
updates. Construction and a 100,000-round computation warmup on every loop are outside
the interval. The event backend is forced epoll, with no disk/network I/O or idle spin.
The runner selects distinct physical cores and applies process affinity before exec;
each loop pins to one selected CPU and verifies its affinity. Override the available
selection with `--cpus 2 4 6 8`, using CPU IDs allowed on your machine.

JSON Lines retain raw runs, metadata and median/min/max summaries. Report throughput,
process CPU/query, each loop's CPU time and computed/committed counts, and
`active_cores` (sum of loop CPU time divided by elapsed time). Loop CPU clocks span
the pre-dispatch barrier through post-completion snapshots, so that last metric is
an approximation with small control overhead. Completion p50/p99 measure time from
batch release to an owner applying a result, including queueing; they are **not**
steady-state request latency under an external arrival rate. Samples shorter than
10 ms are flagged. Each repetition shuffles all core-count/placement combinations;
the default per-process timeout is ten seconds.

The spread case measures the benefit and cost of deliberate compute placement; it
does not add work stealing, rebalance running tasks, or automatically identify safe
work to move. Seastar, Tokio and Go are not part of this experiment. Sources:
`balance.zig`, `compare/balance.py`, `compare/test_balance.py`.
