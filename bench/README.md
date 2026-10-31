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
