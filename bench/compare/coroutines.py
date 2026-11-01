#!/usr/bin/env python3
import argparse
import asyncio
import json
import math
import os
import platform
from pathlib import Path
import random
import statistics
import struct
import subprocess
import sys

import run as echo

ROOT = echo.ROOT
COMMANDS = {
    "velo": [str(ROOT / "zig-out/bin/coroutines")],
    "tokio": [str(ROOT / "zig-out/compare/release/coroutines-tokio")],
    "go": [str(ROOT / "zig-out/bin/coroutines-go")],
}
FIELDS = ("tasks", "iterations", "capacity", "work", "payload", "active", "warmup")
MASK = (1 << 64) - 1


def options(argv=None):
    p = argparse.ArgumentParser(description="Linux in-memory coroutine workloads; JSON Lines output")
    p.add_argument("--workload", choices=("handoff", "pipeline", "parked"), default="handoff")
    p.add_argument("--implementations", choices=COMMANDS, nargs="+", default=list(COMMANDS))
    for field in FIELDS:
        p.add_argument("--" + field, type=int)
    p.add_argument("--cpu", type=int, default=min(os.sched_getaffinity(0)) if sys.platform == "linux" else 0)
    p.add_argument("--repetitions", type=int, default=5)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--timeout", type=float, default=60)
    c = p.parse_args(argv)
    defaults = dict(tasks=64, iterations=10000, capacity=1, work=0, payload=0, active=1, warmup=100)
    if c.workload == "pipeline":
        defaults.update(tasks=4, capacity=16, work=16)
    elif c.workload == "parked":
        defaults.update(tasks=10000, iterations=1, payload=64, active=16, warmup=0)
    for field, value in defaults.items():
        if getattr(c, field) is None:
            setattr(c, field, value)
    limits = dict(tasks=(2, 100000), iterations=(1, 10000000), capacity=(1, 65536),
                  work=(0, 10000), payload=(0, 65536), active=(1, c.tasks), warmup=(0, 1000000),
                  repetitions=(1, 1000))
    for field, (low, high) in limits.items():
        if not low <= getattr(c, field) <= high:
            p.error(f"{field} must be in {low}..{high}")
    if c.tasks * c.payload > 256 * 1024 * 1024 or c.tasks * c.capacity > 10000000:
        p.error("payload or queue storage exceeds the workload limit")
    if c.tasks * max(c.iterations, c.warmup) * max(c.work, 1) > 100000000:
        p.error("at most 100000000 task-iterations times work")
    if c.workload == "pipeline" and c.tasks < 3:
        p.error("pipeline requires at least three tasks")
    if c.workload == "parked" and (c.iterations != 1 or c.warmup != 0):
        p.error("parked requires iterations=1 and warmup=0 for a fresh-process memory baseline")
    if not math.isfinite(c.timeout) or c.timeout <= 0:
        p.error("timeout must be finite and positive")
    if len(c.implementations) != len(set(c.implementations)):
        p.error("implementations must be unique")
    if sys.platform != "linux" or struct.calcsize("P") != 8 or c.cpu not in os.sched_getaffinity(0):
        p.error("requires 64-bit Linux and a CPU in the available affinity mask")
    return c


def environment():
    env = os.environ.copy()
    env.update(GOMAXPROCS="1", GOGC="100", GOTOOLCHAIN="local")
    for key in ("GODEBUG", "GOMEMLIMIT", "GOFLAGS"):
        env.pop(key, None)
    return env


def build(implementations):
    commands = {
        "velo": ["zig", "build", "bench-coroutines", "-Doptimize=ReleaseFast"],
        "tokio": ["cargo", "build", "--locked", "--release", "--manifest-path",
                  "bench/compare/tokio/Cargo.toml", "--target-dir", "zig-out/compare", "--bin", "coroutines-tokio"],
        "go": ["go", "build", "-trimpath", "-o", "zig-out/bin/coroutines-go", "bench/compare/coroutines.go"],
    }
    (ROOT / "zig-out/bin").mkdir(parents=True, exist_ok=True)
    for name in implementations:
        subprocess.run(commands[name], cwd=ROOT, check=True, stdout=sys.stderr, env=environment())
    return [commands[name] for name in implementations]


def expected(c):
    if c.workload == "handoff":
        return c.tasks * c.iterations, c.tasks * c.iterations
    if c.workload == "parked":
        return c.active, sum(((i * c.tasks // c.active) % 251) * c.payload for i in range(c.active)) & MASK
    a, b = 1, 0
    for _ in range((c.tasks - 2) * c.work):
        a, b = (a * 1664525) & MASK, (b * 1664525 + 1013904223) & MASK
    return c.iterations, (a * (c.iterations * (c.iterations - 1) // 2) + b * c.iterations) & MASK


async def record(process, phase):
    line = await process.stdout.readline()
    if not line:
        raise RuntimeError(f"child exited before {phase}: {(await process.stderr.read()).decode()}")
    row = json.loads(line)
    if not isinstance(row, dict) or row.get("phase") != phase:
        raise RuntimeError(f"expected {phase}, got {row!r}")
    return row


def validate(row, c):
    for key in ("operations", "checksum", "elapsed_ns", "cpu_ns", "setup_ns", "drain_ns", "completed"):
        if type(row.get(key)) is not int or not 0 <= row[key] <= MASK:
            raise RuntimeError(f"invalid {key}: {row.get(key)!r}")
    if row["elapsed_ns"] == 0 or row["completed"] != c.tasks or (row["operations"], row["checksum"]) != expected(c):
        raise RuntimeError(f"incorrect benchmark result: {row!r}")


async def measure(name, c):
    process = None
    try:
        async with asyncio.timeout(c.timeout):
            process = await asyncio.create_subprocess_exec(
                "taskset", "-c", str(c.cpu), *COMMANDS[name], c.workload,
                *(str(getattr(c, field)) for field in FIELDS),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, cwd=ROOT, env=environment(),
            )
            await record(process, "baseline")
            baseline = echo.process_stats(process.pid)
            process.stdin.write(b"\n")
            await process.stdin.drain()
            ready = await record(process, "ready")
            if ready.get("tasks") != c.tasks:
                raise RuntimeError("incorrect ready task count")
            parked = echo.process_stats(process.pid)
            masks = {tuple(sorted(os.sched_getaffinity(int(p.name)))) for p in Path(f"/proc/{process.pid}/task").iterdir()}
            if masks != {(c.cpu,)}:
                raise RuntimeError(f"child thread affinity mismatch: {masks}")
            process.stdin.write(b"\n")
            await process.stdin.drain()
            row = await record(process, "result")
            validate(row, c)
            out, err = await process.communicate()
            if process.returncode != 0 or out or err:
                raise RuntimeError(f"child failed ({process.returncode}): {out!r} {err!r}")
        delta = parked["rss_bytes"] - baseline["rss_bytes"]
        row.pop("phase")
        row.update(type="run", implementation=name, workload=c.workload, validated=True,
                   config={k: getattr(c, k) for k in FIELDS}, cpu=c.cpu,
                   rss_baseline_bytes=baseline["rss_bytes"], rss_ready_bytes=parked["rss_bytes"],
                   rss_delta_bytes=delta, rss_delta_bytes_per_task=delta / c.tasks,
                   peak_rss_at_ready_bytes=parked["peak_rss_bytes"], threads_at_ready=parked["threads"])
        if c.workload == "parked":
            # Sparse wakes validate liveness; their short interval cannot rank speed reliably.
            row.pop("elapsed_ns")
            row.pop("cpu_ns")
        else:
            row.update(operations_per_second=row["operations"] * 1e9 / row["elapsed_ns"],
                       cpu_ns_per_operation=row["cpu_ns"] / row["operations"],
                       short_sample=row["elapsed_ns"] < 10000000)
        return row
    finally:
        if process is not None:
            await echo.stop_server(process)


async def run(c):
    rng = random.Random(c.seed)
    results = {name: [] for name in c.implementations}
    for rep in range(c.repetitions):
        order = list(results)
        rng.shuffle(order)
        for name in order:
            row = await measure(name, c)
            row["repetition"] = rep + 1
            echo.emit(row)
            results[name].append(row)
    for name, rows in results.items():
        summary = {"type": "summary", "implementation": name, "workload": c.workload, "runs": len(rows)}
        metrics = ["rss_delta_bytes_per_task", "rss_ready_bytes", "setup_ns", "drain_ns"]
        if c.workload != "parked":
            summary["short_samples"] = sum(r["short_sample"] for r in rows)
            metrics += ["operations_per_second", "cpu_ns_per_operation"]
        for key in metrics:
            values = [r[key] for r in rows]
            summary[key] = dict(median=statistics.median(values), min=min(values), max=max(values))
        echo.emit(summary)


def main():
    c = options()
    commands = build(c.implementations)
    versions = {"python": sys.version}
    for name, cmd in {"velo": ("zig", "version"), "tokio": ("rustc", "--version"), "go": ("go", "version")}.items():
        if name in c.implementations:
            versions[name] = echo.capture(*cmd)
    paths = echo.capture("git", "ls-files", "--cached", "--others", "--exclude-standard").splitlines()
    echo.emit(dict(type="metadata", schema=2, suite="coroutines", config=vars(c),
                   platform=platform.platform(), versions=versions, build_commands=commands,
                   compiler_environment={key: os.environ.get(key) for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "GOAMD64", "GOARM64", "GOEXPERIMENT", "CGO_ENABLED")},
                   git_commit=echo.capture("git", "rev-parse", "HEAD"),
                   git_status=echo.capture("git", "status", "--porcelain"),
                   source_sha256={p: echo.digest(ROOT / p) for p in sorted(set(paths)) if (ROOT / p).is_file()},
                   binary_sha256={name: echo.digest(COMMANDS[name][0]) for name in c.implementations},
                   go_environment=dict(GOMAXPROCS="1", GOGC="100", GOTOOLCHAIN="local", unset=["GODEBUG", "GOMEMLIMIT", "GOFLAGS"])))
    asyncio.run(run(c))


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print(f"coroutine benchmark failed: {error!r}", file=sys.stderr)
        sys.exit(1)
