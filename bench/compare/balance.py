#!/usr/bin/env python3
import argparse
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys

import run as common

ROOT = common.ROOT
BINARY = ROOT / "zig-out/bin/balance"
MASK = (1 << 64) - 1
CASES = (("balanced", "local"), ("skewed", "local"), ("skewed", "spread"))


def core_id(cpu):
    path = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
    return tuple((path / name).read_text().strip() for name in ("physical_package_id", "core_id"))


def physical_cpus():
    selected, seen = [], set()
    for cpu in sorted(os.sched_getaffinity(0)):
        key = core_id(cpu)
        if key not in seen:
            selected.append(cpu)
            seen.add(key)
    return selected


def options(argv=None):
    p = argparse.ArgumentParser(description="Velo shard imbalance; Linux JSON Lines benchmark")
    p.add_argument("--cores", type=int, nargs="+", default=[1, 2, 4])
    p.add_argument("--cpus", type=int, nargs="+")
    p.add_argument("--jobs", type=int, default=4096)
    p.add_argument("--rounds", type=int, default=262144)
    p.add_argument("--repetitions", type=int, default=5)
    p.add_argument("--seed", type=int, default=191)
    p.add_argument("--timeout", type=float, default=10)
    c = p.parse_args(argv)
    if sys.platform != "linux":
        p.error("requires Linux")
    c.cpus = physical_cpus() if c.cpus is None else c.cpus
    if (not c.cpus or len(set(c.cpus)) != len(c.cpus) or
            not set(c.cpus) <= os.sched_getaffinity(0)):
        p.error("CPUs must be distinct and allowed by the current affinity mask")
    if len({core_id(cpu) for cpu in c.cpus}) != len(c.cpus):
        p.error("choose one logical CPU per physical core")
    if not c.cores or len(set(c.cores)) != len(c.cores) or any(n < 1 or n > min(64, len(c.cpus)) for n in c.cores):
        p.error("core counts must be distinct and fit the selected physical CPUs")
    if not 1 <= c.jobs <= 100000 or not 1 <= c.rounds <= 1000000 or c.jobs * c.rounds > 2000000000:
        p.error("invalid workload size; at most two billion arithmetic rounds")
    if not 1 <= c.repetitions <= 100 or not math.isfinite(c.timeout) or c.timeout <= 0:
        p.error("invalid repetitions or timeout")
    return c


def build():
    subprocess.run(["zig", "build", "bench-balance", "-Doptimize=ReleaseFast"], cwd=ROOT, check=True, stdout=sys.stderr)


def expected(config):
    a, b, mul, add, steps = 1, 0, 1664525, 1013904223, config["rounds"]
    while steps:
        if steps & 1:
            a, b = a * mul & MASK, (b * mul + add) & MASK
        mul, add = mul * mul & MASK, add * (mul + 1) & MASK
        steps >>= 1
    loops, jobs = config["loops"], config["jobs"]
    owners = [0] * loops
    executors = [0] * loops
    checksums = [0] * loops
    remote = 0
    hot = (jobs * 9 + 9) // 10
    for i in range(jobs):
        owner = i % loops if config["distribution"] == "balanced" else (0 if i < hot or loops == 1 else 1 + (i - hot) % (loops - 1))
        executor = i % loops if config["policy"] == "spread" else owner
        owners[owner] += 1
        executors[executor] += 1
        checksums[owner] = (checksums[owner] + a * (i + 1) + b) & MASK
        remote += executor != owner
    return owners, executors, checksums, remote


def validate(row, config, cpus):
    if row.get("config") != config or not isinstance(row.get("shards"), list) or len(row["shards"]) != config["loops"]:
        raise RuntimeError("configuration or shard count mismatch")
    for key in ("elapsed_ns", "process_cpu_ns", "completed", "checksum", "remote_commits", "completion_p50_ns", "completion_p99_ns"):
        if type(row.get(key)) is not int or not 0 <= row[key] <= MASK:
            raise RuntimeError(f"invalid {key}")
    if not 0 < row["completion_p50_ns"] <= row["completion_p99_ns"] <= row["elapsed_ns"]:
        raise RuntimeError("invalid completion timing")
    owners, executors, checksums, remote = expected(config)
    if row["completed"] != config["jobs"] or row["checksum"] != sum(checksums) & MASK or row["remote_commits"] != remote:
        raise RuntimeError("incorrect result or remote commit count")
    for i, shard in enumerate(row["shards"]):
        for key in ("cpu", "cpu_ns", "computed", "committed", "checksum", "polls", "posts", "parks"):
            if type(shard.get(key)) is not int or not 0 <= shard[key] <= MASK:
                raise RuntimeError(f"invalid shard {key}")
        if (shard["cpu"] != sorted(cpus)[i] or shard["computed"] != executors[i] or
                shard["committed"] != owners[i] or shard["checksum"] != checksums[i] or
                shard["polls"] != executors[i] * ((config["rounds"] + 1023) // 1024)):
            raise RuntimeError("incorrect per-shard work, ownership, affinity or poll count")


def measure(c, loops, distribution, policy):
    cpus = c.cpus[:loops]
    config = dict(distribution=distribution, policy=policy, loops=loops, jobs=c.jobs, rounds=c.rounds)
    command = ["taskset", "-c", ",".join(map(str, cpus)), str(BINARY), distribution, policy, str(loops), str(c.jobs), str(c.rounds)]
    process = subprocess.run(command, cwd=ROOT, text=True, capture_output=True, timeout=c.timeout, check=True)
    if process.stderr:
        raise RuntimeError(f"unexpected stderr: {process.stderr}")
    row = json.loads(process.stdout)
    validate(row, config, cpus)
    row.update(type="run", validated=True, command=command,
               jobs_per_second=c.jobs * 1e9 / row["elapsed_ns"],
               cpu_ns_per_job=row["process_cpu_ns"] / c.jobs,
               active_cores=sum(s["cpu_ns"] for s in row["shards"]) / row["elapsed_ns"],
               short_sample=row["elapsed_ns"] < 10000000)
    return row


def run(c):
    rng = random.Random(c.seed)
    results = {(n, d, p): [] for n in c.cores for d, p in CASES}
    for repetition in range(c.repetitions):
        order = list(results)
        rng.shuffle(order)
        for case in order:
            row = measure(c, *case)
            row["repetition"] = repetition + 1
            common.emit(row)
            results[case].append(row)
    for (loops, distribution, policy), rows in results.items():
        summary = dict(type="summary", loops=loops, distribution=distribution, policy=policy,
                       runs=len(rows), short_samples=sum(r["short_sample"] for r in rows))
        for key in ("elapsed_ns", "jobs_per_second", "cpu_ns_per_job", "active_cores", "completion_p50_ns", "completion_p99_ns"):
            values = [r[key] for r in rows]
            summary[key] = dict(median=statistics.median(values), min=min(values), max=max(values))
        summary["loop_cpu_fraction_median"] = [statistics.median(r["shards"][i]["cpu_ns"] / r["elapsed_ns"] for r in rows) for i in range(loops)]
        common.emit(summary)


def main():
    c = options()
    build()
    sources = ["bench/balance.zig", "bench/compare/balance.py", "build.zig", "build.zig.zon", "flake.lock"]
    sources += sorted(str(p.relative_to(ROOT)) for p in (ROOT / "src").rglob("*.zig"))
    common.emit(dict(type="metadata", schema=1, suite="balance", config=vars(c),
                     platform=platform.platform(), cpu_model=common.capture("lscpu"), zig=common.capture("zig", "version"),
                     git_commit=common.capture("git", "rev-parse", "HEAD"), git_status=common.capture("git", "status", "--porcelain"),
                     binary_sha256=common.digest(BINARY), source_sha256={p: common.digest(ROOT / p) for p in sources},
                     backend="epoll", quantum=1024, warmup_rounds_per_loop=100000))
    run(c)


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print(f"shard balance benchmark failed: {error!r}", file=sys.stderr)
        sys.exit(1)
