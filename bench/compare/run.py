#!/usr/bin/env python3
import argparse
import asyncio
from array import array
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import shlex
import socket
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
COMMANDS = {
    "velo-epoll": [str(ROOT / "zig-out/bin/echo-velo"), "{connections}", "epoll"],
    "velo-uring": [str(ROOT / "zig-out/bin/echo-velo"), "{connections}", "uring"],
    "libuv": [str(ROOT / "zig-out/bin/echo-libuv"), "{connections}"],
    "tokio": [str(ROOT / "zig-out/compare/release/echo-tokio"), "{connections}"],
}


def capture(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def build():
    commands = [
        ["zig", "build", "bench", "-Doptimize=ReleaseFast"],
        ["cc", "-O3", "-DNDEBUG", "-Wall", "-Wextra", "-Werror",
         "bench/compare/libuv.c", "-o", "zig-out/bin/echo-libuv",
         *shlex.split(capture("pkg-config", "--cflags", "--libs", "libuv"))],
        ["cargo", "build", "--locked", "--release", "--manifest-path",
         "bench/compare/tokio/Cargo.toml", "--target-dir", "zig-out/compare"],
    ]
    for command in commands:
        subprocess.run(command, cwd=ROOT, check=True, stdout=sys.stderr)
    return commands


def process_stats(pid):
    stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    status = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines())
    return {
        "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
        "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
        "peak_rss_bytes": int(status["VmHWM"].split()[0]) * 1024,
        "threads": int(status["Threads"]),
    }


async def start_server(command, timeout, cpu=None):
    process = await asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        cwd=ROOT,
    )
    try:
        if cpu is not None:
            os.sched_setaffinity(process.pid, {cpu})
        async with asyncio.timeout(timeout):
            line = await process.stdout.readline()
            if not line:
                raise RuntimeError("server exited before readiness: " + (await process.stderr.read()).decode())
            ready = json.loads(line)
            if not isinstance(ready["port"], int) or not 0 < ready["port"] < 65536:
                raise ValueError("invalid server port")
        return process, ready
    except BaseException:
        await stop_server(process)
        raise


async def stop_server(process):
    if process.returncode is None:
        try:
            process.kill()
        except ProcessLookupError:
            pass
    await process.communicate()


async def measure(name, config):
    command = [arg.replace("{connections}", str(config.connections)) for arg in COMMANDS[name]]
    process, ready = await start_server(command, config.timeout, config.server_cpu)
    peers = []
    try:
        async with asyncio.timeout(config.timeout):
            for _ in range(config.connections):
                reader, writer = await asyncio.open_connection("127.0.0.1", ready["port"])
                writer.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                peers.append((reader, writer))
            warmed = asyncio.Queue()
            start = asyncio.Event()

            async def client(index, reader, writer):
                payload = bytes((index + offset) % 251 for offset in range(config.size))
                samples = array("Q")

                async def exchange():
                    before = time.perf_counter_ns()
                    writer.write(payload)
                    await writer.drain()
                    reply = await reader.readexactly(len(payload))
                    elapsed = time.perf_counter_ns() - before
                    if reply != payload:
                        raise RuntimeError("incorrect echo payload")
                    return elapsed

                for _ in range(config.warmup):
                    await exchange()
                warmed.put_nowait(None)
                await start.wait()
                for _ in range(config.iterations):
                    samples.append(await exchange())
                return samples

            async with asyncio.TaskGroup() as group:
                tasks = [group.create_task(client(i, *peer)) for i, peer in enumerate(peers)]
                for _ in peers:
                    await warmed.get()
                before = process_stats(process.pid)
                server_affinity = sorted(os.sched_getaffinity(process.pid))
                client_cpu_start = time.process_time_ns()
                begin = time.perf_counter_ns()
                start.set()
                await asyncio.gather(*tasks)
                elapsed = time.perf_counter_ns() - begin
                client_cpu_ns = time.process_time_ns() - client_cpu_start
                after = process_stats(process.pid)

            # Peers stay open through the last sample and resource snapshot.
            drain_begin = time.perf_counter_ns()
            for _, writer in peers:
                writer.write_eof()
            for reader, _ in peers:
                if await reader.read(1):
                    raise RuntimeError("unexpected bytes after final echo")
            for _, writer in peers:
                writer.close()
            for _, writer in peers:
                await writer.wait_closed()
            stdout, stderr = await process.communicate()
            drain_ns = time.perf_counter_ns() - drain_begin
            if process.returncode != 0 or stdout or stderr:
                raise RuntimeError(f"server failed ({process.returncode}): {stdout!r} {stderr!r}")

        samples = sorted(value for task in tasks for value in task.result())
        return {
            "type": "run", "implementation": name, "server": ready,
            "requests": len(samples), "elapsed_ns": elapsed,
            "requests_per_second": len(samples) * 1e9 / elapsed,
            "payload_bytes_per_second": len(samples) * config.size * 1e9 / elapsed,
            "p50_ns": samples[math.ceil(len(samples) * 0.50) - 1],
            "p99_ns": samples[math.ceil(len(samples) * 0.99) - 1],
            "client_cpu_seconds": client_cpu_ns / 1e9,
            "server_cpu_seconds": after["cpu_seconds"] - before["cpu_seconds"],
            "server_rss_warm_bytes": before["rss_bytes"],
            "server_peak_rss_bytes": after["peak_rss_bytes"],
            "server_threads": after["threads"], "server_affinity": server_affinity,
            "client_affinity": sorted(os.sched_getaffinity(0)),
            "drain_ns": drain_ns, "validated": True,
        }
    finally:
        for _, writer in peers:
            writer.transport.abort()
        await stop_server(process)


def options(argv=None):
    parser = argparse.ArgumentParser(description="Linux TCP echo comparison; writes JSON Lines to stdout.")
    parser.add_argument("--implementations", nargs="+", choices=COMMANDS,
                        default=["velo-epoll", "libuv", "tokio"])
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--iterations", type=int, default=1000, help="requests per connection per measured run")
    parser.add_argument("--warmup", type=int, default=100, help="untimed requests per connection")
    parser.add_argument("--size", type=int, default=64, help="request payload bytes")
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=60, help="seconds allowed for startup and for each whole run")
    parser.add_argument("--seed", type=int, default=1, help="shuffle order within each repetition")
    parser.add_argument("--server-cpu", type=int)
    parser.add_argument("--client-cpu", type=int)
    config = parser.parse_args(argv)
    for name, upper in [("connections", 10000), ("size", 1048576), ("iterations", 10000000),
                        ("warmup", 1000000), ("repetitions", 1000)]:
        if not 1 <= getattr(config, name) <= upper:
            parser.error(f"{name} must be in 1..{upper}")
    if config.connections * config.iterations > 10000000:
        parser.error("at most 10000000 latency samples per run")
    if not math.isfinite(config.timeout) or config.timeout <= 0:
        parser.error("timeout must be finite and positive")
    if len(config.implementations) != len(set(config.implementations)):
        parser.error("implementations must be unique")
    if sys.platform != "linux":
        parser.error("comparison runner currently requires Linux")
    for cpu in (config.server_cpu, config.client_cpu):
        if cpu is not None and cpu not in os.sched_getaffinity(0):
            parser.error(f"CPU {cpu} is outside the available affinity mask")
    return config


def emit(record):
    print(json.dumps(record), flush=True)


async def run(config):
    rng = random.Random(config.seed)
    results = {name: [] for name in config.implementations}
    for repetition in range(config.repetitions):
        order = list(config.implementations)
        rng.shuffle(order)
        for name in order:
            result = await measure(name, config)
            result["repetition"] = repetition + 1
            emit(result)
            results[name].append(result)
    for name, rows in results.items():
        rates = [row["requests_per_second"] for row in rows]
        emit({"type": "summary", "implementation": name, "runs": len(rows),
              "median_requests_per_second": statistics.median(rates),
              "min_requests_per_second": min(rates), "max_requests_per_second": max(rates),
              "median_run_p99_ns": statistics.median(row["p99_ns"] for row in rows)})


def main():
    config = options()
    commands = build()
    paths = capture("git", "ls-files", "--cached", "--others", "--exclude-standard").splitlines()
    emit({
        "type": "metadata", "schema": 1, "workload": "tcp-echo-closed-loop",
        "config": vars(config), "platform": platform.platform(),
        "git_commit": capture("git", "rev-parse", "HEAD"),
        "git_status": capture("git", "status", "--porcelain"),
        "source_sha256": {p: digest(ROOT / p) for p in sorted(set(paths)) if (ROOT / p).is_file()},
        "binary_sha256": {name: digest(COMMANDS[name][0]) for name in config.implementations},
        "versions": {"zig": capture("zig", "version"), "rustc": capture("rustc", "--version"),
                     "cargo": capture("cargo", "--version"), "cc": capture("cc", "--version"),
                     "libuv": capture("pkg-config", "--modversion", "libuv"), "python": sys.version},
        "build_commands": commands,
    })
    if config.client_cpu is not None:
        os.sched_setaffinity(0, {config.client_cpu})
    asyncio.run(run(config))


if __name__ == "__main__":
    try:
        main()
    except (Exception, KeyboardInterrupt) as error:
        print(f"benchmark failed: {error!r}", file=sys.stderr)
        sys.exit(1)
