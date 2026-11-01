import asyncio
import contextlib
import io
import json
import subprocess
import sys
import unittest
from unittest import mock

import coroutines as bench


class CoroutineTests(unittest.IsolatedAsyncioTestCase):
    async def test_workloads_match_across_runtimes(self):
        cases = [
            ["--workload", "handoff", "--tasks", "2", "--iterations", "1", "--warmup", "0"],
            ["--workload", "handoff", "--tasks", "17", "--iterations", "31", "--warmup", "3"],
            ["--workload", "pipeline", "--tasks", "3", "--capacity", "1", "--work", "0"],
            ["--workload", "pipeline", "--tasks", "7", "--capacity", "1", "--work", "13"],
            ["--workload", "pipeline", "--tasks", "5", "--capacity", "256", "--work", "31"],
            ["--workload", "parked", "--tasks", "257", "--active", "7", "--payload", "257"],
            ["--workload", "parked", "--tasks", "9", "--active", "9", "--payload", "0"],
            ["--workload", "parked", "--tasks", "10000", "--active", "1", "--payload", "64"],
        ]
        for args in cases:
            if args[1] == "pipeline":
                args += ["--iterations", "37", "--warmup", "3"]
            c = bench.options(args)
            for name in bench.COMMANDS:
                with self.subTest(args=args, implementation=name):
                    row = await bench.measure(name, c)
                    self.assertTrue(row["validated"])
                    self.assertEqual(row["completed"], c.tasks)
                    self.assertGreater(row["rss_ready_bytes"], 0)
                    self.assertGreaterEqual(row["threads_at_ready"], 1)
                    self.assertEqual(row["cpu"], c.cpu)

    def test_checksum_oracle_uses_the_full_pipeline(self):
        c = bench.options(["--workload", "pipeline", "--tasks", "5", "--work", "7", "--iterations", "19"])
        values = []
        for i in range(c.iterations):
            value = i
            for _ in range(c.tasks - 2):
                for _ in range(c.work):
                    value = (value * 1664525 + 1013904223) & bench.MASK
            values.append(value)
        self.assertEqual(bench.expected(c), (19, sum(values) & bench.MASK))

    async def fake(self, script, c=None):
        c = c or bench.options(["--tasks", "2", "--iterations", "1", "--warmup", "0"])
        children = []
        spawn = asyncio.create_subprocess_exec

        async def track(*args, **kwargs):
            child = await spawn(*args, **kwargs)
            children.append(child)
            return child

        with mock.patch.dict(bench.COMMANDS, fake=[sys.executable, "-c", script]):
            with mock.patch.object(asyncio, "create_subprocess_exec", track):
                with self.assertRaises((RuntimeError, TimeoutError, json.JSONDecodeError)):
                    await bench.measure("fake", c)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)

    async def test_timeouts_kill_and_reap_child_in_each_phase(self):
        phases = ["", "print('{\"phase\":\"baseline\"}',flush=True);input();",
                  "print('{\"phase\":\"baseline\"}',flush=True);input();print('{\"phase\":\"ready\",\"tasks\":2}',flush=True);input();"]
        c = bench.options(["--tasks", "2", "--iterations", "1", "--warmup", "0", "--timeout", "0.15"])
        for prefix in phases:
            with self.subTest(prefix=prefix):
                await self.fake("import time;" + prefix + "time.sleep(30)", c)

    async def test_bad_results_and_nonzero_exit_are_rejected(self):
        row = dict(phase="result", operations=2, checksum=2, completed=2,
                   elapsed_ns=100, cpu_ns=50, setup_ns=1, drain_ns=0)
        cases = []
        for key, value in [("checksum", 1), ("operations", 3), ("completed", 1),
                           ("elapsed_ns", 0), ("cpu_ns", -1), ("cpu_ns", float("nan")),
                           ("checksum", True)]:
            bad = dict(row, **{key: value})
            cases.append((bad, ""))
        cases += [(row, ";sys.exit(2)"), (row, ";print('unexpected output')")]
        for bad, suffix in cases:
            with self.subTest(row=bad, suffix=suffix):
                script = ("import sys;print('{\"phase\":\"baseline\"}',flush=True);input();"
                          "print('{\"phase\":\"ready\",\"tasks\":2}',flush=True);input();"
                          f"print({json.dumps(json.dumps(bad))},flush=True)" + suffix)
                await self.fake(script)

    async def test_invalid_ready_protocol_is_rejected(self):
        await self.fake("print('not json',flush=True)")
        await self.fake("print('{\"phase\":\"wrong\"}',flush=True)")
        await self.fake("print('{\"phase\":\"baseline\"}',flush=True);input();print('{\"phase\":\"ready\",\"tasks\":99}',flush=True)")

    def test_invalid_options_are_rejected(self):
        for args in (["--tasks", "0"], ["--capacity", "0"], ["--timeout", "nan"],
                     ["--workload", "pipeline", "--tasks", "2"],
                     ["--workload", "parked", "--warmup", "1"],
                     ["--active", "99999"], ["--tasks", "100000", "--payload", "65536"],
                     ["--implementations", "go", "go"], ["--cpu", "-1"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    bench.options(args)

    def test_native_programs_reject_bad_sizes(self):
        for name, cmd in bench.COMMANDS.items():
            for args in (["handoff", "0", "1", "1", "0", "0", "1", "0"],
                         ["pipeline", "2", "1", "1", "0", "0", "1", "0"],
                         ["handoff", "2", "1", "0", "0", "0", "1", "0"],
                         ["parked", "2", "2", "1", "0", "0", "1", "0"]):
                with self.subTest(implementation=name, args=args):
                    proc = subprocess.run(cmd + args, input="", text=True, capture_output=True, timeout=5)
                    self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    bench.build(list(bench.COMMANDS))
    unittest.main()
