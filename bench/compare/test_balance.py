import contextlib
import copy
import io
import unittest

import balance as bench


class BalanceTests(unittest.TestCase):
    def test_all_placements_keep_the_same_work_and_owner_results(self):
        available = bench.physical_cpus()
        c = bench.options(["--cores", "1", "--jobs", "23", "--rounds", "1031"])
        checksum = None
        for loops in sorted({1, min(2, len(available))}):
            for distribution, policy in bench.CASES:
                with self.subTest(loops=loops, distribution=distribution, policy=policy):
                    row = bench.measure(c, loops, distribution, policy)
                    if checksum is None:
                        checksum = row["checksum"]
                    self.assertEqual(checksum, row["checksum"])
                    self.assertEqual(sum(s["computed"] for s in row["shards"]), c.jobs)
                    self.assertEqual(sum(s["committed"] for s in row["shards"]), c.jobs)

    def test_oracle_rejects_ownership_errors_even_when_totals_match(self):
        if len(bench.physical_cpus()) < 2:
            self.skipTest("two physical cores required")
        c = bench.options(["--cores", "2", "--jobs", "23", "--rounds", "1031"])
        row = bench.measure(c, 2, "skewed", "spread")
        for key in ("committed", "computed", "checksum"):
            with self.subTest(key=key):
                bad = copy.deepcopy(row)
                bad["shards"][0][key] -= 1
                bad["shards"][1][key] += 1
                with self.assertRaises(RuntimeError):
                    bench.validate(bad, row["config"], c.cpus[:2])

    def test_size_and_affinity_limits(self):
        cpu = str(bench.physical_cpus()[0])
        for args in (["--jobs", "0"], ["--rounds", "0"], ["--cores", "0"],
                     ["--jobs", "100000", "--rounds", "1000000"], ["--timeout", "nan"],
                     ["--cpus", cpu, cpu], ["--cores", "1", "1"]):
            with self.subTest(args=args), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    bench.options(args)


if __name__ == "__main__":
    bench.build()
    unittest.main()
