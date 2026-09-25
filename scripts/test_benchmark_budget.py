import csv
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

from benchmark_budget import import_budget, run_capped


class BudgetTests(unittest.TestCase):
    def test_full_import_requires_matching_baseline_and_viable_estimate(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "clone.tsv"
            with path.open("w") as f:
                writer = csv.writer(f, delimiter="\t")
                writer.writerow(["operation", "seconds", "sha", "hardlinked_pack_files"])
                writer.writerow(["git clone --no-hardlinks", "2.5", "abc", "0"])
            self.assertEqual(import_budget(path, "abc", 24), 25)
            self.assertEqual(import_budget(path, "abc", 25), 25)
            for estimate in [None, 0, -1, 25.001, float("nan"), float("inf")]:
                with self.assertRaises(ValueError):
                    import_budget(path, "abc", estimate)
            with self.assertRaises(ValueError):
                import_budget(path, "different", 9)

    def test_deadline_kills_sigterm_ignoring_import_and_descendants(self):
        with tempfile.TemporaryDirectory() as tmp:
            pidfile = Path(tmp) / "child.pid"
            code = """
import os, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
with open(sys.argv[1], 'w') as f:
    f.write(str(child.pid))
time.sleep(60)
"""
            rc, elapsed, expired = run_capped(
                [sys.executable, "-c", code, str(pidfile)], timeout=0.5,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.assertTrue(expired)
            self.assertEqual(rc, -signal.SIGKILL)
            self.assertLess(elapsed, 2)
            self.assertTrue(pidfile.exists(), "fixture did not start its child")
            pid = int(pidfile.read_text())
            status = subprocess.run(["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True)
            # A killed child can briefly remain a zombie awaiting OS reaping.
            if status.returncode == 0:
                self.assertTrue(status.stdout.strip().startswith("Z"), status.stdout)

    def test_successful_process_is_not_reported_as_timeout(self):
        rc, _, expired = run_capped([sys.executable, "-c", "pass"], timeout=2,
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertEqual(rc, 0)
        self.assertFalse(expired)


if __name__ == "__main__":
    unittest.main()
