"""Validated clone budgets and a process-group hard deadline for benchmarks."""

import csv
import math
import os
import signal
import subprocess
import threading
import time

# Full imports may run for at most 10x the measured clone, with no deadline grace.
IMPORT_CLONE_MULTIPLIER = 10


def import_budget(path, source_sha, estimate):
    with open(path, newline="") as source:
        rows = list(csv.DictReader(source, delimiter="\t"))
    if len(rows) != 1:
        raise ValueError("expected one measured local clone baseline")
    row = rows[0]
    if row["operation"] != "git clone --no-hardlinks" or row["hardlinked_pack_files"] != "0":
        raise ValueError("baseline must be a local clone with no hardlinks")
    if row["sha"] != source_sha:
        raise ValueError("clone baseline and import source have different HEADs")
    seconds = float(row["seconds"])
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("invalid clone duration")
    limit = IMPORT_CLONE_MULTIPLIER * seconds
    if estimate is None or not math.isfinite(estimate) or estimate <= 0 or estimate > limit:
        raise ValueError(f"full import requires a supported upper-bound estimate within {limit:.6f}s; run smaller experiments first")
    return limit


def run_capped(command, *, timeout, stdout, stderr, env=None, cwd=None):
    """Kill the entire owned process group at the wall-clock deadline.

    There is deliberately no graceful-shutdown extension to the deadline.
    Returns (exit code, elapsed seconds, deadline expired).
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    started = time.monotonic()
    child = subprocess.Popen(command, cwd=cwd, env=env, stdout=stdout,
                             stderr=stderr, start_new_session=True)
    expired = threading.Event()

    def kill():
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    def deadline():
        if child.poll() is None:
            expired.set()
            kill()

    timer = threading.Timer(max(0, started + timeout - time.monotonic()), deadline)
    timer.daemon = True
    timer.start()
    try:
        rc = child.wait()
    except BaseException:
        kill()
        child.wait()
        raise
    finally:
        timer.cancel()
        timer.join()
    return rc, time.monotonic() - started, expired.is_set()
