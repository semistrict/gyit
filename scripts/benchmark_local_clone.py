#!/usr/bin/env python3
"""Measure a local clone with checkout and no hardlinks, then remove the clone."""

import argparse
import csv
import os
from pathlib import Path
import subprocess
import tempfile

from benchmark_budget import IMPORT_CLONE_MULTIPLIER, run_capped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    report = output / "timings.tsv"
    if report.exists():
        raise RuntimeError("baseline already exists; use a distinct output directory for a new measurement")
    output.mkdir(parents=True, exist_ok=True)

    def git(repo, *arguments):
        return subprocess.check_output(["git", "-C", str(repo), *arguments], text=True, timeout=30).strip()

    source_sha = git(source, "rev-parse", "HEAD")
    objects = Path(git(source, "rev-parse", "--path-format=absolute", "--git-path", "objects"))
    with tempfile.TemporaryDirectory(prefix="gat-clone-baseline-", dir=source.parent) as tmp:
        destination = Path(tmp) / "clone"
        with (output / "clone.out").open("wb") as stdout, (output / "clone.err").open("wb") as stderr:
            rc, elapsed, expired = run_capped(
                ["git", "clone", "--no-hardlinks", str(source), str(destination)],
                timeout=300, stdout=stdout, stderr=stderr)
        if rc != 0 or expired:
            raise RuntimeError(f"local clone failed ({rc}); see clone.err")
        if git(destination, "rev-parse", "HEAD") != source_sha or git(source, "rev-parse", "HEAD") != source_sha:
            raise RuntimeError("source HEAD changed during clone")
        clone_objects = destination / ".git" / "objects"
        if (clone_objects / "info" / "alternates").exists():
            raise RuntimeError("clone unexpectedly borrows source objects")
        for original in objects.rglob("*"):
            copied = clone_objects / original.relative_to(objects)
            if original.is_file() and copied.is_file() and os.path.samestat(original.stat(), copied.stat()):
                raise RuntimeError("clone unexpectedly shares an object-file inode")
        seconds = f"{elapsed:.6f}"
        limit = IMPORT_CLONE_MULTIPLIER * float(seconds)
        with report.open("x", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["operation", "seconds", "import_limit_seconds", "sha", "hardlinked_pack_files"])
            writer.writerow(["git clone --no-hardlinks", seconds, f"{limit:.6f}", source_sha, "0"])
        print(f"clone={seconds}s import_limit={limit:.6f}s hardlinked_object_files=0")


if __name__ == "__main__":
    main()
