#!/usr/bin/env python3
"""Compare complete worktree traversal on a mount and a matching local checkout.

Run after remounting to measure a fresh mount; this does not flush OS caches.
No Git executable or repository metadata is needed. Only the root .git directory
is excluded on both sides. Symlinks are not followed; hidden files are included.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import time


def scan(root, with_stat):
    rows = []
    pending = [(str(root), "")]
    started = time.perf_counter()
    while pending:
        directory, prefix = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                if not prefix and entry.name == ".git":
                    continue
                relative = prefix + entry.name
                directory_entry = entry.is_dir(follow_symlinks=False)
                kind = "directory" if directory_entry else "link" if entry.is_symlink() else "file"
                size = None
                if with_stat:
                    info = entry.stat(follow_symlinks=False)
                    size = None if directory_entry else info.st_size
                rows.append((relative, kind, size))
                if directory_entry:
                    pending.append((entry.path, relative + "/"))
    elapsed = time.perf_counter() - started
    rows.sort()
    digest = hashlib.sha256(json.dumps(rows, ensure_ascii=True).encode()).hexdigest()
    return {"seconds": elapsed, "entries": len(rows), "directories": sum(row[1] == "directory" for row in rows), "digest": digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--mode", choices=["names", "stat", "both"], default="both",
                        help="Use a separate fresh mount per mode for independent first-scan results")
    parser.add_argument("--max-ratio", type=float, default=4)
    parser.add_argument("--label", default="existing mount; OS caches not flushed")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.runs < 2:
        parser.error("at least two runs are required")
    result = {"label": args.label, "max_ratio": args.max_ratio, "modes": {}, "ok": True,
              "scanner": "Python os.scandir; stat mode calls DirEntry.stat(follow_symlinks=False) for every entry",
              "excluded": ["root .git"], "follows_symlinks": False}
    for name, with_stat in [("names", False), ("stat", True)]:
        if args.mode not in ("both", name):
            continue
        pairs = []
        for iteration in range(args.runs):
            # Alternate order after the first pass to reduce order effects.
            paths = [("checkout", args.checkout), ("mount", args.mount)]
            if iteration % 2:
                paths.reverse()
            pair = {label: scan(path, with_stat) for label, path in paths}
            if pair["checkout"]["digest"] != pair["mount"]["digest"]:
                raise SystemExit(f"{name}: traversal differs; refusing performance comparison")
            pair["ratio"] = pair["mount"]["seconds"] / pair["checkout"]["seconds"]
            pairs.append(pair)
        first = pairs[0]["ratio"]
        warm = statistics.median(p["mount"]["seconds"] for p in pairs[1:]) / statistics.median(p["checkout"]["seconds"] for p in pairs[1:])
        ok = first <= args.max_ratio and warm <= args.max_ratio
        result["ok"] &= ok
        result["modes"][name] = {"first_ratio": first, "warm_ratio": warm, "ok": ok, "runs": pairs,
                                 "preceded_by_names_scan": name == "stat" and args.mode == "both"}
        print(f"{name}: first {first:.2f}x; warm {warm:.2f}x; {pairs[0]['mount']['entries']} entries; {'PASS' if ok else 'FAIL'}", flush=True)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded)
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
