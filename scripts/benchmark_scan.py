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
import subprocess
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


def find_scan(root):
    # Reject .git rather than quietly timing repository internals on one side.
    if (root / '.git').exists():
        raise ValueError('find benchmark requires a worktree without root .git')
    started = time.perf_counter()
    result = subprocess.run(['find', '.', '-name', 'DOES_NOT_EXIST'], cwd=root,
                            capture_output=True, check=True)
    elapsed = time.perf_counter() - started
    if result.stdout or result.stderr:
        raise ValueError('find benchmark expected no matches or diagnostics')
    return {"seconds": elapsed}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mount", type=Path, required=True)
    parser.add_argument("--checkout", type=Path, required=True)
    parser.add_argument("--local-checkout", type=Path,
                        help="Optional guest-local ordinary filesystem baseline (4x gate)")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--pause-between-runs", type=float, default=0, help="Seconds outside timed scans; use > metadata TTL to test expired caches")
    parser.add_argument("--mode", choices=["names", "stat", "both", "find"], default="both",
                        help="Use a separate fresh mount per mode for independent first-scan results")
    parser.add_argument("--max-ratio", type=float, default=4)
    parser.add_argument("--label", default="existing mount; OS caches not flushed")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.pause_between_runs < 0:
        parser.error("pause must be nonnegative")
    if args.runs < 2:
        parser.error("at least two runs are required")
    result = {"label": args.label, "max_ratio": args.max_ratio, "modes": {}, "ok": True,
              "scanner": "Python os.scandir; stat mode calls DirEntry.stat(follow_symlinks=False) for every entry",
              "excluded": ["root .git"], "follows_symlinks": False, "pause_between_runs_seconds": args.pause_between_runs}
    for name, with_stat in [("names", False), ("stat", True), ("find", False)]:
        if name == 'find' and args.mode != 'find':
            continue
        if args.mode not in ("both", name):
            continue
        pairs = []
        for iteration in range(args.runs):
            if iteration and args.pause_between_runs:
                time.sleep(args.pause_between_runs)
            # Alternate order after the first pass to reduce order effects.
            paths = [("checkout", args.checkout), ("mount", args.mount)]
            if args.local_checkout:
                paths.append(("local", args.local_checkout))
            if iteration % 2:
                paths.reverse()
            pair = {label: find_scan(path) if name == 'find' else scan(path, with_stat) for label, path in paths}
            if name != 'find' and len({p['digest'] for p in pair.values()}) != 1:
                raise SystemExit(f"{name}: traversal differs; refusing performance comparison")
            pair["ratio"] = pair["mount"]["seconds"] / pair["checkout"]["seconds"]
            pairs.append(pair)
        if name == 'find':
            # Verify full path/type/size equality only AFTER every timed run.
            verified = {label: scan(path, True) for label, path in paths}
            if len({v['digest'] for v in verified.values()}) != 1:
                raise SystemExit('find: traversal differs; refusing performance comparison')
            for pair in pairs:
                for label, value in verified.items():
                    pair[label].update({k: v for k, v in value.items() if k != 'seconds'})
        first = pairs[0]["ratio"]
        warm = statistics.median(p["mount"]["seconds"] for p in pairs[1:]) / statistics.median(p["checkout"]["seconds"] for p in pairs[1:])
        ok = first <= args.max_ratio and warm <= args.max_ratio
        local = None
        if args.local_checkout:
            local = {"first_ratio": pairs[0]['mount']['seconds'] / pairs[0]['local']['seconds'],
                     "warm_ratio": statistics.median(p['mount']['seconds'] for p in pairs[1:]) / statistics.median(p['local']['seconds'] for p in pairs[1:])}
            ok = ok or (local['first_ratio'] <= 4 and local['warm_ratio'] <= 4)
        result["ok"] &= ok
        result["modes"][name] = {"first_ratio": first, "warm_ratio": warm, "ok": ok, "runs": pairs,
                                 "preceded_by_names_scan": name == "stat" and args.mode == "both"}
        if local:
            result['modes'][name]['local'] = local
        if name == 'find':
            result['scanner'] = 'find . -name DOES_NOT_EXIST; full path/type/size verification after timing'
            result['excluded'] = []
        print(f"{name}: first {first:.2f}x; warm {warm:.2f}x; {pairs[0]['mount']['entries']} entries; {'PASS' if ok else 'FAIL'}", flush=True)
    encoded = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded)
    print(encoded)
    raise SystemExit(0 if result["ok"] else 1)


if __name__ == "__main__":
    main()
