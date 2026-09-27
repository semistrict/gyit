#!/usr/bin/env python3
"""Measure actual native Git tracked-file and untracked-directory scanning."""
import argparse
import json
import os
import re
from pathlib import Path
import statistics
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mount', type=Path, required=True)
    parser.add_argument('--checkout', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--label', default='native-status')
    parser.add_argument('--runs', type=int, default=5)
    parser.add_argument('--max-ratio', type=float, default=2)
    args = parser.parse_args()
    if args.runs < 2:
        parser.error('at least two runs required')
    env = dict(os.environ, GIT_OPTIONAL_LOCKS='0', GIT_PAGER='cat', LC_ALL='C')

    def git(root, *options):
        return subprocess.check_output(['git', '-C', str(root), *options], env=env)

    # Setup is not a timed scan. A fresh VM may need to import its full history.
    deadline = time.monotonic() + 120
    while not (args.mount / '.git' / 'HEAD').exists():
        notice = args.mount / 'NOTICE'
        if notice.exists():
            message = notice.read_text()
            if 'Setup failed:' in message:
                raise SystemExit(message)
        if time.monotonic() > deadline:
            raise SystemExit('repository did not become ready')
        time.sleep(0.1)
    sha = git(args.mount, 'rev-parse', 'HEAD').decode().strip()
    if sha != git(args.checkout, 'rev-parse', 'HEAD').decode().strip():
        raise SystemExit('revision mismatch')
    pairs = []
    command = ['-c', 'core.fsmonitor=false', '-c', 'core.untrackedCache=false',
               'status', '--porcelain=v1', '--untracked-files=all']
    for iteration in range(args.runs):
        order = [('checkout', args.checkout), ('mount', args.mount)]
        if iteration % 2:
            order.reverse()
        pair = {}
        for label, root in order:
            trace = args.output.with_name(f'{label}-status.trace')
            trace.unlink(missing_ok=True)
            started = time.perf_counter()
            completed = subprocess.run(['git', '-C', str(root), *command],
                                       env=dict(env, GIT_TRACE2_PERF=str(trace)),
                                       capture_output=True, timeout=90)
            elapsed = time.perf_counter() - started
            if completed.returncode or completed.stdout:
                raise SystemExit(f'{label} status failed or worktree is dirty: {completed.stdout!r} {completed.stderr!r}')
            pair[label] = {'seconds': elapsed, 'trace': trace.read_text()}
        pair['ratio'] = pair['mount']['seconds'] / pair['checkout']['seconds']
        pairs.append(pair)
        print(f"status iteration {iteration}: {pair['mount']['seconds']:.3f}s / {pair['checkout']['seconds']:.3f}s = {pair['ratio']:.2f}x", flush=True)
    tracked = None
    for root in [args.checkout, args.mount]:
        files = git(root, 'ls-files', '-v', '-z').split(b'\0')
        if any(f and not f.startswith(b'H ') for f in files):
            raise SystemExit('index suppresses tracked-file scanning')
        names = [f[2:] for f in files if f]
        if tracked is not None and names != tracked:
            raise SystemExit('tracked file lists differ')
        tracked = names

    for pair in pairs:
        for label in ('checkout', 'mount'):
            counts = [int(n) for n in re.findall(r'(?:preload|refresh)/sum_lstat:(\d+)', pair[label]['trace'])]
            pair[label]['lstat_count'] = sum(counts)
            if sum(counts) < len(tracked):
                raise SystemExit(f'{label} did not stat every tracked entry')
    first = pairs[0]['ratio']
    warm = statistics.median(p['mount']['seconds'] for p in pairs[1:]) / statistics.median(p['checkout']['seconds'] for p in pairs[1:])
    ok = first <= args.max_ratio and warm <= args.max_ratio
    result = {'revision': sha, 'tracked_files': len(tracked), 'label': args.label,
              'command': command, 'max_ratio': args.max_ratio, 'ok': ok,
              'modes': {'status': {'first_ratio': first, 'warm_ratio': warm, 'ok': ok, 'runs': pairs}}}
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    raise SystemExit(0 if ok else 1)


if __name__ == '__main__':
    main()
