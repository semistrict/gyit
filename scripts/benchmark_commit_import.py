#!/usr/bin/env python3
"""Compare bounded commit-heavy imports without cloning or reading Linux history."""

import argparse
import csv
import hashlib
import os
from pathlib import Path
import re
import subprocess
import tempfile

from benchmark_budget import run_capped


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--gat', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--commits', type=int, default=10000)
    args = parser.parse_args()
    if not 1 <= args.commits <= 100000:
        parser.error('--commits must be 1..100000')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, GIT_CONFIG_GLOBAL='/dev/null', GIT_CONFIG_NOSYSTEM='1')
    rows = []
    with tempfile.TemporaryDirectory(prefix='gat-commit-sample-') as tmp:
        root = Path(tmp)
        source = root / 'source.git'
        subprocess.run(['git', 'init', '--bare', '-q', str(source)], check=True, env=env, timeout=10)
        # Roughly the size of a Linux commit, without blobs or tree changes, so
        # this benchmark isolates graph/commit processing from file conversion.
        message = 'Commit processing fixture.\n' * 28
        commands = []
        for i in range(args.commits):
            commands.append('commit refs/heads/main\n'
                            f'committer Test <test@example.test> {1000000000+i} +0000\n'
                            f'data {len(message)}\n{message}\n')
        subprocess.run(['git', '-C', str(source), '-c', 'gc.auto=0', 'fast-import', '--quiet'],
                       input=''.join(commands).encode(), check=True, env=env, timeout=15)
        subprocess.run(['git', '-C', str(source), 'symbolic-ref', 'HEAD', 'refs/heads/main'],
                       check=True, env=env, timeout=10)
        sha = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], env=env, timeout=10).decode().strip()
        (output / 'source.txt').write_text(f'commits={args.commits}\nsha={sha}\n')
        for label, executable in [('baseline', args.baseline), ('updated', args.gat)]:
            executable = executable.resolve()
            storage = root / label
            with (output / f'{label}.out').open('wb') as out, (output / f'{label}.err').open('wb') as err:
                rc, elapsed, expired = run_capped(
                    [str(executable), 'import', '--repo', str(source), '--store', str(storage)],
                    timeout=30, stdout=out, stderr=err, env=env)
            if expired or rc:
                raise RuntimeError(f'{label} failed or exceeded its 30s hard limit; see {output}')
            match = re.search(r'\((\d+) new objects,', (output / f'{label}.out').read_text())
            if match is None or int(match[1]) != args.commits + 1:
                raise RuntimeError('import did not report every commit and the empty tree')
            listing = subprocess.check_output([str(executable), 'ls', '--store', str(storage), '--sha', sha], env=env, timeout=10)
            if listing:
                raise RuntimeError('empty-tree fixture returned directory entries')
            size = sum(p.stat().st_size for p in storage.rglob('*') if p.is_file())
            row = [label, args.commits, f'{elapsed:.6f}', size,
                   hashlib.sha256(executable.read_bytes()).hexdigest(), 'SLOW >1s' if elapsed > 1 else '']
            rows.append(row)
            print(f'{label}: {elapsed:.3f}s, {size} store bytes', flush=True)
    with (output / 'results.tsv').open('w') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['build', 'commits', 'seconds', 'store_bytes', 'binary_sha256', 'flag'])
        writer.writerows(rows)


if __name__ == '__main__':
    main()
