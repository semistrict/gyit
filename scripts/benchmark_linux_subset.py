#!/usr/bin/env python3
"""Compare imports on a bounded history of real Linux directory trees.

Borrows existing local Git objects; never fetches or changes the source repo.
The synthetic linear history is a focused benchmark, not full Linux history.
"""
import argparse
import csv
import hashlib
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--gat', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--path', default='drivers/net', help='directory to sample; empty selects repository root')
    parser.add_argument('--versions', type=int, default=64)
    parser.add_argument('--timeout', type=int, default=60)
    parser.add_argument('--baseline-workers', type=int, default=0, help='worker count for the baseline executable; 0 uses its default')
    parser.add_argument('--workers', type=int, default=0, help='worker count for the updated executable; 0 uses its default')
    parser.add_argument('--expect-same-payloads', action='store_true', help='require byte-identical blob packs in both imports')
    args = parser.parse_args()
    if args.workers < 0 or args.baseline_workers < 0:
        parser.error('worker counts cannot be negative')
    source, output = args.source.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, GIT_CONFIG_GLOBAL='/dev/null', GIT_CONFIG_NOSYSTEM='1',
               GIT_AUTHOR_NAME='Test', GIT_AUTHOR_EMAIL='test@example.test',
               GIT_COMMITTER_NAME='Test', GIT_COMMITTER_EMAIL='test@example.test',
               GIT_AUTHOR_DATE='1000000000 +0000', GIT_COMMITTER_DATE='1000000000 +0000')

    def git(repo, *argv):
        return subprocess.check_output(['git', '-C', str(repo), *argv], env=env, timeout=args.timeout).decode().strip()

    if not 1 <= args.versions <= 1024:
        raise ValueError('versions must be 1..1024')
    history_args = ['log', '--first-parent', f'-{args.versions}', '--format=%H', 'HEAD']
    if args.path:
        history_args += ['--', args.path]
    originals = git(source, *history_args).splitlines()
    if not originals:
        raise ValueError('no source revisions')
    (output / 'source.txt').write_text(f'source={source}\npath={args.path}\n' + '\n'.join(originals) + '\n')
    with tempfile.TemporaryDirectory(prefix='gat-linux-subset-') as tmp:
        root = Path(tmp)
        sample = root / 'source.git'
        subprocess.run(['git', 'init', '--bare', '-q', str(sample)], check=True, env=env)
        objects = git(source, 'rev-parse', '--path-format=absolute', '--git-path', 'objects')
        (sample / 'objects/info/alternates').write_text(objects + '\n')
        revisions = list(reversed(originals))
        queries = ''.join(original + ':' + args.path + '\n' for original in revisions)
        checked = subprocess.check_output(
            ['git', '-C', str(source), 'cat-file', '--batch-check=%(objectname) %(objecttype)'],
            input=queries.encode(), env=env, timeout=args.timeout).decode().splitlines()
        if len(checked) != len(revisions) or any(not row.endswith(' tree') for row in checked):
            raise ValueError('sample path must be a directory in every revision')
        commands = []
        for original, row in zip(revisions, checked):
            tree = row.split()[0]
            message = 'Linux directory sample ' + original + '\n'
            commands.append('commit refs/heads/main\n'
                            'committer Test <test@example.test> 1000000000 +0000\n'
                            f'data {len(message.encode())}\n{message}'
                            f'M 040000 {tree} ""\n\n')
        subprocess.run(['git', '-C', str(sample), 'fast-import', '--quiet'],
                       input=''.join(commands).encode(), check=True, env=env, timeout=args.timeout)
        parent = git(sample, 'rev-parse', 'refs/heads/main')
        git(sample, 'symbolic-ref', 'HEAD', 'refs/heads/main')
        count = int(git(sample, 'rev-list', '--objects', '--all', '--count'))
        if count > 250000:
            raise ValueError(f'sample has {count} objects; limit is 250000, reduce --versions')
        object_ids = git(sample, 'rev-list', '--objects', '--all', '--no-object-names') + '\n'
        sizes = subprocess.check_output(
            ['git', '-C', str(sample), 'cat-file', '--buffer', '--batch-check=%(objectsize)'],
            input=object_ids.encode(), env=env, timeout=args.timeout).splitlines()
        if len(sizes) != count:
            raise ValueError('sample object enumeration differs from count')
        raw_bytes = sum(int(size) for size in sizes)
        (output / 'sample.txt').write_text(f'head={parent}\nobjects={count}\nraw_bytes={raw_bytes}\n')
        if raw_bytes > 4 << 30:
            raise ValueError(f'sample expands to {raw_bytes} bytes; limit is 4 GiB, reduce --versions')
        print(f'sample: {count} objects, {raw_bytes} decoded bytes', flush=True)
        rows = []
        for label, executable in [('baseline', args.baseline), ('updated', args.gat)]:
            executable = executable.resolve()
            store = root / label
            scratch = root / (label + '-scratch')
            scratch.mkdir()
            command = [str(executable), 'import', '--repo', str(sample), '--store', str(store), '--temp-dir', str(scratch)]
            workers = args.baseline_workers if label == 'baseline' else args.workers
            if workers:
                command += ['--workers', str(workers)]
            peak = 0
            start = time.monotonic()
            with (output / (label + '.out')).open('wb') as out, (output / (label + '.err')).open('wb') as err:
                process = subprocess.Popen(command, stdout=out, stderr=err, env=env, start_new_session=True)
                try:
                    while process.poll() is None:
                        if time.monotonic() - start > args.timeout:
                            raise TimeoutError(f'{label} exceeded {args.timeout}s')
                        sizes = []
                        for path in scratch.rglob('*'):
                            try:
                                if path.is_file():
                                    sizes.append(path.stat().st_size)
                            except FileNotFoundError:
                                pass
                        peak = max(peak, sum(sizes))
                        time.sleep(0.01)
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
            elapsed = time.monotonic() - start
            if process.returncode:
                raise RuntimeError(f'{label} failed; see {output / (label + ".err")}')
            size = sum(p.stat().st_size for p in store.rglob('*') if p.is_file())
            payloads = []
            for pack in sorted(store.glob('packs/*/*')):
                if pack.is_file():
                    payloads.append(f'{pack.name}\t{pack.stat().st_size}\t{hashlib.sha256(pack.read_bytes()).hexdigest()}\n')
            (output / (label + '.payloads.tsv')).write_text(''.join(payloads))
            # Compare the full root listing across versions of the reader.
            listing = subprocess.check_output([str(executable), 'ls', '--store', str(store), '--sha', parent], env=env, timeout=args.timeout)
            (output / (label + '.ls')).write_bytes(listing)
            rows.append([label, f'{elapsed:.6f}', size, peak, hashlib.sha256(executable.read_bytes()).hexdigest(), 'SLOW >1s' if elapsed > 1 else ''])
            print(f'{label}: {elapsed:.3f}s, {size} store bytes, {peak} peak scratch bytes', flush=True)
        with (output / 'results.tsv').open('w') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['build', 'seconds', 'store_bytes', 'peak_scratch_bytes', 'binary_sha256', 'flag'])
            writer.writerows(rows)
        if (output / 'baseline.ls').read_bytes() != (output / 'updated.ls').read_bytes():
            raise RuntimeError('root listings differ')
        if args.expect_same_payloads and (output / 'baseline.payloads.tsv').read_bytes() != (output / 'updated.payloads.tsv').read_bytes():
            raise RuntimeError('blob pack payloads differ')


if __name__ == '__main__':
    main()
