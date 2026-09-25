#!/usr/bin/env python3
"""Prepare small independent file/directory oracles; never mounts or imports."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SOURCE = ROOT / '.testdata/linux-repo.git'
FILES = ('README', 'Makefile', 'init/main.c')
DIRS = ('', 'init', 'include', 'include/linux', 'arch/x86/include/asm')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def git(source, *args):
    env = dict(os.environ, GIT_NO_REPLACE_OBJECTS='1', GIT_NO_LAZY_FETCH='1', LC_ALL='C')
    result = subprocess.run(['git', '-C', str(source), *args], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=20, check=True)
    if len(result.stdout) > 16 << 20:
        raise ValueError('small Git oracle exceeded 16 MiB')
    return result.stdout


def parse_directory(raw):
    result = []
    for row in raw.split(b'\0'):
        if not row:
            continue
        header, name = row.split(b'\t', 1)
        mode, kind, oid, size = header.split()
        if len(oid) != 40 or not name or b'/' in name:
            raise ValueError('invalid independent directory row')
        value = int(size) if kind == b'blob' else 0
        result.append((name.hex(), mode.decode(), oid.decode(), value))
    result.sort()
    if len({r[0] for r in result}) != len(result):
        raise ValueError('duplicate directory entry')
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--source', type=Path, default=SOURCE)
    args = parser.parse_args()
    output = args.out.resolve()
    source = args.source.resolve()
    if output == source or output.is_relative_to(source) or source.is_relative_to(output):
        raise ValueError('oracle output and source must be separate directories')
    output.mkdir(parents=True, exist_ok=False)
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError('oracle preparation exceeded 90 seconds')))
    signal.alarm(90)
    report = {'ok': False, 'scope': 'independent Git HEAD/v4.4 file and directory oracles',
              'operations': [], 'files': list(FILES), 'directories': list(DIRS),
              'preparer_sha256': sha(Path(__file__).read_bytes())}
    try:
        head = git(source, 'rev-parse', '--verify', 'HEAD').decode().strip()
        old = git(source, 'rev-parse', '--verify', 'v4.4^{commit}').decode().strip()
        report['revisions'] = {'head': head, 'old': old}
        report['entries'] = {}
        for label, revision in report['revisions'].items():
            version = output / label
            version.mkdir()
            for path in FILES:
                started = time.monotonic()
                body = git(source, 'cat-file', 'blob', revision + ':' + path)
                target = version / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(body)
                report['operations'].append({'operation': 'git_blob', 'revision': revision, 'path': path,
                                             'bytes': len(body), 'sha256': sha(body),
                                             'seconds': time.monotonic() - started,
                                             'over_1s': time.monotonic() - started > 1})
            for ordinal, path in enumerate(DIRS):
                started = time.monotonic()
                tree = revision + (':' + path if path else '^{tree}')
                rows = parse_directory(git(source, 'ls-tree', '-z', '-l', tree))
                filename = f'directory-{ordinal}.tsv'
                encoded = ''.join(f'{name}\t{mode}\t{oid}\t{size}\n' for name, mode, oid, size in rows).encode()
                (version / filename).write_bytes(encoded)
                report['entries'][label + ':' + path] = {'tsv': label + '/' + filename,
                                                         'count': len(rows), 'sha256': sha(encoded)}
                report['operations'].append({'operation': 'git_directory', 'revision': revision, 'path': path,
                                             'entries': len(rows), 'seconds': time.monotonic() - started,
                                             'over_1s': time.monotonic() - started > 1})
        if (output / 'head/init/main.c').read_bytes() == (output / 'old/init/main.c').read_bytes():
            raise ValueError('pinned file fixture needs differing historical contents')
        if git(source, 'rev-parse', '--verify', 'HEAD').decode().strip() != head or git(source, 'rev-parse', '--verify', 'v4.4^{commit}').decode().strip() != old:
            raise ValueError('source refs changed during preparation')
        report['artifacts'] = {str(path.relative_to(output)): sha(path.read_bytes())
                               for path in sorted(output.rglob('*')) if path.is_file()}
        report['ok'] = True
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        signal.alarm(0)
        (output / 'oracle.json').write_text(json.dumps(report, indent=2) + '\n')


if __name__ == '__main__':
    main()
