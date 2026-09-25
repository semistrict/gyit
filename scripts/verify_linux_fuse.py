#!/usr/bin/env python3
"""Linux-only isolated two-mount verification; no Git or source-repo access."""
import argparse
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import stat
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DEFAULT_BINARY = ROOT / '.build/linux-correctness/bin/gat'
DEFAULT_STORE = ROOT / '.testdata/linux-store'


def digest(data):
    return hashlib.sha256(data).hexdigest()


def mounted(path):
    # This reads kernel state only and remains usable when FUSE requests stall.
    encoded = os.fsencode(path)
    for row in Path('/proc/self/mountinfo').read_bytes().splitlines():
        parts = row.split(b' ')
        if len(parts) > 4 and parts[4] == encoded:
            return True
    return False


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--oracle', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=DEFAULT_BINARY)
    parser.add_argument('--store', type=Path, default=DEFAULT_STORE)
    args = parser.parse_args()
    require(os.uname().sysname == 'Linux', 'mounted verification requires Linux FUSE')
    args.oracle, args.report, args.binary, args.store = (path.resolve() for path in (args.oracle, args.report, args.binary, args.store))
    require((args.store / 'HEAD').is_file(), 'a retained published store is required')
    require(args.binary.is_file() and os.access(args.binary, os.X_OK), 'an executable ordinary gat build is required')
    require(not args.report.exists(), 'report must be a new file')
    require(not args.report.is_relative_to(args.store), 'report must stay outside the published store')
    require(args.report.is_absolute() and args.report.parent.is_dir(), 'report parent must already exist')
    oracle = json.loads((args.oracle / 'oracle.json').read_text())
    require(oracle['ok'], 'oracle preparation did not complete')
    for name, expected in oracle['artifacts'].items():
        require(digest((args.oracle / name).read_bytes()) == expected, 'oracle artifact changed: ' + name)
    head_before = digest((args.store / 'HEAD').read_bytes())
    report = {'ok': False, 'operations': [], 'cleanup': {}, 'cache_mib_per_mount': 32,
              'store': str(args.store), 'binary_sha256': digest(args.binary.read_bytes()),
              'oracle_sha256': digest((args.oracle / 'oracle.json').read_bytes()),
              'head_sha256_before': head_before, 'operational_seconds': 180}
    require(shutil.which('fusermount3'), 'fusermount3 is required')
    owned = Path(tempfile.mkdtemp(prefix='gat-correctness-fuse-', dir='/tmp'))
    mounts = [owned / 'mount-one', owned / 'mount-two']
    sockets = [owned / 'one.sock', owned / 'two.sock']
    processes, logs, handles = [], [], []
    env = dict(os.environ, LC_ALL='C', TERM='dumb', GIT_PAGER='cat', PAGER='cat')
    for key in tuple(env):
        if key.startswith('GAT_') or key.startswith('GIT_'):
            env.pop(key, None)
    trap = owned / 'bin'
    trap.mkdir()
    (trap / 'git').write_text('#!/bin/sh\necho attempted >> "' + str(owned / 'git-attempts') + '"\nexit 77\n')
    (trap / 'git').chmod(0o700)
    env['PATH'] = str(trap) + os.pathsep + env.get('PATH', '')

    def timed(name, function, **fields):
        started = time.monotonic()
        row = {'operation': name, **fields, 'ok': False}
        try:
            value = function()
            row['ok'] = True
            return value
        except BaseException as error:
            row['error'] = f'{type(error).__name__}: {error}'
            raise
        finally:
            row['seconds'] = time.monotonic() - started
            row['over_1s'] = row['seconds'] > 1
            report['operations'].append(row)

    def command(arguments, cwd=None, expect_success=True, timeout=20):
        process = subprocess.Popen([str(args.binary), *arguments], cwd=cwd, env=env,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        try:
            out, err = process.communicate(timeout=timeout)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise
        require((process.returncode == 0) == expect_success,
                f'command {arguments} exit {process.returncode}: {err.decode(errors="replace")}')
        return out

    def revision(mount):
        return command(['rev-parse', 'HEAD'], cwd=mount / 'init').decode().strip()

    def check_file(mount, label, path):
        want = (args.oracle / label / path).read_bytes()
        got = (mount / path).read_bytes()
        require(got == want, f'{mount.name} {label} file differs: {path}')

    def check_directory(mount, label, path):
        description = oracle['entries'][label + ':' + path]
        rows = []
        for line in (args.oracle / description['tsv']).read_text().splitlines():
            name, mode, oid, size = line.split('\t')
            rows.append((bytes.fromhex(name), int(mode, 8), oid, int(size)))
        directory = mount / path
        names = sorted(os.fsencode(name) for name in os.listdir(directory))
        require(names == [r[0] for r in rows], f'{mount.name} {label} directory names differ: {path}')
        for name, mode, _, size in rows:
            got = os.lstat(os.fsencode(directory) + b'/' + name)
            if mode in (0o040000, 0o160000):
                expected_mode = stat.S_IFDIR | 0o555
            elif mode == 0o120000:
                expected_mode = stat.S_IFLNK | 0o777
            else:
                expected_mode = stat.S_IFREG | (mode & 0o555)
            require(got.st_mode == expected_mode, f'directory entry mode differs: {path}/{os.fsdecode(name)}')
            if stat.S_IFMT(expected_mode) != stat.S_IFDIR:
                require(got.st_size == size, f'directory entry size differs: {path}/{os.fsdecode(name)}')

    def check_mount(mount, label):
        require(revision(mount) == oracle['revisions'][label], 'mounted revision differs')
        for path in oracle['files']:
            timed('exact_file', lambda p=path: check_file(mount, label, p), mount=mount.name, revision=label, path=path)
        for path in oracle['directories']:
            timed('exact_directory_and_stats', lambda p=path: check_directory(mount, label, p), mount=mount.name, revision=label, path=path)

    def readonly(mount):
        for path, flags in [(mount / 'README', os.O_WRONLY),
                            (mount / '.correctness-write-must-fail', os.O_WRONLY | os.O_CREAT | os.O_EXCL)]:
            try:
                descriptor = os.open(path, flags, 0o600)
            except OSError as error:
                require(error.errno in (errno.EROFS, errno.EACCES, errno.EPERM), 'write failed for an unrelated reason')
            else:
                os.close(descriptor)
                raise AssertionError('read-only mount allowed writable open: ' + str(path))

    def timeout(*_):
        raise TimeoutError('isolated FUSE verification deadline exceeded')

    signal.signal(signal.SIGALRM, timeout)
    signal.signal(signal.SIGTERM, timeout)
    signal.signal(signal.SIGINT, timeout)
    signal.alarm(180)
    try:
        for mount, socket in zip(mounts, sockets):
            mount.mkdir()
            log_path = args.report.parent / (args.report.stem + '-' + mount.name + '.log')
            log = log_path.open('wb')
            logs.append(log)
            processes.append(subprocess.Popen([str(args.binary), 'mount', '--store', str(args.store),
                                               '--sha', oracle['revisions']['head'], '--cache-mib', '32',
                                               '--socket', str(socket), str(mount)], env=env,
                                              stdout=log, stderr=subprocess.STDOUT, start_new_session=True))
        def ready():
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                for p in processes:
                    require(p.poll() is None, 'mount process exited before readiness')
                if all(mounted(m) for m in mounts) and all(s.exists() for s in sockets):
                    return
                time.sleep(0.05)
            raise TimeoutError('two mounts did not become ready')
        timed('mount_two_instances', ready)
        check_mount(mounts[0], 'head')
        check_mount(mounts[1], 'head')
        timed('read_only_writes', lambda: [readonly(m) for m in mounts])
        paths = list(oracle['files']) + ['init', 'include/linux']
        inodes = {path: os.lstat(mounts[0] / path).st_ino for path in paths}
        old_handle = (mounts[0] / 'init/main.c').open('rb')
        handles.append(old_handle)
        head_body = (args.oracle / 'head/init/main.c').read_bytes()
        old_body = (args.oracle / 'old/init/main.c').read_bytes()
        require(old_handle.read(17) == head_body[:17], 'initial pinned handle differs')
        timed('switch_tag_from_nested_cwd', lambda: command(['switch', 'v4.4'], cwd=mounts[0] / 'init'))
        check_mount(mounts[0], 'old')
        require(inodes == {p: os.lstat(mounts[0] / p).st_ino for p in paths}, 'same-path inode changed after switch')
        old_handle.seek(0)
        require(old_handle.read() == head_body, 'open handle lost its original generation')
        check_mount(mounts[1], 'head')
        historical = (mounts[0] / 'init/main.c').open('rb')
        handles.append(historical)
        timed('switch_previous_from_nested_cwd', lambda: command(['switch', '-'], cwd=mounts[0] / 'init'))
        check_mount(mounts[0], 'head')
        require(historical.read() == old_body, 'historical open handle changed after switch back')
        require(inodes == {p: os.lstat(mounts[0] / p).st_ino for p in paths}, 'inode changed on switch back')
        timed('reject_nonexistent_switch', lambda: command(['switch', 'missing-correctness-ref-4a096f'], cwd=mounts[0] / 'init', expect_success=False))
        require(revision(mounts[0]) == oracle['revisions']['head'], 'failed switch changed selected revision')
        check_file(mounts[0], 'head', 'init/main.c')
        require(revision(mounts[1]) == oracle['revisions']['head'], 'second mount changed generation')
        require(not (owned / 'git-attempts').exists(), 'mounted commands attempted source Git access')
        report['stable_inodes'], report['pinned_handles'], report['second_mount_pinned'] = True, True, True
        report['ok'] = True
    except BaseException as error:
        report['error'] = f'{type(error).__name__}: {error}'
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, timeout)
        signal.alarm(30)
        cleanup_errors = []
        def group_exists(process):
            try:
                os.killpg(process.pid, 0)
                return True
            except ProcessLookupError:
                return False
        try:
            for handle in handles:
                try:
                    handle.close()
                except BaseException as error:
                    cleanup_errors.append('handle: ' + str(error))
            for mount in reversed(mounts):
                if mounted(mount):
                    try:
                        subprocess.run(['fusermount3', '-u', str(mount)], capture_output=True, check=True, timeout=8)
                    except BaseException as error:
                        cleanup_errors.append('unmount: ' + str(error))
            for process in processes:
                try:
                    if group_exists(process):
                        os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=3)
                    if group_exists(process):
                        os.killpg(process.pid, signal.SIGKILL)
                    deadline = time.monotonic() + 2
                    while group_exists(process) and time.monotonic() < deadline:
                        time.sleep(0.05)
                    require(not group_exists(process), 'server process group remains')
                except BaseException as error:
                    cleanup_errors.append('process: ' + str(error))
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=2)
                    except ProcessLookupError:
                        pass
            for log in logs:
                log.close()
        except BaseException as error:
            cleanup_errors.append('cleanup deadline/error: ' + str(error))
            # A deadline must still produce evidence and stop our exact groups.
            for process in processes:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        finally:
            signal.alarm(0)
        remaining_mounts = [str(m) for m in mounts if mounted(m)]
        report['cleanup']['mounts_absent'] = not remaining_mounts
        report['cleanup']['remaining_mounts'] = remaining_mounts
        report['cleanup']['processes_exited'] = all(p.poll() is not None for p in processes)
        report['cleanup']['process_groups_absent'] = all(not group_exists(p) for p in processes)
        report['cleanup']['pids'] = [p.pid for p in processes]
        if not remaining_mounts and report['cleanup']['process_groups_absent']:
            try:
                # Remove only our mkdtemp root, after proving no owned live mount.
                shutil.rmtree(owned)
            except BaseException as error:
                cleanup_errors.append('remove owned directory: ' + str(error))
        report['cleanup']['sockets_absent'] = all(not s.exists() for s in sockets)
        report['cleanup']['temporary_root_absent'] = not owned.exists()
        report['cleanup']['errors'] = cleanup_errors
        try:
            report['head_sha256_after'] = digest((args.store / 'HEAD').read_bytes())
            report['head_unchanged'] = report['head_sha256_after'] == head_before
        except BaseException as error:
            report['head_unchanged'] = False
            cleanup_errors.append('HEAD inspection: ' + str(error))
        report['ok'] = (report['ok'] and not cleanup_errors and report['cleanup']['mounts_absent']
                        and report['cleanup']['processes_exited'] and report['cleanup']['process_groups_absent']
                        and report['cleanup']['sockets_absent'] and report['cleanup']['temporary_root_absent']
                        and report['head_unchanged'])
        args.report.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'ok': report['ok'], 'report': str(args.report), 'cleanup': report['cleanup']}))
    return 0 if report['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
