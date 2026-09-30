#!/usr/bin/env python3
"""Guest half of verify_nested_kvm_regressions.py. Runs inside the QEMU guest.

Each check prints one `GYIT_VERIFY <json>` line. The host exposes whether a
background history deepen fetch is running through --signal; the guest cannot
observe host processes itself.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time


def report(check, **fields):
    print('GYIT_VERIFY ' + json.dumps({'check': check, **fields}), flush=True)


def deepening(signal):
    try:
        return signal.read_text().strip() == '1'
    except OSError:
        return False


def wait_for_snapshot(repo, timeout):
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            names = os.listdir(repo)
        except OSError:
            names = []
        if 'Makefile' in names:
            report('snapshot', seconds=round(time.monotonic() - start, 3))
            return
        time.sleep(1)
    raise TimeoutError('snapshot did not appear')


READ_LIMIT = 10  # Seconds; a few blob fetches from GitHub take 1-3 s.


def reads_during_deepen(repo, signal, directories, timeout):
    """Foreground blob reads must not queue behind a deepen batch."""
    start = time.monotonic()
    while not deepening(signal):
        if time.monotonic() - start > timeout:
            report('deepen-reads', ok=False, error='no deepen batch observed')
            return
        time.sleep(0.05)
    operations = []
    for directory in directories:
        before = deepening(signal)
        t = time.monotonic()
        error = None
        read = 0
        try:
            path = repo / directory
            names = sorted(e.name for e in os.scandir(path) if e.is_file())[:3]
            for name in names:
                read += len((path / name).read_bytes())
        except OSError as e:
            error = str(e)
        operations.append({'directory': directory, 'seconds': round(time.monotonic() - t, 3),
                           'bytes': read, 'deepen_before': before, 'deepen_after': deepening(signal),
                           'error': error})
    # A read queued behind the batch would finish only when the batch ends.
    during = [o for o in operations if o['deepen_before']]
    slowest = max((o['seconds'] for o in during), default=None)
    report('deepen-reads', ok=bool(during) and slowest < READ_LIMIT and all(o['error'] is None for o in operations),
           operations=operations, started_during=len(during), max_seconds_started_during=slowest)


F_SETPIPE_SZ = 1031


def paused(cli, repo, args, count):
    """Start log commands whose output nobody reads, like paused pagers.

    Each gets a one-page pipe, so it blocks writing its first entries rather
    than waiting for history coverage.
    """
    procs = []
    for _ in range(count):
        read, write = os.pipe()
        fcntl.fcntl(read, F_SETPIPE_SZ, 4096)
        procs.append(subprocess.Popen([cli, 'log', *args], cwd=repo, stdout=write, stderr=subprocess.PIPE))
        os.close(write)
        procs[-1].stdout = os.fdopen(read, 'rb')
    time.sleep(5)
    return procs


def stop(procs):
    """Kill paused commands; return bytes each had queued and any errors.

    A full pipe (4096 bytes) shows the command was blocked writing.
    """
    for p in procs:
        p.kill()
    queued, failures = [], []
    for p in procs:
        queued.append(len(p.stdout.read()))
        p.stdout.close()
        err = p.stderr.read().decode(errors='replace').strip()
        p.stderr.close()
        p.wait()
        if err:
            failures.append(err[-300:])
    return queued, failures


def timed(command, repo, timeout):
    t = time.monotonic()
    try:
        r = subprocess.run(command, cwd=repo, capture_output=True, timeout=timeout)
        return {'rc': r.returncode, 'seconds': round(time.monotonic() - t, 3),
                'stdout_bytes': len(r.stdout), 'stderr': r.stderr.decode(errors='replace')[-300:]}
    except subprocess.TimeoutExpired:
        return {'rc': None, 'seconds': timeout, 'stderr': 'timed out'}


def status_with_paused_logs(cli, repo, count):
    """Paused log streams must not consume the request budget."""
    procs = paused(cli, repo, [], count)
    running = sum(p.poll() is None for p in procs)
    status = timed([cli, 'status'], repo, 30)
    queued, failures = stop(procs)
    report('status-with-paused-logs', ok=status['rc'] == 0 and running == count,
           paused=count, running=running, queued_bytes=queued, status=status, stream_errors=failures[:3])


def file_log_with_paused_file_logs(cli, repo, count, path):
    """Paused file-history streams must not hold the process-wide read views."""
    query = [cli, 'log', '-n', '3', '--', path]
    alone = timed(query, repo, 60)
    procs = paused(cli, repo, ['--', '.'], count)
    running = sum(p.poll() is None for p in procs)
    beside = timed(query, repo, 60)
    queued, failures = stop(procs)
    report('file-log-with-paused-file-logs', ok=alone['rc'] == 0 and beside['rc'] == 0 and running == count,
           paused=count, running=running, queued_bytes=queued, alone=alone, beside_paused=beside,
           stream_errors=failures[:3])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', type=Path, required=True)
    p.add_argument('--signal', type=Path, required=True)
    p.add_argument('--cli', default='/usr/bin/gyit')
    p.add_argument('--timeout', type=int, default=600)
    args = p.parse_args()
    try:
        wait_for_snapshot(args.repo, args.timeout)
        reads_during_deepen(args.repo, args.signal, [
            'kernel/sched', 'mm', 'fs/ext4', 'net/ipv4', 'drivers/net/ethernet/intel/e1000',
            'Documentation/admin-guide', 'crypto', 'lib', 'block', 'security/selinux',
            'sound/core', 'fs/btrfs', 'drivers/gpu/drm/i915', 'arch/x86/kernel', 'ipc',
        ], args.timeout)
        status_with_paused_logs(args.cli, args.repo, 20)
        file_log_with_paused_file_logs(args.cli, args.repo, 2, 'MAINTAINERS')
    except Exception as e:  # Report, then let the host collect the result.
        report('error', ok=False, error=repr(e))
    print('GYIT_VERIFY_DONE', flush=True)


if __name__ == '__main__':
    main()
