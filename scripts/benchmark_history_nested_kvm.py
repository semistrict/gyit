#!/usr/bin/env python3
"""Measure a real guest history command against a minimal host Git fetch.

Requires an isolated, seeded depth-one GCS namespace. Restore its original HEAD
between runs; this script intentionally leaves all durable objects untouched.
The seed must contain the mounted snapshot but not its ancestor commits.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import subprocess
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server', required=True)
    p.add_argument('--store', required=True)
    p.add_argument('--initrd', required=True)
    p.add_argument('--kernel', required=True)
    p.add_argument('--checkout', required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--sha', required=True)
    args = p.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        p.error("--sha must be a full lowercase commit ID")
    args.output.mkdir(parents=True, exist_ok=False)
    base = args.output.resolve()
    children, logs = [], []

    def launch(name, command, **kw):
        log = (base / (name + '.log')).open('wb')
        logs.append(log)
        proc = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, start_new_session=True, **kw)
        children.append(proc)
        return proc

    def wait_for(path, needle, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            text = path.read_text(errors='replace') if path.exists() else ''
            if needle in text:
                return text
            if any(c.poll() is not None for c in children):
                raise RuntimeError('test process exited; inspect logs')
            time.sleep(.02)
        raise TimeoutError('guest did not reach ' + needle)

    try:
        launch('server', [args.server, '--store', args.store, '--socket', str(base / 'gyit.sock'),
                         '--state', str(base / 'state'), '--cache', str(base / 'cache')])
        launch('baseline', ['/usr/libexec/virtiofsd', '--socket-path', str(base / 'checkout.sock'),
                            '--shared-dir', args.checkout, '--cache=always', '--sandbox=none'])
        deadline = time.monotonic() + 20
        while not all((base / name).exists() for name in ('gyit.sock', 'checkout.sock')):
            if time.monotonic() > deadline:
                raise TimeoutError('server sockets')
            time.sleep(.02)
        qemu = launch('guest', ['qemu-system-x86_64', '-enable-kvm', '-cpu', 'host',
                               '-machine', 'q35,memory-backend=mem', '-m', '8G', '-smp', '4',
                               '-object', 'memory-backend-memfd,id=mem,size=8G,share=on',
                               '-kernel', args.kernel, '-initrd', args.initrd,
                               '-append', 'console=ttyS0 rdinit=/init panic=-1 quiet',
                               '-nographic', '-no-reboot', '-nic', 'none',
                               '-chardev', f'socket,id=gyit,path={base}/gyit.sock',
                               '-device', 'vhost-user-fs-pci,chardev=gyit,tag=gyit',
                               '-chardev', f'socket,id=checkout,path={base}/checkout.sock',
                               '-device', 'vhost-user-fs-pci,chardev=checkout,tag=checkout'], stdin=subprocess.PIPE)
        guest = base / 'guest.log'
        wait_for(guest, 'built-in shell')
        def send(command):
            qemu.stdin.write((command + '\n').encode())
            qemu.stdin.flush()
        send('cd /mnt/gyit/github.com/torvalds/linux@' + args.sha + '; ls README; echo SNAPSHOT_READY')
        wait_for(guest, '\nSNAPSHOT_READY\n', 30)
        native = base / 'native.git'
        start = time.monotonic()
        commands = [
            ['git', 'init', '--bare', '--quiet', str(native)],
            ['git', '-C', str(native), 'fetch', '--quiet', '--depth=10', '--filter=tree:0', '--no-tags',
             'https://github.com/torvalds/linux.git', args.sha],
            ['git', '-C', str(native), 'log', '--oneline', '-n', '10', 'FETCH_HEAD'],
        ]
        for command in commands:
            result = subprocess.run(command, capture_output=True, check=True, timeout=10)
        native_seconds = time.monotonic() - start
        expected = result.stdout
        (base / 'expected.txt').write_bytes(expected)
        expected_hash = hashlib.sha256(expected).hexdigest()
        guest_script = """import subprocess,time,json,hashlib
for label in ['cold','warm','warm2']:
 t=time.monotonic()
 p=subprocess.run(['gyit','log','--oneline','-n','10','--timeout','5s'],capture_output=True)
 print('LOG_RESULT '+json.dumps(dict(label=label,seconds=time.monotonic()-t,exit=p.returncode,sha256=hashlib.sha256(p.stdout).hexdigest(),lines=len(p.stdout.splitlines()))),flush=True)
 print(p.stdout.decode(),end='',flush=True)
 print(p.stderr.decode(),end='',flush=True)
for name,command in [('startup',['gyit']),('status',['gyit','status'])]:
 t=time.monotonic()
 p=subprocess.run(command,capture_output=True)
 print('PROBE '+json.dumps(dict(name=name,seconds=time.monotonic()-t,exit=p.returncode)),flush=True)
print('LOG_DONE',flush=True)
"""
        send('python3 -c ' + shlex.quote(guest_script))
        text = wait_for(guest, '\nLOG_DONE\n', 20).replace('\r', '')
        rows = [json.loads(line.removeprefix('LOG_RESULT ')) for line in text.splitlines() if line.startswith('LOG_RESULT {')]
        if len(rows) != 3:
            raise RuntimeError('missing complete guest measurements')
        warm_start = time.monotonic()
        for _ in range(5):
            subprocess.run(commands[-1], stdout=subprocess.DEVNULL, check=True, timeout=2)
        native_warm = (time.monotonic() - warm_start) / 5
        for row in rows:
            row['matches_git'] = row['sha256'] == expected_hash and row['exit'] == 0
            row['ratio'] = row['seconds'] / (native_seconds if row['label'] == 'cold' else native_warm)
        probes = [json.loads(line.removeprefix('PROBE ')) for line in text.splitlines() if line.startswith('PROBE {')]
        report = dict(probes=probes, native_seconds=native_seconds, native_warm=native_warm, native_commands=commands,
                      revision=args.sha, count=10, guest=rows, expected_sha256=expected_hash,
                      cold_definition='Mounted depth-one snapshot; ancestors missing; fresh guest, service, and decoded cache',
                      baseline='Fresh commit-only depth-ten Git fetch plus log on the same host',
                      store=args.store)
        (base / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2), flush=True)
        return 0 if all(r['matches_git'] for r in rows) and rows[0]['ratio'] <= 2 else 1
    finally:
        import signal
        for child in reversed(children):
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
        for log in logs:
            log.close()


if __name__ == '__main__':
    raise SystemExit(main())
