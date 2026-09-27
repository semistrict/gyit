#!/usr/bin/env python3
"""Boot a fresh macOS 27 custom-Virtio VM and compare complete worktree scans.

The checkout baseline uses either our VZ adapter or Apple's directory sharing.
It reads ordinary host files; gyit reads its durable store through our adapter.
No FSKit, host filesystem mount, vhost-user daemon, or lnx is involved.
"""
import argparse
import base64
import fcntl
import json
import os
from pathlib import Path
import selectors
import shlex
import subprocess
import time
import tempfile

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.build/virtiofs'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', default='direct')
    parser.add_argument('--baseline', choices=['custom', 'apple'], default='custom',
                        help='Checkout transport: our adapter or Apple Virtualization.framework directory sharing')
    parser.add_argument('--metadata-ttl', type=int, default=300,
                        help='Metadata lifetime for our adapter; Apple uses its own defaults')
    parser.add_argument('--timeout', type=int, default=180)
    parser.add_argument('--guest-disk', action='store_true')
    parser.add_argument('--empty-cache', action='store_true', help='Start with a new decoded cache, retaining durable data and host OS caches')
    parser.add_argument('--checkout', type=Path, default=ROOT/'.testdata/scan-checkout')
    parser.add_argument('--data', type=Path, default=ROOT/'.testdata/direct-data')
    parser.add_argument('--cache', type=Path, default=ROOT/'.testdata/direct-cache')
    parser.add_argument('--remotes', type=Path, default=ROOT/'.testdata/direct-remotes')
    parser.add_argument('--repository', default='langchain-ai/open-swe')
    parser.add_argument('--status', action='store_true', help='Measure native git status instead of a Python traversal')
    parser.add_argument('--scan-mode', choices=['names', 'stat', 'both'], default='both',
                        help='Non-Git scanner mode; use separate boots for independent first scans')
    args = parser.parse_args()
    if len(args.repository.split('/')) != 2 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_/.' for c in args.repository) or any(p in ('','.', '..') for p in args.repository.split('/')):
        parser.error('repository must be owner/repo')
    guest_repo = '/mnt/gyit/github.com/' + args.repository

    if not args.label or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in args.label):
        parser.error('label must contain only letters, numbers, hyphens and underscores')
    if args.metadata_ttl < 0 or args.metadata_ttl > 86400:
        parser.error('metadata TTL must be 0..86400 seconds')
    paths = [OUT/'gyit-vm', OUT/'Image', OUT/'initrd', OUT/'disk.raw', args.checkout.resolve()]
    for path in paths:
        if not path.exists():
            parser.error(f'missing fixture: {path}; see SCAN_PERFORMANCE.md')
    # A writable guest disk must never be opened by two VMs at once.
    lock = (OUT/'run.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('another direct Virtio benchmark is using this fixture')
    empty_cache = tempfile.TemporaryDirectory(prefix='empty-cache-', dir=OUT) if args.empty_cache else None
    cache = empty_cache.name if empty_cache else str(args.cache.resolve())
    command = [str(p) for p in paths] + [str(args.data.resolve()), cache,
                                        args.remotes.resolve().as_uri(), str(args.metadata_ttl), args.baseline]
    # Boot a private disk copy prepared from a stopped fixture VM. Each run gets
    # fresh guest/kernel/backend state; persistent decoded and OS caches remain.
    process = subprocess.Popen(command, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    selector = selectors.DefaultSelector(); selector.register(process.stdout, selectors.EVENT_READ)
    transcript = bytearray(); sent = False
    deadline = time.monotonic() + args.timeout
    encoded = base64.b64encode((ROOT/('scripts/benchmark_status.py' if args.status else 'scripts/benchmark_scan.py')).read_bytes()).decode()
    chunks = '\n'.join(encoded[i:i+512] for i in range(0,len(encoded),512))
    guest = """stty -echo
PS1= PS2=
set -e
trap 'sync; poweroff -ff' EXIT
rm -f /tmp/shared.json /tmp/local.json /tmp/checkout-status /tmp/gyit-status
export HOME=/root GIT_CONFIG_GLOBAL=/tmp/gyit-gitconfig TERM=dumb GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.directory GIT_CONFIG_VALUE_0='*' GIT_PAGER=cat
git config --global --add safe.directory '*'
mountpoint -q /proc || mount -t proc proc /proc
mountpoint -q /sys || mount -t sysfs sysfs /sys
modprobe virtiofs
mkdir -p /mnt/gyit /mnt/checkout
mount -t virtiofs -o ro gyit /mnt/gyit
mount -t virtiofs -o ro checkout /mnt/checkout
base64 -d > /tmp/scan.py <<'GYIT_SCRIPT'
""" + chunks + "\nGYIT_SCRIPT\n"
    guest += '[ "$(findmnt -n -o FSTYPE -T /mnt/gyit)" = virtiofs ]\n[ "$(findmnt -n -o FSTYPE -T /mnt/checkout)" = virtiofs ]\n'

    # The minimal init shell has no clock-sync service. Repeated boots at the
    # Unix epoch make a reused guest index appear racy against its own files.
    guest += f'date -u -s @{int(time.time())} > /dev/null\n'

    # Trigger/wait for import outside the timed traversal without running Git.
    guest += """python3 - <<'GYIT_READY'
from pathlib import Path
import time
root = Path('/mnt/gyit/github.com/langchain-ai/open-swe')
deadline = time.monotonic() + 120
while not (root / '.git' / 'HEAD').exists():
    notice = root / 'NOTICE'
    if notice.exists() and 'Setup failed:' in notice.read_text():
        raise SystemExit(notice.read_text())
    if time.monotonic() > deadline:
        raise SystemExit('repository did not become ready')
    time.sleep(0.1)
GYIT_READY
"""

    guest += "python3 /tmp/scan.py --mount /mnt/gyit/github.com/langchain-ai/open-swe --checkout /mnt/checkout --output /tmp/shared.json --label " + shlex.quote(args.label) + " || [ $? -eq 1 ]\n"
    guest += "echo GYIT_SHARED_BEGIN\ncat /tmp/shared.json\necho GYIT_SHARED_END\n"
    if args.guest_disk:
        guest += "if [ ! -d /var/tmp/gyit-scan-checkout/.git ]; then git -c safe.directory='*' clone -q --no-hardlinks /mnt/checkout /var/tmp/gyit-scan-checkout; fi\n"
        # Settle a newly cloned worktree and write a normal stat cache. This
        # leaves every tracked entry subject to stat during each timed status.
        guest += "sleep 1\ngit -C /var/tmp/gyit-scan-checkout update-index --refresh --force-write-index\n"
        guest += "python3 /tmp/scan.py --mount /mnt/gyit/github.com/langchain-ai/open-swe --checkout /var/tmp/gyit-scan-checkout --output /tmp/local.json --label guest-local || [ $? -eq 1 ]\n"
        guest += "echo GYIT_LOCAL_BEGIN\ncat /tmp/local.json\necho GYIT_LOCAL_END\n"
    # Smoke checks happen after scans so they do not prewarm traversal.
    guest += "git -C /mnt/gyit/github.com/langchain-ai/open-swe log -1 --format=%H > /tmp/mounted-head\n"
    guest += "git -C /mnt/checkout log -1 --format=%H > /tmp/checkout-head\ntest -s /tmp/mounted-head\ncmp /tmp/mounted-head /tmp/checkout-head\ncmp /mnt/checkout/README.md /mnt/gyit/github.com/langchain-ai/open-swe/README.md\necho GYIT_READS_OK\n"
    guest += "echo GYIT_REVISION_BEGIN\ncat /tmp/mounted-head\necho GYIT_REVISION_END\n"
    guest += "git -C /mnt/checkout status --porcelain=v1 --untracked-files=all > /tmp/checkout-status\n"
    guest += "git -C /mnt/gyit/github.com/langchain-ai/open-swe status --porcelain=v1 --untracked-files=all > /tmp/gyit-status\ncat /tmp/checkout-status /tmp/gyit-status\ncmp /tmp/checkout-status /tmp/gyit-status\ntest ! -s /tmp/checkout-status\necho GYIT_STATUS_OK\n"
    guest = guest.replace('/mnt/gyit/github.com/langchain-ai/open-swe', guest_repo)
    if not args.status:
        guest = guest.replace('python3 /tmp/scan.py ', 'python3 /tmp/scan.py --mode ' + args.scan_mode + ' ')
    # Separate guest-local clones by source identity, so switching fixtures
    # cannot silently reuse an unrelated worktree.
    guest = guest.replace('/var/tmp/gyit-scan-checkout', '/var/tmp/gyit-scan-' + args.repository.replace('/', '-'))
    guest += "sync\npoweroff -ff\n"
    try:
        while process.poll() is None:
            if time.monotonic() > deadline:
                raise TimeoutError('direct Virtio benchmark exceeded timeout')
            for key, _ in selector.select(1):
                data=os.read(key.fileobj.fileno(),65536)
                transcript.extend(data)
            if not sent and b':/# ' in transcript:
                process.stdin.write(guest.encode()); process.stdin.flush(); sent=True
        transcript.extend(process.stdout.read())
    finally:
        selector.close()
        if process.poll() is None:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        (OUT/(args.label+'.console.log')).write_bytes(transcript)
        if empty_cache: empty_cache.cleanup()
    text=transcript.decode(errors='replace').replace('\r','')
    def extract(name):
        # Match marker lines, not the commands echoed by the initial shell.
        body=text.split('\nGYIT_'+name+'_BEGIN\n',1)[1].split('\nGYIT_'+name+'_END',1)[0]
        result=json.loads(body)
        result['transport']='VZCustomVirtioDevice → Go-FUSE → backend'
        result['metadata_ttl_seconds']=args.metadata_ttl
        result['checkout_transport']=('guest-local disk' if name=='LOCAL' else
            'Apple VZVirtioFileSystemDeviceConfiguration → host checkout' if args.baseline=='apple' else
            'VZCustomVirtioDevice → Go-FUSE loopback → host checkout')
        result['checkout_metadata_ttl_seconds']=None if name=='LOCAL' or args.baseline=='apple' else args.metadata_ttl
        result['revision']=text.split('\nGYIT_REVISION_BEGIN\n',1)[1].split('\nGYIT_REVISION_END',1)[0].strip()
        result['cache_state']='fresh guest and backend; '+('empty decoded cache at boot; ' if args.empty_cache else 'persistent decoded cache; ')+'host OS caches retained; import readiness checked before timing; revisions checked after timing'
        if name=='LOCAL':
            result['cache_state']+='; gyit already warmed by the shared comparison'
        destination=OUT/(args.label+'-'+name.lower()+'.json')
        destination.write_text(json.dumps(result,indent=2)+'\n')
        for mode,v in result['modes'].items():
            first=v['runs'][0]
            print(f"{name.lower()} ({args.baseline if name=='SHARED' else 'guest disk'}) {mode}: {first['mount']['seconds']*1000:.1f} ms / {first['checkout']['seconds']*1000:.1f} ms; first {v['first_ratio']:.2f}×, warm {v['warm_ratio']:.2f}×")
        return result
    try:
        primary=extract('SHARED')
        if args.guest_disk: extract('LOCAL')
        if '\nGYIT_READS_OK\n' not in text or '\nGYIT_STATUS_OK\n' not in text or process.returncode != 0:
            raise RuntimeError('guest correctness checks or shutdown failed')
    except (IndexError, ValueError, RuntimeError) as error:
        raise SystemExit(f'{error}; inspect {OUT/(args.label+".console.log")}')
    raise SystemExit(0 if primary['ok'] else 1)


if __name__ == '__main__': main()
