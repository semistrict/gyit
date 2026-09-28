#!/usr/bin/env python3
"""Linux-only QEMU/KVM scan benchmark: direct gyit vhost-user vs virtiofsd.

Run as root on a disposable nested-KVM host. Repository data stays in the
configured object store; only the decoded cache is local to the gyit server.
The checkout is a separate ordinary-directory performance/correctness baseline.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time


def run(*args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def initrd(base, kernel, destination, scratch, script, mode, label, guest_disk=False, read_only=True, pause=0):
    root = scratch / 'initrd'
    unpacked = scratch / 'unpacked'
    run('unmkinitramfs', base, unpacked, stdout=subprocess.DEVNULL)
    # Ubuntu initrds can contain early archives plus a compressed main archive.
    # Repack a single archive; appending raw cpio to zstd is not supported by
    # every guest kernel's decompressor.
    shutil.copytree(unpacked / 'main' if (unpacked / 'main').exists() else unpacked,
                    root, symlinks=True)
    def copy(path, target=None):
        path = Path(path)
        dest = root / str(target or path).lstrip('/')
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dest)
        shutil.copymode(path, dest)
    copy('/bin/busybox')
    copy('/usr/bin/kmod', '/usr/bin/insmod')
    copy('/usr/bin/find')
    copy(sys.executable, '/usr/bin/python3')
    pythonlib = Path('/usr/lib') / f'python{sys.version_info.major}.{sys.version_info.minor}'
    shutil.copytree(pythonlib, root / str(pythonlib).lstrip('/'), dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns('__pycache__'))
    for binary in [Path(sys.executable), Path('/usr/bin/kmod'), Path('/usr/bin/find'), *pythonlib.rglob('*.so')]:
        text = subprocess.check_output(['ldd', str(binary)], text=True)
        for path in re.findall(r'(/[\w/+.\-]+)', text):
            if Path(path).is_file():
                copy(path)
    copy(script, '/scan.py')
    modules = []
    release = kernel.name.removeprefix('vmlinuz-')
    for name in ('virtio_pci', 'virtiofs', 'virtio_blk', 'ext4'):
        dependencies = subprocess.check_output(['modprobe', '--show-depends', '-S', release, name], text=True)
        for line in dependencies.splitlines():
            if line.startswith('insmod '):
                path = line.split()[1]
                if path not in modules:
                    copy(path)
                    modules.append(path)
    load_modules = '\n'.join('/usr/bin/insmod ' + path + ' || exit 1' for path in modules)
    (root / 'bin/sh').unlink(missing_ok=True)
    (root / 'bin/sh').symlink_to('busybox')
    init = f'''#!/bin/sh
/bin/busybox --install -s /bin
export PATH=/sbin:/usr/sbin:/bin:/usr/bin
trap 'poweroff -f' EXIT
mkdir -p /proc /sys /dev /tmp
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mkdir -p /mnt/gyit /mnt/checkout /tmp
{load_modules}
mount -t virtiofs {'-o ro' if read_only else ''} gyit /mnt/gyit || exit 1
mount -t virtiofs -o ro checkout /mnt/checkout || exit 1
{'mkdir -p /mnt/local; mount -t ext4 -o ro /dev/vda /mnt/local || exit 1' if guest_disk else ''}
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
python3 /scan.py --mount /mnt/gyit/github.com/acme/large --checkout /mnt/checkout {'--local-checkout /mnt/local' if guest_disk else ''} --pause-between-runs {pause} --mode {mode} --label {label} --max-ratio 2 --output /tmp/result.json
test -s /tmp/result.json || exit 1
echo GYIT_RESULT_BEGIN
cat /tmp/result.json
echo GYIT_RESULT_END
cmp /mnt/gyit/github.com/acme/large/README.md /mnt/checkout/README.md || exit 1
echo GYIT_CONTENT_OK
sync
poweroff -f
'''
    (root / 'init').write_text(init)
    (root / 'init').chmod(0o755)
    with destination.open('wb') as out:
        listing = subprocess.check_output(['find', '.', '-print0'], cwd=root)
        subprocess.run(['cpio', '--null', '-o', '-H', 'newc', '--quiet'],
                       input=listing, stdout=out, cwd=root, check=True)


def stop(process, timeout=15):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--store', required=True)
    p.add_argument('--sha', required=True)
    p.add_argument('--server', type=Path, required=True)
    p.add_argument('--checkout', type=Path, required=True)
    p.add_argument('--cache', type=Path, required=True)
    p.add_argument('--kernel', type=Path, required=True)
    p.add_argument('--initrd', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--mode', choices=['names', 'stat', 'find'], required=True)
    p.add_argument('--guest-disk', action='store_true', help='Also compare a guest-local ext4 worktree')
    p.add_argument('--metadata-ttl', type=float, default=1,
                   help='Attribute TTL for the fixed snapshot; 1s matches the live freshness bound; 0 forces revalidation')
    p.add_argument('--pause-between-runs', type=float, default=0)
    p.add_argument('--empty-cache', action='store_true')
    p.add_argument('--timeout', type=int, default=300)
    args = p.parse_args()
    if args.pause_between_runs < 0:
        p.error('pause must be nonnegative')
    if args.metadata_ttl < 0:
        p.error('metadata TTL must be nonnegative')
    if os.geteuid() != 0 or not Path('/dev/kvm').exists():
        p.error('run as root on a Linux host with /dev/kvm')
    args.output.mkdir(parents=True, exist_ok=True)
    lock = (args.output / 'run.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    label = args.mode + ('-empty' if args.empty_cache else '-cached')
    processes, logs = [], []
    qemu = None
    with tempfile.TemporaryDirectory(prefix='gyit-kvm-') as directory:
        scratch = Path(directory)
        cache = scratch / 'cache' if args.empty_cache else args.cache.resolve()
        initially_empty = not cache.exists() or not any(cache.iterdir())
        archive = scratch / 'initrd.img'
        initrd(args.initrd, args.kernel, archive, scratch, Path(__file__).with_name('benchmark_scan.py'), args.mode, label, args.guest_disk, pause=args.pause_between_runs)
        disk_args = []
        if args.guest_disk:
            disk = scratch / 'checkout.ext4'
            with disk.open('wb') as f:
                f.truncate(1 << 30)
            run('mkfs.ext4', '-q', '-F', '-d', args.checkout.resolve(), disk)
            if not (args.checkout / 'lost+found').exists():
                run('debugfs', '-w', '-R', 'rmdir /lost+found', disk,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            disk_args = ['-drive', f'file={disk},format=raw,if=virtio,readonly=on']
        metrics = args.output / (label + '-requests.json')
        metrics.unlink(missing_ok=True)
        def launch(name, command):
            log = (args.output / (label + '-' + name + '.log')).open('wb')
            logs.append(log)
            process = subprocess.Popen([str(x) for x in command], stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            return process
        try:
            startup = time.monotonic()
            gyit = launch('server', [args.server.resolve(), '--prepared', '--store', args.store,
                          '--socket', scratch / 'gyit.sock', '--state', scratch / 'state',
                          '--cache', cache, '--metrics', metrics, '--sha', args.sha,
                          '--metadata-ttl', f'{args.metadata_ttl}s'])
            baseline = launch('virtiofsd', ['/usr/libexec/virtiofsd', '--socket-path', scratch / 'checkout.sock',
                              '--shared-dir', args.checkout.resolve(), '--cache=always', '--sandbox=none'])
            deadline = time.monotonic() + 90
            while not all((scratch / name).exists() for name in ('gyit.sock', 'checkout.sock')):
                if gyit.poll() is not None or baseline.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError('server startup failed; inspect server/virtiofsd logs')
                time.sleep(0.1)
            startup_seconds = time.monotonic() - startup
            qemu = launch('guest', ['qemu-system-x86_64', '-enable-kvm', '-cpu', 'host',
                          '-machine', 'q35,memory-backend=mem', '-m', '8G', '-smp', '4',
                          '-object', 'memory-backend-memfd,id=mem,size=8G,share=on',
                          '-kernel', args.kernel, '-initrd', archive,
                          '-append', 'console=ttyS0 rdinit=/init panic=-1 quiet',
                          '-nographic', '-no-reboot', '-nic', 'none',
                          '-chardev', f'socket,id=gyit,path={scratch}/gyit.sock',
                          '-device', 'vhost-user-fs-pci,chardev=gyit,tag=gyit',
                          '-chardev', f'socket,id=checkout,path={scratch}/checkout.sock',
                          '-device', 'vhost-user-fs-pci,chardev=checkout,tag=checkout', *disk_args])
            deadline = time.monotonic() + args.timeout
            guest_log = args.output / (label + '-guest.log')
            while qemu.poll() is None:
                # The experimental backend can stall QEMU during device reset
                # after guest poweroff. Completion comes from the guest checks;
                # always terminate this disposable, diskless VM in finally.
                if '\nGYIT_CONTENT_OK\n' in guest_log.read_text(errors='replace').replace('\r', ''):
                    break
                if 'reboot: Power down' in guest_log.read_text(errors='replace'):
                    raise RuntimeError('guest powered down before benchmark completed; inspect guest log')
                if time.monotonic() > deadline:
                    raise TimeoutError('guest benchmark exceeded timeout; inspect guest log')
                time.sleep(0.1)
            if qemu.returncode not in (None, 0):
                raise RuntimeError('QEMU failed; inspect guest log')
        finally:
            for process in reversed(processes):
                stop(process, timeout=2 if process is qemu else 15)
            for log in logs:
                log.close()
    text = (args.output / (label + '-guest.log')).read_text(errors='replace').replace('\r', '')
    if '\nGYIT_CONTENT_OK\n' not in text:
        raise RuntimeError('guest traversal or content check failed; inspect guest log')
    result = json.loads(text.split('\nGYIT_RESULT_BEGIN\n', 1)[1].split('\nGYIT_RESULT_END', 1)[0])
    result['transport'] = 'QEMU/KVM → Linux virtio-fs → Go-FUSE vhost-user → gyit'
    result['checkout_transport'] = 'QEMU/KVM → Linux virtio-fs → virtiofsd → ordinary checkout'
    if args.guest_disk:
        result['local_transport'] = 'QEMU/KVM → virtio-blk → ext4 checkout; new filesystem image; host page cache retained'
    result['store'] = args.store
    result['store_requests'] = json.loads(metrics.read_text())
    result['cache_state'] = ('empty' if args.empty_cache else 'persistent') + ' decoded cache; fresh guest/backend; host OS caches retained'
    result['cache_initially_empty'] = initially_empty
    result['server_startup_seconds'] = startup_seconds
    result['metadata_ttl_seconds'] = args.metadata_ttl
    result['baseline_cache_policy'] = 'virtiofsd --cache=always'
    result['guest_cpus'] = 4
    result['guest_ram_gib'] = 8
    (args.output / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    v = result['modes'][args.mode]
    first = v['runs'][0]
    print(f"{label}: {first['mount']['seconds']:.3f}s / {first['checkout']['seconds']:.3f}s = {v['first_ratio']:.2f}x; warm {v['warm_ratio']:.2f}x")
    print('object requests:', result['store_requests'])
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
