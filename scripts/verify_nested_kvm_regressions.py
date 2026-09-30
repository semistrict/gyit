#!/usr/bin/env python3
"""Check responsiveness regressions end to end in a nested QEMU/KVM guest.

Run as root on a Linux host with /dev/kvm (see GCE_BENCHMARK.md). The gyit
vhost-user server imports a real public GitHub repository into --store; the
guest reaches it only through virtio-fs and the mount's control file. Checks:

  deepen-reads                    file reads while a history deepen batch runs
  status-with-paused-logs         `gyit status` while 20 log streams are paused
  file-log-with-paused-file-logs  a file log while two file-log streams are paused

A host thread watches the server's Git children and publishes whether a
`--deepen` fetch is running through a separate virtio-fs share. Use a fresh
--store prefix per run: the import is durable and is not removed here.

--interactive boots the same fresh guest into a shell on this terminal instead
of running the checks. `poweroff -f` in the guest ends the run and stops the
server; the console is also logged to guest.log.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

GUEST_SCRIPT = Path(__file__).with_name('verify_guest_regressions.py')


def run(*args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def build_initrd(base, kernel, destination, scratch, cli, repository, interactive):
    root = scratch / 'initrd'
    unpacked = scratch / 'unpacked'
    run('unmkinitramfs', base, unpacked, stdout=subprocess.DEVNULL)
    shutil.copytree(unpacked / 'main' if (unpacked / 'main').exists() else unpacked, root, symlinks=True)

    def copy(path, target=None):
        path = Path(path)
        dest = root / str(target or path).lstrip('/')
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, dest)
        shutil.copymode(path, dest)

    copy('/bin/busybox')
    copy('/usr/bin/kmod', '/usr/bin/insmod')
    copy(cli, '/usr/bin/gyit')
    copy(GUEST_SCRIPT, '/verify.py')
    copy(sys.executable, '/usr/bin/python3')
    tools = [Path('/usr/bin/kmod')]
    if interactive:
        copy('/usr/bin/less')
        copy('/usr/share/terminfo/l/linux')
        tools.append(Path('/usr/bin/less'))
    pythonlib = Path('/usr/lib') / f'python{sys.version_info.major}.{sys.version_info.minor}'
    shutil.copytree(pythonlib, root / str(pythonlib).lstrip('/'), dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns('__pycache__'))
    for binary in [Path(sys.executable), *tools, *pythonlib.rglob('*.so')]:
        text = subprocess.check_output(['ldd', str(binary)], text=True)
        for path in re.findall(r'(/[\w/+.\-]+)', text):
            if Path(path).is_file():
                copy(path)
    modules = []
    release = kernel.name.removeprefix('vmlinuz-')
    for name in ('virtio_pci', 'virtiofs'):
        dependencies = subprocess.check_output(['modprobe', '--show-depends', '-S', release, name], text=True)
        for line in dependencies.splitlines():
            if line.startswith('insmod ') and line.split()[1] not in modules:
                copy(line.split()[1])
                modules.append(line.split()[1])
    load_modules = '\n'.join('/usr/bin/insmod ' + path + ' || exit 1' for path in modules)
    (root / 'bin/sh').unlink(missing_ok=True)
    (root / 'bin/sh').symlink_to('busybox')
    if interactive:
        command = f'''export TERM=linux PS1='gyit-vm:\\w # '
for arg in $(cat /proc/cmdline); do
  case "$arg" in
    gyit.rows=*) rows=${{arg#*=}} ;;
    gyit.cols=*) cols=${{arg#*=}} ;;
  esac
done
stty rows "${{rows:-24}}" cols "${{cols:-80}}"
cd /mnt/gyit/github.com || exit 1
echo 'Fresh gyit mount. Try: cd {repository}; gyit log -n 5'
while true; do
  setsid cttyhack /bin/sh -i
done'''
    else:
        command = f'python3 /verify.py --repo /mnt/gyit/github.com/{repository} --signal /mnt/signal/deepen'
    # The control file needs a mount without the kernel ro option; the
    # filesystem itself still rejects repository mutation with EROFS.
    (root / 'init').write_text(f'''#!/bin/sh
/bin/busybox --install -s /bin
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
trap 'poweroff -f' EXIT
mkdir -p /proc /sys /dev /tmp /mnt/gyit /mnt/signal
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
{load_modules}
mount -t virtiofs gyit /mnt/gyit || exit 1
mount -t virtiofs -o ro signal /mnt/signal || exit 1
{command}
poweroff -f
''')
    (root / 'init').chmod(0o755)
    with destination.open('wb') as out:
        listing = subprocess.check_output(['find', '.', '-print0'], cwd=root)
        subprocess.run(['cpio', '--null', '-o', '-H', 'newc', '--quiet'], input=listing, stdout=out, cwd=root, check=True)


def watch_deepen(state, signal, log, stopped):
    """Publish whether the server has a `git fetch --deepen` running."""
    marker = str(state).encode()
    current = None
    while not stopped.is_set():
        active = False
        for pid in os.listdir('/proc'):
            if not pid.isdigit():
                continue
            try:
                cmdline = Path('/proc', pid, 'cmdline').read_bytes()
            except OSError:
                continue
            if b'--deepen=' in cmdline and marker in cmdline:
                active = True
                break
        if active != current:
            current = active
            tmp = signal.with_suffix('.tmp')
            tmp.write_text('1' if active else '0')
            os.replace(tmp, signal)
            log.write(f'{time.time():.3f} {int(active)}\n')
            log.flush()
        time.sleep(0.05)


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
    p.add_argument('--server', type=Path, required=True, help='Linux gyit-vhost build')
    p.add_argument('--cli', type=Path, required=True, help='Linux gyit CLI installed in the guest')
    p.add_argument('--store', required=True, help='fresh repository namespace, e.g. gs://BUCKET/PREFIX/github')
    p.add_argument('--repository', default='torvalds/linux')
    p.add_argument('--kernel', type=Path, required=True)
    p.add_argument('--initrd', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--timeout', type=int, default=1200)
    p.add_argument('--interactive', action='store_true', help='boot into a guest shell on this terminal')
    args = p.parse_args()
    if os.geteuid() != 0 or not Path('/dev/kvm').exists():
        p.error('run as root on a Linux host with /dev/kvm')
    args.output.mkdir(parents=True, exist_ok=False)
    out = args.output.resolve()
    state, share = out / 'state', out / 'signal'
    share.mkdir()
    (share / 'deepen').write_text('0')
    processes, logs = [], []
    stopped = threading.Event()

    def launch(name, command):
        log = (out / (name + '.log')).open('wb')
        logs.append(log)
        process = subprocess.Popen([str(x) for x in command], stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    with tempfile.TemporaryDirectory(prefix='gyit-verify-') as directory:
        scratch = Path(directory)
        archive = scratch / 'initrd.img'
        build_initrd(args.initrd, args.kernel, archive, scratch, args.cli, args.repository, args.interactive)
        watcher_log = (out / 'deepen.log').open('w')
        watcher = threading.Thread(target=watch_deepen, args=(state, share / 'deepen', watcher_log, stopped))
        try:
            launch('server', [args.server.resolve(), '--store', args.store, '--socket', scratch / 'gyit.sock',
                              '--state', state, '--cache', out / 'cache'])
            launch('signal', ['/usr/libexec/virtiofsd', '--socket-path', scratch / 'signal.sock',
                              '--shared-dir', share, '--cache=never', '--sandbox=none'])
            deadline = time.monotonic() + 60
            while not all((scratch / n).exists() for n in ('gyit.sock', 'signal.sock')):
                if any(c.poll() is not None for c in processes) or time.monotonic() > deadline:
                    raise RuntimeError('server startup failed; inspect server/signal logs')
                time.sleep(0.1)
            watcher.start()
            guest = out / 'guest.log'
            console = 'console=ttyS0 rdinit=/init panic=-1 quiet'
            display = ['-nographic']
            if args.interactive:
                size = os.get_terminal_size(0)
                console += f' gyit.rows={size.lines} gyit.cols={size.columns}'
                # The console stays on this terminal (Ctrl-C reaches the guest)
                # and is also logged, so the power-down below can be observed.
                display = ['-display', 'none', '-chardev', f'stdio,id=console,mux=on,signal=off,logfile={guest}',
                           '-serial', 'chardev:console', '-mon', 'chardev=console,mode=readline']
            qemu = ['qemu-system-x86_64', '-enable-kvm', '-cpu', 'host',
                    '-machine', 'q35,memory-backend=mem', '-m', '8G', '-smp', '4',
                    '-object', 'memory-backend-memfd,id=mem,size=8G,share=on',
                    '-kernel', args.kernel, '-initrd', archive, '-append', console,
                    *display, '-no-reboot', '-nic', 'none',
                    '-chardev', f'socket,id=gyit,path={scratch}/gyit.sock',
                    '-device', 'vhost-user-fs-pci,chardev=gyit,tag=gyit',
                    '-chardev', f'socket,id=signal,path={scratch}/signal.sock',
                    '-device', 'vhost-user-fs-pci,chardev=signal,tag=signal']
            if args.interactive:
                # QEMU can stall in vhost-user device teardown after the guest
                # powers off; the finally block stops it.
                qemu = subprocess.Popen([str(x) for x in qemu])
                processes.append(qemu)
                while qemu.poll() is None and 'reboot: Power down' not in (guest.read_text(errors='replace') if guest.exists() else ''):
                    time.sleep(0.5)
                return 0
            qemu = launch('guest', qemu)
            deadline = time.monotonic() + args.timeout
            while qemu.poll() is None and 'GYIT_VERIFY_DONE' not in guest.read_text(errors='replace'):
                if time.monotonic() > deadline:
                    raise TimeoutError('guest checks exceeded timeout; inspect guest log')
                time.sleep(0.5)
        finally:
            stopped.set()
            if watcher.is_alive():
                watcher.join()
            watcher_log.close()
            for process in reversed(processes):
                stop(process, timeout=2 if process.args[0] == 'qemu-system-x86_64' else 15)
            for log in logs:
                log.close()
    results = [json.loads(line.split('GYIT_VERIFY ', 1)[1])
               for line in (out / 'guest.log').read_text(errors='replace').replace('\r', '').splitlines()
               if line.startswith('GYIT_VERIFY ')]
    (out / 'result.json').write_text(json.dumps(results, indent=2) + '\n')
    for r in results:
        print(json.dumps(r))
    return 0 if results and all(r.get('ok', True) for r in results) else 1


if __name__ == '__main__':
    sys.exit(main())
