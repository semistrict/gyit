#!/usr/bin/env python3
"""Live update proof; all guest and fixture resources are removed on exit."""
import argparse, os, time, tempfile, subprocess, shutil
from pathlib import Path
from benchmark_nested_kvm import initrd, stop
GUEST = r'''
import json, os, subprocess, time, shutil
from pathlib import Path
root = Path('/mnt/gyit/github.com/acme/large')
for _ in range(3):
    assert sorted(os.listdir(root)) == ['README.md', 'sub']
    assert os.listdir(root / 'sub') == ['old']
old = open(root / 'sub/old', 'rb')
print('GYIT_UPDATE_READY', flush=True)
while not Path('/mnt/checkout/published').exists():
    time.sleep(0.05)
shutil.copyfile('/mnt/checkout/gyit', '/tmp/gyit')
os.chmod('/tmp/gyit', 0o755)
subprocess.run(['/tmp/gyit', 'update'], cwd=root, check=True)
started = time.monotonic()
while True:
    try:
        assert sorted(os.listdir(root)) == ['README.md', 'added', 'sub'], os.listdir(root)
        assert os.listdir(root / 'sub') == ['new'], os.listdir(root / 'sub')
        assert (root / 'sub/new').read_bytes() == b'new\n'
        assert not (root / 'sub/old').exists()
        assert (root / 'README.md').stat().st_size == len(b'updated fixture\n')
        assert (root / 'README.md').read_bytes() == b'updated fixture\n'
        break
    except (AssertionError, FileNotFoundError):
        if time.monotonic() - started > 1.25:
            raise
        time.sleep(0.025)
fresh_seconds = time.monotonic() - started
assert old.read() == b'old\n'
print('update visible after %.3fs' % fresh_seconds, flush=True)
Path('/tmp/result.json').write_text(json.dumps({'ok': True, 'fresh_seconds': fresh_seconds, 'test': 'virtio-fs directory cache invalidation and pinned open file'}))
'''


def main():
    parser = argparse.ArgumentParser(description='Verify live directory-cache invalidation through an actual nested-KVM virtio-fs mount.')
    for flag in ('server', 'cli', 'kernel', 'initrd', 'output'):
        parser.add_argument('--' + flag, type=Path, required=True)
    args = parser.parse_args()
    if os.geteuid() != 0 or not Path('/dev/kvm').exists():
        parser.error('requires root and /dev/kvm')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='gyit-update-proof-') as temp:
        p = Path(temp)
        src = p / 'source'
        src.mkdir()
        remote = p / 'remotes/acme'
        remote.mkdir(parents=True)
        aux = p / 'aux'
        aux.mkdir()

        def git(*args):
            return subprocess.run(['git', '-C', str(src), *args], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=dict(os.environ, GIT_CONFIG_GLOBAL='/dev/null', GIT_CONFIG_NOSYSTEM='1', GIT_AUTHOR_NAME='Test', GIT_AUTHOR_EMAIL='test@example.test', GIT_COMMITTER_NAME='Test', GIT_COMMITTER_EMAIL='test@example.test'))
        git('init', '-q', '-b', 'main')
        (src / 'README.md').write_text('fixture\n')
        (src / 'sub').mkdir()
        (src / 'sub/old').write_text('old\n')
        git('add', '.')
        git('commit', '-qm', 'fixture')
        git('clone', '--bare', '--quiet', str(src), str(remote / 'large.git'))
        (aux / 'README.md').write_text('fixture\n')
        shutil.copy2(args.cli.resolve(), aux / 'gyit')
        image = p / 'initrd.img'
        kernel = args.kernel.resolve()
        guest = p / 'guest.py'
        guest.write_text(GUEST)
        initrd(args.initrd.resolve(), kernel, image, p, guest, 'find', 'update-proof', read_only=False)
        processes = []
        logs = []

        def launch(name, args):
            f = (output / (name + '.log')).open('wb')
            logs.append(f)
            proc = subprocess.Popen([str(a) for a in args], stdout=f, stderr=subprocess.STDOUT)
            processes.append(proc)
            return proc
        try:
            launch('server', [args.server.resolve(), '--store', p / 'store', '--state', p / 'state', '--cache', p / 'cache', '--remote-base', 'file://' + str(p / 'remotes'), '--socket', p / 'gyit.sock'])
            launch('baseline', ['/usr/libexec/virtiofsd', '--socket-path', p / 'checkout.sock', '--shared-dir', aux, '--cache=never', '--sandbox=none'])
            deadline = time.monotonic() + 30
            while not (p / 'gyit.sock').exists() or not (p / 'checkout.sock').exists():
                if time.monotonic() > deadline:
                    raise TimeoutError('startup')
                time.sleep(0.1)
            vm = launch('guest', ['qemu-system-x86_64', '-enable-kvm', '-cpu', 'host', '-machine', 'q35,memory-backend=mem', '-m', '2G', '-smp', '2', '-object', 'memory-backend-memfd,id=mem,size=2G,share=on', '-kernel', kernel, '-initrd', image, '-append', 'console=ttyS0 rdinit=/init panic=-1 quiet', '-nographic', '-no-reboot', '-nic', 'none', '-chardev', f'socket,id=gyit,path={p}/gyit.sock', '-device', 'vhost-user-fs-pci,chardev=gyit,tag=gyit', '-chardev', f'socket,id=checkout,path={p}/checkout.sock', '-device', 'vhost-user-fs-pci,chardev=checkout,tag=checkout'])
            deadline = time.monotonic() + 90
            published = False
            while time.monotonic() < deadline:
                text = (output / 'guest.log').read_text(errors='replace')
                if 'GYIT_UPDATE_READY' in text and (not published):
                    (src / 'added').write_text('added\n')
                    (src / 'README.md').write_text('updated fixture\n')
                    (aux / 'README.md').write_text('updated fixture\n')
                    (src / 'sub/old').unlink()
                    (src / 'sub/new').write_text('new\n')
                    git('add', '-A')
                    git('commit', '-qm', 'updated fixture')
                    git('push', '--quiet', str(remote / 'large.git'), 'main')
                    (aux / 'published').touch()
                    published = True
                if 'GYIT_CONTENT_OK' in text:
                    print(text)
                    break
                if vm.poll() is not None or 'reboot: Power down' in text:
                    raise RuntimeError(text)
                time.sleep(0.1)
            else:
                raise TimeoutError('guest update proof')
        finally:
            for proc in reversed(processes):
                stop(proc, 2)
            for f in logs:
                f.close()
if __name__ == '__main__':
    main()
