#!/usr/bin/env python3
"""Build the Linux vhost-user benchmark server, including guest-buffer fixes."""
import argparse
import os
from pathlib import Path
import subprocess

from build_virtiofs import ROOT, prepare_go_fuse


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--arch', choices=['amd64', 'arm64'], default='amd64', help='Linux guest-host architecture')
    p.add_argument('--output', type=Path, default=ROOT / '.build/vhost')
    args = p.parse_args()
    output = args.output.resolve()
    modfile = prepare_go_fuse(output)
    env = dict(os.environ, GOOS='linux', GOARCH=args.arch, CGO_ENABLED='0')
    for command in [
        ['go', 'test', '-c', '-o', output / 'fuse.test', 'github.com/hanwen/go-fuse/v2/fuse'],
        ['go', 'build', '-o', output / 'gyit-vhost', './cmd/gyit-vhost'],
    ]:
        command[2:2] = [f'-modfile={modfile}', '-tags=gyit_virtiofs']
        subprocess.run([str(x) for x in command], cwd=ROOT, env=env, check=True)


if __name__ == '__main__':
    main()
