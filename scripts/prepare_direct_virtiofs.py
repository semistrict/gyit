#!/usr/bin/env python3
"""Prepare a reusable direct-Virtio fixture from a running Ubuntu ARM64 Lima VM.

Stops the named VM before cloning its disk. It remains stopped afterward.
The source checkout must be the same revision as the repository under test.
"""
import argparse
import gzip
import json
from pathlib import Path
import subprocess

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'.build/virtiofs'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vm',required=True,help='Disposable running Lima VZ fixture; will be stopped')
    args=parser.parse_args()
    checkout=ROOT/'.testdata/scan-checkout'
    if not (checkout/'.git').exists(): parser.error('prepare .testdata/scan-checkout first')
    OUT.mkdir(parents=True,exist_ok=True)
    if (OUT/'disk.raw').exists(): parser.error('fixture disk already exists; reuse it with benchmark_direct_virtiofs.py')
    info=json.loads(subprocess.check_output(['limactl','list','--json',args.vm]))
    if info['status']!='Running' or info['arch']!='aarch64' or info['vmType']!='vz':
        parser.error('source must be a running ARM64 VZ instance')
    # cat follows the guest boot symlinks; scp may copy dangling symlinks.
    for guest,local in [('/boot/vmlinuz','vmlinuz'),('/boot/initrd.img','initrd')]:
        with (OUT/local).open('wb') as output:
            subprocess.run(['limactl','shell','--workdir=/',args.vm,'sudo','cat',guest],stdout=output,check=True)
    kernel=(OUT/'vmlinuz').read_bytes()
    (OUT/'Image').write_bytes(gzip.decompress(kernel) if kernel[:2]==b'\x1f\x8b' else kernel)
    subprocess.run(['limactl','stop',args.vm],check=True)
    subprocess.run(['cp','-c',str(Path(info['dir'])/'disk'),str(OUT/'disk.raw')],check=True)
    remote=ROOT/'.testdata/direct-remotes/langchain-ai/open-swe.git'
    if not remote.exists():
        remote.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['git','clone','--bare','--shared',str(checkout),str(remote)],check=True)
    print('Fixture ready. Run python3 scripts/build_virtiofs.py, then python3 scripts/benchmark_direct_virtiofs.py.')

if __name__=='__main__':main()
