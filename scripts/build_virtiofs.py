#!/usr/bin/env python3
"""Build the macOS 27 direct Virtio test harness, without FSKit or a host mount."""
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / '.build/virtiofs'

def run(*args):
    subprocess.run([str(x) for x in args], cwd=ROOT, check=True)

def prepare_go_fuse(out):
    out.mkdir(parents=True, exist_ok=True)
    module = json.loads(subprocess.check_output(['go','mod','download','-json','github.com/hanwen/go-fuse/v2'],cwd=ROOT))
    # Upstream selects the wire ABI by host OS; this host serves a Linux guest.
    # Keep that adaptation isolated from the normal CLI and FSKit builds.
    patched=out/'go-fuse'
    if patched.exists():
        patched.chmod(0o755)
        for p in patched.rglob('*'):
            p.chmod(0o755 if p.is_dir() else 0o644)
    shutil.copytree(module['Dir'], patched, dirs_exist_ok=True, copy_function=shutil.copyfile)
    patched.chmod(0o755)
    for p in patched.rglob('*'):
        p.chmod(0o755 if p.is_dir() else 0o644)
    overlay=ROOT/'scripts/virtiofs-overlay'
    for source in overlay.rglob('*.go.in'):
        destination=patched/source.relative_to(overlay).with_suffix('')
        destination.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source,destination)
    modfile=out/'virtiofs.mod'
    shutil.copyfile(ROOT/'go.mod',modfile)
    shutil.copyfile(ROOT/'go.sum',modfile.with_suffix('.sum'))
    run('go','mod','edit',f'-modfile={modfile}',f'-replace=github.com/hanwen/go-fuse/v2={patched}')
    run('go','mod','edit',f'-modfile={modfile}',f'-replace=github.com/klauspost/compress={ROOT}/third_party/compress')
    return modfile

def main():
    modfile = prepare_go_fuse(OUT)
    run('go','build',f'-modfile={modfile}','-tags=gyit_virtiofs','-buildmode=c-archive','-o',OUT/'libgyitvirtio.a','./cmd/gyit-vz')
    run('xcrun','swiftc','-O','-target','arm64-apple-macos27.0','-import-objc-header',OUT/'libgyitvirtio.h',ROOT/'macos/virtiofs/VM.swift',OUT/'libgyitvirtio.a','-framework','Virtualization','-framework','Security','-framework','CoreFoundation','-lresolv','-lz','-o',OUT/'gyit-vm')
    run('codesign','--force','--sign','-','--entitlements',ROOT/'macos/virtiofs/VM.entitlements',OUT/'gyit-vm')

if __name__=='__main__': main()
