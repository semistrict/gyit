#!/usr/bin/env python3
"""Build the real Go reader demo and a portable, offline presentation."""
import argparse
import base64
import os
from pathlib import Path
import shutil
import subprocess

root = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--refresh-fixture', action='store_true')
args = parser.parse_args()

def run(*command, **kwargs):
    return subprocess.check_output(command, cwd=root, **kwargs).decode().strip()

fixture = root / 'internal/tour/testdata/repository.zip'
if args.refresh_fixture or not fixture.exists():
    print(run('go', 'run', './cmd/gyit-tour-fixture'))
assets = root / 'docs/assets'
assets.mkdir(exist_ok=True)
wasm = assets / 'gyit.wasm'
run('go', 'build', '-trimpath', '-ldflags=-s -w', '-o', str(wasm), './cmd/gyit-tour-wasm',
    env={**os.environ, 'GOOS': 'js', 'GOARCH': 'wasm'})
runtime = Path(run('go', 'env', 'GOROOT')) / 'lib/wasm/wasm_exec.js'
shutil.copyfile(runtime, assets / 'wasm_exec.js')
html = (root / 'docs/storage-tour.html').read_text()
# Generated output is never edited directly. Embed the matching Go runtime too.
script = runtime.read_text().replace('</script', r'<\/script')
inline = '<script>' + script + '\nwindow.gyitWasmBase64="' + base64.b64encode(wasm.read_bytes()).decode() + '";</script>'
html = html.replace('<!-- WASM_RUNTIME -->', inline)
out = root / '.build/storage-tour/standalone.html'
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(html)
print(f'{out} ({out.stat().st_size / (1 << 20):.1f} MiB)')
