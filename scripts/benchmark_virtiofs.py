#!/usr/bin/env python3
"""Run the complete scan comparison inside the VZ/virtio-fs test VM.

Start scripts/virtiofs.yaml first. For a fresh-mount measurement, stop the VM,
restart the host gyit mount, then start the VM again. This script does not evict
the host's bounded disk cache or claim that OS page caches are empty.
"""
import argparse
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--vm", default="gyit-virtiofs")
    p.add_argument("--repository", default="langchain-ai/open-swe")
    p.add_argument("--label", default="virtiofs")
    p.add_argument("--guest-disk", action="store_true", help="Also compare to a guest-local checkout, after the shared comparison")
    args = p.parse_args()
    if any(part in ("", ".", "..") for part in args.repository.split("/")):
        p.error("repository must be an owner/repository path")
    if not args.label or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for c in args.label):
        p.error("label must contain only letters, numbers, hyphens and underscores")
    def guest(*command, **kwargs):
        return subprocess.run(["limactl", "shell", "--workdir=/", args.vm, *command], **kwargs)
    for path in ("/mnt/gyit", "/mnt/checkout"):
        kind = guest("findmnt", "-n", "-o", "FSTYPE", "-T", path, check=True, capture_output=True, text=True).stdout.strip()
        if kind != "virtiofs":
            raise SystemExit(f"{path} is {kind!r}, expected virtiofs")
    subprocess.run(["limactl", "copy", str(ROOT / "scripts/benchmark_scan.py"), f"{args.vm}:/tmp/gyit-benchmark-scan.py"], check=True)
    comparisons = [("shared", "/mnt/checkout")]
    if args.guest_disk:
        # The source share must be a self-contained Git checkout (no host-only
        # object alternates). Keep the fixture across benchmark runs.
        guest("sh", "-c", 'if [ ! -d "$HOME/scan-checkout/.git" ]; then git clone --no-hardlinks /mnt/checkout "$HOME/scan-checkout"; fi', check=True)
        home = guest("printenv", "HOME", check=True, capture_output=True, text=True).stdout.strip()
        comparisons.append(("guest-disk", home + "/scan-checkout"))
    failed = False
    for name, checkout in comparisons:
        label = args.label + "-" + name
        remote = "/tmp/gyit-scan-" + label + ".json"
        result = guest("python3", "/tmp/gyit-benchmark-scan.py", "--mount", "/mnt/gyit/github.com/" + args.repository,
                       "--checkout", checkout, "--label", label, "--output", remote, capture_output=True, text=True)
        if result.returncode not in (0, 1) or "\"modes\"" not in result.stdout:
            raise SystemExit(result.stderr + result.stdout)
        destination = ROOT / ".build" / ("scan-" + label + ".json")
        destination.parent.mkdir(exist_ok=True)
        subprocess.run(["limactl", "copy", f"{args.vm}:{remote}", str(destination)], check=True)
        metrics = json.loads(destination.read_text())
        for mode, values in metrics["modes"].items():
            print(f"{name} {mode}: first {values['first_ratio']:.2f}x, warm {values['warm_ratio']:.2f}x; {'PASS' if values['ok'] else 'FAIL'}", flush=True)
        print(destination, flush=True)
        # The matched transport comparison is the primary gate. Guest-local
        # results remain separate so transport overhead is never hidden.
        if name == "shared":
            failed = not metrics["ok"]
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
