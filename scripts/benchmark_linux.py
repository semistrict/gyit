#!/usr/bin/env python3
"""Measure an existing full-history Linux fixture; never fetch or modify it.

Run --import-only on the host, then --skip-import inside a Linux FUSE VM.
Outputs are retained alongside a TSV report for exact native-Git comparisons.
Cold means a fresh gyit server/cache, not a flushed OS or storage cache.
"""

import argparse
import contextlib
import csv
import hashlib
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time

from benchmark_budget import import_budget, run_capped


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--store", type=Path, required=True)
    p.add_argument("--gyit", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--import-only", action="store_true")
    mode.add_argument("--skip-import", action="store_true")
    p.add_argument("--timeout", type=int, default=330)
    p.add_argument("--clone-baseline", type=Path,
                   help="TSV from a measured local git clone --no-hardlinks")
    p.add_argument("--estimated-import-seconds", type=float,
                   help="upper-bound estimate supported by smaller experiments; required for a full import")
    a = p.parse_args()
    a.source, a.store, a.gyit, a.output = (
        x.resolve() for x in (a.source, a.store, a.gyit, a.output)
    )
    a.output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ, LC_ALL="C", TZ="UTC", TERM="dumb",
               GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1",
               GIT_PAGER="cat", GYIT_PAGER="cat")
    git = ["git", "-C", str(a.source), "-c", "core.abbrev=7", "-c",
           "color.ui=false", "-c", "log.decorate=false"]

    def native(*args):
        return subprocess.check_output(git + list(args), env=env).decode().strip()

    if native("rev-parse", "--is-shallow-repository") != "false":
        raise RuntimeError("full-history benchmark refuses a shallow source")
    sha = native("rev-parse", "HEAD")
    old = native("rev-parse", "HEAD~100")
    import_limit = None
    if not a.skip_import:
        if a.clone_baseline is None:
            raise RuntimeError("full import is gated: supply --clone-baseline and a supported --estimated-import-seconds")
        import_limit = import_budget(a.clone_baseline, sha, a.estimated_import_seconds)
    (a.output / "environment.txt").write_text(
        f"platform={platform.platform()}\ncpu_count={os.cpu_count()}\n"
        f"source={a.source}\nsha={sha}\nold={old}\nstore={a.store}\n"
        f"gyit={a.gyit}\ngyit_sha256={digest(a.gyit)}\ncache_mib=32\n"
        f"git={native('--version')}\n"
        f"cold=fresh gyit process; OS cache not flushed\n"
        f"import_limit_seconds={import_limit}\n"
        f"import_workers={a.workers}\n"
    )
    report = (a.output / "timings.tsv").open("w", newline="")
    writer = csv.writer(report, delimiter="\t")
    writer.writerow(["operation", "state", "seconds", "exit", "bytes", "sha256", "flag"])
    issues = []

    def record(name, state, elapsed, rc, size=0, checksum=""):
        flag = "SLOW" if elapsed > 1 else ""
        if rc != 0:
            flag += " ERROR"
        writer.writerow([name, state, f"{elapsed:.6f}", rc, size, checksum, flag.strip()])
        report.flush()
        print(f"{name} {state}: {elapsed:.3f}s exit={rc} {flag}", flush=True)

    def run(name, state, cmd, cwd=None, timeout=None, expected=(0,)):
        prefix = a.output / f"{name}-{state}"
        out, err = prefix.with_suffix(".out"), prefix.with_suffix(".err")
        with out.open("wb") as stdout, err.open("wb") as stderr:
            rc, elapsed, expired = run_capped(cmd, cwd=cwd, env=env, stdout=stdout,
                                              stderr=stderr, timeout=timeout or a.timeout)
            if expired:
                rc = 124
        record(name, state, elapsed, rc, out.stat().st_size, digest(out))
        if rc not in expected:
            issues.append(f"{name} {state}: exit {rc}: {err.read_text(errors='replace')[:1000]}")
        return rc, digest(out), out

    if not a.skip_import:
        if a.store.exists() and any(a.store.iterdir()):
            raise RuntimeError("fresh import requires an empty destination; use --skip-import for reads")
        cmd = [str(a.gyit), "import", "--repo", str(a.source), "--store", str(a.store)]
        rc, _, _ = run("import", "fresh", cmd, timeout=import_limit)
        if rc != 0:
            raise RuntimeError(issues[-1])
        run("import", "unchanged", cmd, timeout=import_limit)
        sizes = {}
        for root, _, files in os.walk(a.store):
            for filename in files:
                path = Path(root) / filename
                category = path.relative_to(a.store).parts[0]
                count, size = sizes.get(category, (0, 0))
                sizes[category] = count + 1, size + path.stat().st_size
        with (a.output / "store-size.tsv").open("w") as f:
            f.write("category\tfiles\tbytes\n")
            for category, (count, size) in sorted(sizes.items()):
                f.write(f"{category}\t{count}\t{size}\n")
    if a.import_only:
        if issues:
            raise RuntimeError("\n".join(issues))
        return
    if platform.system() != "Linux":
        raise RuntimeError("operation suite requires Linux FUSE")
    if not (a.store / "HEAD").is_file():
        raise RuntimeError("store has no published import")

    changed = subprocess.check_output(git + ["diff", "--name-only", "--diff-filter=M",
                                             "-z", old, sha], env=env).split(b"\0")
    switch_path = None
    for candidate in changed:
        if not candidate:
            continue
        name = os.fsdecode(candidate)
        metadata = native("ls-tree", "-l", sha, "--", name).split(None, 4)
        if metadata[0] not in ("100644", "100755") or int(metadata[3]) > 1024 * 1024:
            continue
        current_bytes = subprocess.check_output(git + ["show", f"{sha}:{name}"], env=env)
        old_bytes = subprocess.check_output(git + ["show", f"{old}:{name}"], env=env)
        if current_bytes != old_bytes:
            switch_path = name
            break
    if switch_path is None:
        raise RuntimeError("fixture has no small changed regular file for the open-handle check")
    (a.output / "switch-path.txt").write_text(switch_path + "\n")

    mount_number = 0

    @contextlib.contextmanager
    def mounted(label):
        nonlocal mount_number
        mount_number += 1
        with tempfile.TemporaryDirectory(prefix="gyit-linux-bench-") as tmp:
            root = Path(tmp)
            mount = root / "repo"
            mount.mkdir()
            sock = root / "control.sock"
            with (a.output / f"mount-{mount_number}.log").open("wb") as log:
                started = time.monotonic()
                server = subprocess.Popen([str(a.gyit), "mount", "--store", str(a.store),
                    "--sha", sha, "--cache-mib", "32", "--socket", str(sock), str(mount)],
                    stdout=log, stderr=log, env=env, start_new_session=True)
                try:
                    while not (sock.exists() and os.path.ismount(mount)):
                        if server.poll() is not None:
                            raise RuntimeError(f"mount failed; see mount-{mount_number}.log")
                        if time.monotonic() - started > 60:
                            raise RuntimeError("mount readiness timed out")
                        time.sleep(0.005)
                    record("mount", label, time.monotonic() - started, 0)
                    yield mount
                finally:
                    try:
                        if os.path.ismount(mount):
                            subprocess.run(["fusermount3", "-u", str(mount)], timeout=15)
                    finally:
                        if server.poll() is None:
                            server.terminate()
                        try:
                            server.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            server.kill()
                            server.wait()
                    if os.path.ismount(mount):
                        subprocess.run(["fusermount3", "-u", str(mount)], check=True, timeout=15)
                    if os.path.ismount(mount):
                        raise RuntimeError(f"benchmark mount did not clean up: {mount}")

    with mounted("navigation") as mount:
        paths = ["", "drivers", "drivers/net", "drivers/net/ethernet",
                 "drivers/net/ethernet/intel", "drivers/net/ethernet/intel/igb"]
        for state in ("cold", "warm"):
            for i, path in enumerate(paths):
                target = mount / path
                start = time.monotonic()
                names = sorted(os.listdir(target))
                record(f"directory-{i}", state, time.monotonic() - start, 0,
                       len("\n".join(names).encode()))
                tree = sha if not path else f"{sha}:{path}"
                expected = sorted(native("ls-tree", "--name-only", tree).splitlines())
                if names != expected:
                    issues.append(f"directory listing mismatch: {path}")
        inode = (mount / switch_path).stat().st_ino
        with (mount / switch_path).open("rb") as handle:
            before = handle.read()
            for state, revision in (("old", old), ("back", sha)):
                run("switch", state, [str(a.gyit), "switch", revision], cwd=mount)
                if (mount / switch_path).stat().st_ino != inode:
                    issues.append(f"{switch_path} inode changed across switch")
                actual = (mount / switch_path).read_bytes()
                expected = subprocess.check_output(git + ["show", f"{revision}:{switch_path}"], env=env)
                if actual != expected:
                    issues.append(f"{switch_path} after switch to {revision} differs")
                handle.seek(0)
                if handle.read() != before:
                    issues.append("open handle contents changed across switch")

    cases = [
        ("read-file", ["show", "HEAD:init/main.c"]),
        ("ls-tree-root", ["ls-tree", "HEAD"]),
        ("ls-tree-recursive", ["ls-tree", "-r", "--name-only", "HEAD"]),
        ("log-20", ["log", "--oneline", "-n", "20"]),
        ("log-file", ["log", "--oneline", "-n", "20", "--", "kernel/sched/core.c"]),
        ("show", ["show", "--first-parent", "HEAD"]),
        ("diff-names", ["diff", "--no-renames", "--name-status", "HEAD~1", "HEAD"]),
        ("blame", ["blame", "--line-porcelain", "-L", "1,40", "HEAD", "--", "init/main.c"]),
        ("grep-file", ["grep", "-n", "-F", "start_kernel", "HEAD", "--", "init/main.c"]),
        ("grep-repository", ["grep", "-l", "-F", "start_kernel", "HEAD"]),
        ("rev-list-count", ["rev-list", "--count", "HEAD"]),
        ("merge-base", ["merge-base", "HEAD", "HEAD~100"]),
    ]
    for name, args in cases:
        expected = run(name, "git", git + args)
        with mounted(name) as mount:
            for state in ("cold", "warm"):
                cmd = (["cat", str(mount / "init/main.c")] if name == "read-file" else
                       [str(a.gyit), args[0], "--timeout", "5m"] + args[1:])
                actual = run(name, state, cmd, cwd=mount)
                if actual[:2] != expected[:2]:
                    issues.append(f"{name} {state}: output or exit status differs from Git")
    (a.output / "issues.txt").write_text("\n".join(issues) + ("\n" if issues else ""))
    if issues:
        raise RuntimeError("\n".join(issues))


if __name__ == "__main__":
    main()
