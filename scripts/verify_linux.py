#!/usr/bin/env python3
"""Bounded full-history Linux correctness checks using an ordinary Go build.

The source is an existing local clone. This script never clones, fetches, repacks,
or removes it. Each import requires an empty destination; read phases reuse the
published store. Reports, independent Git output, and failed stores are retained.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = ROOT / ".build/linux-correctness"
PHASES = {
    "import": ("repo.test", "TestCorrectnessImportLinux", "GAT_RUN_CORRECTNESS_IMPORT", 175, 180),
    "verify": ("repo.test", "TestCorrectnessVerifyLinux", "GAT_RUN_CORRECTNESS_VERIFY", 190, 200),
    "reader": ("repo.test", "TestCorrectnessPersistedReader", "GAT_RUN_READER_CORRECTNESS", 190, 200),
    "catalog": ("repo.test", "TestCorrectnessFullCatalog", "GAT_RUN_FULL_CATALOG_CORRECTNESS", 190, 200),
    "commands": ("commands.test", "TestCorrectnessLinuxCommandParity", "GAT_RUN_CORRECTNESS_COMMANDS", 250, 260),
}


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_env():
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("GAT_", "GIT_"))}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL="/dev/null",
               GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1",
               GIT_NO_LAZY_FETCH="1", LC_ALL="C", TZ="UTC")
    return env


def run_capped(argv, *, timeout, stdout, stderr, env=None):
    """Kill the entire owned process group on timeout or interruption."""
    process = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=stdout,
                               stderr=stderr, start_new_session=True)
    started, timed_out = time.monotonic(), False
    try:
        try:
            result = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(process.pid, signal.SIGKILL)
            result = process.wait(timeout=10)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)
        raise
    try:
        os.killpg(process.pid, 0)
        survivors = True
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        survivors = False
    return {"exit_code": result, "seconds": time.monotonic() - started,
            "timed_out": timed_out, "surviving_group_killed": survivors}


def source_head(source):
    return subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "--verify", "HEAD"],
        env=clean_env(), timeout=10).decode().strip()


def source_identity(source):
    gitdir = Path(subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "--absolute-git-dir"],
        env=clean_env(), timeout=10).decode().strip())
    result = {}
    for path in sorted((gitdir / "objects").rglob("*")):
        if path.is_file():
            stat = path.stat()
            result[str(path.relative_to(gitdir))] = [stat.st_size, stat.st_mtime_ns, stat.st_ino]
    return gitdir, result


def inventory(facts):
    h = hashlib.sha256()
    population, count = {}, 0
    with facts.open("rb") as stream:
        for line in stream:
            h.update(line)
            fields = line.split()
            if len(fields) != 3 or len(fields[0]) != 40:
                raise ValueError("facts must contain SHA-1 object ID, kind, and size")
            bytes.fromhex(fields[0].decode("ascii"))
            kind, size = fields[1].decode("ascii"), int(fields[2])
            if kind not in ("blob", "tree", "commit", "tag") or size < 0:
                raise ValueError("invalid independent object fact")
            entry = population.setdefault(kind, {"Objects": 0, "RawBytes": 0})
            entry["Objects"] += 1
            entry["RawBytes"] += size
            count += len(line)
    if not population.get("commit", {}).get("Objects"):
        raise ValueError("source inventory has no commits")
    return h.hexdigest(), count, population


def prepare(args):
    target = args.fixture
    if target.exists():
        fixture = json.loads(target.read_text())
        if fixture["source"] != str(args.source) or fixture["head"] != source_head(args.source):
            raise ValueError("existing fixture belongs to a different source or HEAD")
        _, current = source_identity(args.source)
        if current != fixture["source_identity"]:
            raise ValueError("source objects changed; use a new fixture output")
        if digest(Path(fixture["facts"])) != fixture["facts_sha256"]:
            raise ValueError("saved independent facts changed")
        print("Reusing independent Git facts:", target)
        return
    head = source_head(args.source)
    gitdir, before = source_identity(args.source)
    packs = sorted((gitdir / "objects/pack").glob("*.pack"))
    if len(packs) != 1:
        raise ValueError("this full Linux fixture requires one existing source pack")
    facts = args.facts or args.output / "objects.tsv"
    if facts.exists():
        if not args.expected_facts_sha256:
            raise ValueError("reusing facts without a fixture requires --expected-facts-sha256 from the independent inventory")
    else:
        facts.parent.mkdir(parents=True, exist_ok=True)
        with facts.open("xb") as stdout, (args.output / "prepare.stderr").open("xb") as stderr:
            process = run_capped(
                ["git", "-C", str(args.source), "cat-file", "--batch-all-objects", "--unordered",
                 "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                timeout=180, stdout=stdout, stderr=stderr, env=clean_env())
        save(args.output / "prepare-process.json", process)
        if process["exit_code"] or process["timed_out"] or process["surviving_group_killed"]:
            raise ValueError("independent Git inventory failed; partial facts are retained")
    checksum, size, population = inventory(facts)
    if args.expected_facts_sha256 and checksum != args.expected_facts_sha256:
        raise ValueError("independent facts digest differs")
    _, after = source_identity(args.source)
    if head != source_head(args.source) or before != after:
        raise ValueError("source changed during independent inventory")
    pack = packs[0].with_suffix("")
    with packs[0].open("rb") as stream:
        header = stream.read(12)
    if header[:4] != b"PACK" or int.from_bytes(header[8:12], "big") != sum(p["Objects"] for p in population.values()):
        raise ValueError("facts population differs from source pack; loose objects are unsupported by this fixture")
    save(target, {"source": str(args.source), "head": head, "pack": str(pack),
                  "facts": str(facts), "facts_sha256": checksum, "facts_bytes": size,
                  "population": population, "source_identity": before})
    print("Prepared independent Git facts:", target)


def build(output):
    binary = output / "bin"
    binary.mkdir(exist_ok=True)
    for package, name in [("internal/repo", "repo.test"), ("internal/controlcli", "commands.test")]:
        subprocess.run(["go", "test", "-c", "-o", str(binary / name), "./" + package],
                       cwd=ROOT, check=True, timeout=180)
    subprocess.run(["go", "build", "-o", str(binary / "gat"), "./cmd/gat"],
                   cwd=ROOT, check=True, timeout=180)
    inputs = {ROOT / "go.mod", ROOT / "go.sum", Path(__file__)}
    for directory in (ROOT / "internal", ROOT / "cmd", ROOT / "proto"):
        inputs.update(path for path in directory.rglob("*")
                      if path.is_file() and path.suffix in (".go", ".c", ".h", ".proto"))
    binaries = [binary / name for name in ("repo.test", "commands.test", "gat")]
    save(output / "build.json", {"inputs": {str(p): digest(p) for p in sorted(inputs)},
                                  "binaries": {str(p): digest(p) for p in binaries},
                                  "go": subprocess.check_output(["go", "version"]).decode().strip()})


def run(args):
    manifest = json.loads((args.output / "build.json").read_text())
    for path, expected in (manifest["inputs"] | manifest["binaries"]).items():
        if digest(Path(path)) != expected:
            raise ValueError("changed build input; rebuild before executing: " + path)
    fixture = json.loads(args.fixture.read_text())
    if fixture["source"] != str(args.source):
        raise ValueError("fixture belongs to a different source")
    if args.store == args.source or args.store.is_relative_to(args.source) or args.source.is_relative_to(args.store):
        raise ValueError("source and store must be separate directories")
    if args.phase == "import":
        args.store.mkdir(parents=True, exist_ok=True)
        if args.store.is_symlink() or any(args.store.iterdir()):
            raise ValueError("import needs an empty real store; reuse read phases for a published store")
    elif not (args.store / "HEAD").is_file():
        raise ValueError("verification needs a retained published store")
    output = Path(tempfile.mkdtemp(prefix="run-" + args.phase + "-", dir=args.output))
    work = output / "scratch"
    work.mkdir()
    binary, test, optin, test_cap, group_cap = PHASES[args.phase]
    env = clean_env()
    env.update({optin: "1", "GAT_CORRECTNESS_SOURCE": str(args.source),
                "GAT_CORRECTNESS_FIXTURE": str(args.fixture),
                "GAT_CORRECTNESS_STORE": str(args.store), "GAT_CORRECTNESS_WORK": str(work),
                "GAT_CORRECTNESS_FACTS": fixture["facts"], "GAT_CORRECTNESS_REQUIRE_EDGES": "1"})
    for phase in PHASES:
        key = "COMMAND" if phase == "commands" else phase.upper()
        env["GAT_CORRECTNESS_" + key + "_REPORT"] = str(output / (phase + ".json"))
    argv = [str(args.output / "bin" / binary), "-test.run=^" + test + "$", "-test.v",
            "-test.count=1", "-test.timeout=" + str(test_cap) + "s"]
    source_before = None
    if args.phase == "import":
        if source_head(args.source) != fixture["head"]:
            raise ValueError("source HEAD changed since the independent inventory")
        _, source_before = source_identity(args.source)
        if source_before != fixture["source_identity"]:
            raise ValueError("source objects changed since the independent inventory")
        argv = [str(args.output / "bin/gat"), "import", "--repo", str(args.source),
                "--store", str(args.store), "--temp-dir", str(work)]
    save(output / "command.json", {"argv": argv, "cwd": str(ROOT), "phase": args.phase,
                                  "store": str(args.store), "group_cap_seconds": group_cap})
    print("Running", args.phase, "logs", output, flush=True)
    with (output / "stdout").open("wb") as stdout, (output / "stderr").open("wb") as stderr:
        result = run_capped(argv, timeout=group_cap, stdout=stdout, stderr=stderr, env=env)
    if args.phase == "import":
        # The timed command is the shipping executable. A separate bounded test
        # process verifies its publication, source refs, selected payloads, and
        # commit metadata before the import report can claim correctness.
        matched = re.fullmatch(r"generation ([0-9a-f]{64}) \((\d+) new objects, (\d+) compressed bytes uploaded\)\n",
                               (output / "stdout").read_text())
        expected = sum(p["Objects"] for kind, p in fixture["population"].items() if kind != "tag")
        import_ok = not result["exit_code"] and not result["timed_out"] and not result["surviving_group_killed"]
        import_ok = import_ok and matched is not None and int(matched[2]) == expected
        verification = None
        if import_ok:
            env["GAT_RUN_CORRECTNESS_VERIFY"] = "1"
            env["GAT_CORRECTNESS_NEW_IMPORT"] = "1"
            with (output / "verify.stdout").open("wb") as stdout, (output / "verify.stderr").open("wb") as stderr:
                verification = run_capped(
                    [str(args.output / "bin/repo.test"), "-test.run=^TestCorrectnessVerifyLinux$",
                     "-test.v", "-test.count=1", "-test.timeout=190s"],
                    timeout=200, stdout=stdout, stderr=stderr, env=env)
            try:
                verified = json.loads((output / "verify.json").read_text())["correctness"] is True
            except (OSError, ValueError, KeyError):
                verified = False
            import_ok = verified and not verification["exit_code"] and not verification["timed_out"] and not verification["surviving_group_killed"]
        _, source_after = source_identity(args.source)
        unchanged = source_before == source_after and source_head(args.source) == fixture["head"]
        files, size = 0, 0
        for path in args.store.rglob("*"):
            if path.is_file():
                files += 1
                size += path.stat().st_size
        save(output / "import.json", {"correctness": bool(import_ok and unchanged),
                                     "scope": "ordinary CLI import followed by independent persisted-store verification",
                                     "import_seconds": result["seconds"], "import_over_1s": result["seconds"] > 1,
                                     "generation": matched[1] if matched else None,
                                     "objects": int(matched[2]) if matched else None,
                                     "physical_store_files": files, "physical_store_bytes": size,
                                     "source_unchanged": unchanged, "verification_process": verification})
    try:
        report = json.loads((output / (args.phase + ".json")).read_text())
        report_ok = report.get("correctness", report.get("Completed", False)) is True
        error = None if report_ok else "phase report did not confirm completed correctness checks"
    except (OSError, ValueError) as exc:
        report_ok, error = False, str(exc)
    residual = sorted(path.name for path in work.iterdir())
    if not residual:
        work.rmdir()
    result.update(scratch_entries=residual, store_preserved=str(args.store),
                  report_ok=report_ok, report_error=error,
                  published_HEAD=(args.store / "HEAD").is_file())
    save(output / "process.json", result)
    print("Finished", args.phase, "exit", result["exit_code"], "seconds", round(result["seconds"], 3), "logs", output)
    if result["exit_code"] or result["timed_out"] or result["surviving_group_killed"] or residual or not report_ok:
        if error:
            print("Report error:", error)
        print((output / "stdout").read_text(errors="replace")[-6000:])
        print((output / "stderr").read_text(errors="replace")[-3000:])
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["prepare", "build", *PHASES])
    parser.add_argument("--source", type=Path, default=ROOT / ".testdata/linux-repo.git")
    parser.add_argument("--store", type=Path, default=ROOT / ".testdata/linux-store")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--fixture", type=Path, help="independent fixture manifest; defaults to OUTPUT/fixture.json")
    parser.add_argument("--facts", type=Path, help="existing independent Git inventory; prepare only")
    parser.add_argument("--expected-facts-sha256", help="required to adopt existing facts without a fixture")
    args = parser.parse_args()
    for field in ("source", "store", "output", "fixture", "facts"):
        value = getattr(args, field)
        if value is not None:
            setattr(args, field, value.resolve())
    args.output.mkdir(parents=True, exist_ok=True)
    args.fixture = args.fixture or args.output / "fixture.json"
    if args.phase == "build":
        build(args.output)
    elif args.phase == "prepare":
        prepare(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
