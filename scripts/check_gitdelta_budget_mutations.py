#!/usr/bin/env python3
"""Replay the two constant mutations Gremlins cannot reach through Go coverage.

Go overlays keep the working tree unchanged. Each candidate must compile and
fail the operation-budget assertion; build errors and timeouts are not kills.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile


def run(command, root, environment, log):
    with log.open("w") as output:
        process = subprocess.Popen(command, cwd=root, env=environment,
                                   stdout=output, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            code = process.wait(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            return {"command": command, "status": "timeout", "log": log.name}
    return {"command": command, "status": "exited", "exit_code": code, "log": log.name}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="new directory for replay evidence")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    source = root / "internal/gitdelta/program.go"
    original = source.read_text()
    declaration = "const maxOperations = MaxSize/8 + 1"
    if original.count(declaration) != 1:
        parser.error("operation-budget declaration changed; review the mutation catalogue")
    if args.output:
        output = args.output.resolve()
        output.mkdir(parents=True, exist_ok=False)
    else:
        (root / ".build").mkdir(exist_ok=True)
        output = Path(tempfile.mkdtemp(prefix="gitdelta-budget-", dir=root / ".build"))
    environment = {k: v for k, v in os.environ.items()
                   if not k.startswith(("GYIT_", "GREMLINS_"))}
    environment.update(GOWORK="off", GOFLAGS="")
    report = {"source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "cases": []}
    cases = [
        ("baseline", declaration),
        ("division-to-multiplication", "const maxOperations = MaxSize*8 + 1"),
        ("addition-to-subtraction", "const maxOperations = MaxSize/8 - 1"),
    ]
    success = True
    for name, replacement in cases:
        candidate = output / name
        candidate.mkdir()
        patched = candidate / "program.go"
        patched.write_text(original.replace(declaration, replacement))
        overlay = candidate / "overlay.json"
        overlay.write_text(json.dumps({"Replace": {str(source): str(patched)}}) + "\n")
        binary = candidate / "gitdelta.test"
        built = run(["go", "test", "-c", "-overlay=" + str(overlay), "-o", str(binary),
                     "./internal/gitdelta"], root, environment, candidate / "build.log")
        record = {"name": name, "replacement": replacement, "build": built}
        report["cases"].append(record)
        if built.get("exit_code") != 0:
            record["outcome"] = "build-error" if built["status"] != "timeout" else "timeout"
        else:
            tested = run([str(binary), "-test.run=^TestFragmentedRecipeOperationBudget$",
                          "-test.v", "-test.count=1", "-test.timeout=30s"],
                         root, environment, candidate / "test.log")
            record["test"] = tested
            text = (candidate / "test.log").read_text()
            if tested["status"] == "timeout" or "panic: test timed out" in text:
                record["outcome"] = "timeout"
            elif tested.get("exit_code") == 0 and "--- PASS: TestFragmentedRecipeOperationBudget" in text:
                record["outcome"] = "passed"
            elif tested.get("exit_code") == 1 and "--- FAIL: TestFragmentedRecipeOperationBudget" in text:
                record["outcome"] = "assertion-failure"
            else:
                record["outcome"] = "unclassified-failure"
        expected = "passed" if name == "baseline" else "assertion-failure"
        success &= record["outcome"] == expected
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(name + ": " + record["outcome"], flush=True)
        if name == "baseline" and not success:
            break
    print("Evidence: " + str(output))
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
