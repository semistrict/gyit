#!/usr/bin/env python3
"""Build, Developer ID sign, notarize, and verify an arm64 gyit distribution."""

import argparse
import hashlib
import json
import os
import pathlib
import plistlib
import shutil
import subprocess
import sys


ROOT = pathlib.Path(__file__).resolve().parents[1]
BUILD = ROOT / ".build" / "macos"


def run(*args, capture=False):
    return subprocess.run(
        [str(arg) for arg in args], cwd=ROOT, check=True,
        capture_output=capture, text=capture,
    )


def signing_identity():
    identities = run("security", "find-identity", "-v", "-p", "codesigning", capture=True).stdout
    matches = [line.split('"')[1] for line in identities.splitlines()
               if '"Developer ID Application:' in line]
    if len(matches) != 1:
        raise RuntimeError("expected exactly one Developer ID Application identity")
    return matches[0]


def check_profile(bundle, identifier, entitlement):
    path = bundle / "Contents" / "embedded.provisionprofile"
    decoded = subprocess.run(
        ["security", "cms", "-D", "-i", str(path)],
        check=True, capture_output=True,
    )
    profile = plistlib.loads(decoded.stdout)
    claims = profile["Entitlements"]
    actual = claims.get("com.apple.application-identifier", claims.get("application-identifier"))
    if not profile.get("ProvisionsAllDevices") or actual != identifier or claims.get(entitlement) is not True:
        raise RuntimeError(f"missing Developer ID profile authorizing {identifier} and {entitlement}")


def check_signature(bundle, identity):
    run("codesign", "--verify", "--strict", "--verbose=2", bundle)
    details = run("codesign", "-dvv", bundle, capture=True).stderr
    if f"Authority={identity}" not in details or "flags=0x10000(runtime)" not in details or "Timestamp=" not in details:
        raise RuntimeError(f"missing timestamped Developer ID hardened-runtime signature: {bundle}")
    archs = run("xcrun", "lipo", "-archs", bundle / "Contents" / "MacOS" / bundle.stem, capture=True).stdout.strip()
    if archs != "arm64":
        raise RuntimeError(f"expected arm64 only, found {archs}: {bundle}")


def digest(path):
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            sha.update(chunk)
    return sha.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team", required=True, help="Apple Developer team identifier")
    parser.add_argument("--notary-profile", required=True, help="existing notarytool Keychain profile")
    args = parser.parse_args()
    if sys.platform != "darwin" or os.uname().machine != "arm64":
        raise RuntimeError("release builds require Apple Silicon macOS")
    if run("git", "status", "--porcelain", capture=True).stdout.strip():
        raise RuntimeError("commit source changes before creating a release")
    commit = run("git", "rev-parse", "HEAD", capture=True).stdout.strip()
    identity = signing_identity()
    # Check credentials before building or uploading. This reads the local
    # Keychain profile; no secret enters an argument or release artifact.
    run("xcrun", "notarytool", "history", "--keychain-profile", args.notary_profile,
        "--output-format", "json", capture=True)

    run(sys.executable, ROOT / "scripts" / "build_macos.py", "--team", args.team)
    info = plistlib.loads((BUILD / "App.Info.plist").read_bytes())
    version = f'{info["CFBundleShortVersionString"]}.{info["CFBundleVersion"]}'
    work = BUILD / "release"
    archive = work / "gyit.xcarchive"
    exported = work / "exported"
    stage = work / "stage"
    dist = work / "dist"
    for path in (archive, exported, stage, dist):
        if path.exists():
            shutil.rmtree(path)
    work.mkdir(parents=True, exist_ok=True)
    export_options = work / "ExportOptions.plist"
    export_options.write_bytes(plistlib.dumps({
        "method": "developer-id", "signingStyle": "automatic", "teamID": args.team,
        "destination": "export", "signingCertificate": "Developer ID Application",
    }))
    run("xcodebuild", "-project", BUILD / "gyit.xcodeproj", "-scheme", "gyit",
        "-configuration", "Debug", "-destination", "generic/platform=macOS",
        "-archivePath", archive, "-allowProvisioningUpdates", "archive")
    run("xcodebuild", "-exportArchive", "-archivePath", archive,
        "-exportPath", exported, "-exportOptionsPlist", export_options,
        "-allowProvisioningUpdates")
    app = exported / "gyit.app"
    extension = app / "Contents" / "Extensions" / "gyitFS.appex"
    check_profile(app, args.team + ".com.semistrict.gat", "com.apple.developer.fskit.mount")
    check_profile(extension, args.team + ".com.semistrict.gat.filesystem",
                  "com.apple.developer.fskit.fsmodule")
    for bundle in (extension, app):
        check_signature(bundle, identity)
    run("codesign", "--verify", "--deep", "--strict", app)

    stage.mkdir()
    shutil.copytree(app, stage / "gyit.app", symlinks=True)
    cli = stage / "gyit"
    run("go", "build", "-trimpath", "-o", cli, "./cmd/gyit")
    if run("xcrun", "lipo", "-archs", cli, capture=True).stdout.strip() != "arm64":
        raise RuntimeError("CLI is not arm64-only")
    run("codesign", "--force", "--options", "runtime", "--timestamp",
        "--sign", identity, cli)
    run("codesign", "--verify", "--strict", cli)
    cli_signature = run("codesign", "-dvv", cli, capture=True).stderr
    if (f"Authority={identity}" not in cli_signature or
            "flags=0x10000(runtime)" not in cli_signature or
            "Timestamp=" not in cli_signature):
        raise RuntimeError("standalone command lacks a timestamped hardened-runtime signature")
    (stage / "INSTALL.txt").write_text(
        "Install gyit.app in /Applications and enable its file system extension "
        "in System Settings. Copy the gyit command to a directory on your PATH, "
        "such as ~/.local/bin. The app mounts repositories at /Volumes/gyit.\n"
    )
    for name in ("LICENSE", "NOTICE"):
        shutil.copy2(ROOT / name, stage / name)
    (stage / "Applications").symlink_to("/Applications")
    dist.mkdir()
    dmg = dist / f"gyit_{version}_arm64.dmg"
    run("hdiutil", "create", "-ov", "-volname", "gyit", "-srcfolder", stage,
        "-format", "UDZO", dmg)
    run("codesign", "--force", "--timestamp", "--sign", identity, dmg)
    if run("git", "status", "--porcelain", capture=True).stdout.strip() or run(
            "git", "rev-parse", "HEAD", capture=True).stdout.strip() != commit:
        raise RuntimeError("source changed during the release build")

    result = run("xcrun", "notarytool", "submit", dmg,
                 "--keychain-profile", args.notary_profile, "--wait",
                 "--timeout", "2h", "--output-format", "json", capture=True)
    receipt = json.loads(result.stdout)
    (work / "notary.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if receipt.get("status") != "Accepted":
        raise RuntimeError(f'Apple notarization status: {receipt.get("status")}')
    run("xcrun", "notarytool", "log", receipt["id"], work / "notary-log.json",
        "--keychain-profile", args.notary_profile)
    run("xcrun", "stapler", "staple", dmg)
    run("xcrun", "stapler", "validate", dmg)
    run("xcrun", "stapler", "staple", app)
    run("xcrun", "stapler", "validate", app)
    run("codesign", "--verify", "--deep", "--strict", app)
    run("spctl", "--assess", "--type", "execute", "--verbose=2", app)
    run("spctl", "--assess", "--type", "open", "--context", "context:primary-signature",
        "--verbose=2", dmg)
    zip_path = dist / f"gyit_{version}_arm64.zip"
    shutil.rmtree(stage / "gyit.app")
    shutil.copytree(app, stage / "gyit.app", symlinks=True)
    run("ditto", "-c", "-k", "--sequesterRsrc", stage, zip_path)
    extracted = work / "extracted"
    if extracted.exists():
        shutil.rmtree(extracted)
    extracted.mkdir()
    run("ditto", "-x", "-k", zip_path, extracted)
    check_signature(extracted / "gyit.app", identity)
    run("xcrun", "stapler", "validate", extracted / "gyit.app")
    run("codesign", "--verify", "--strict", extracted / "gyit")
    (dist / "SOURCE_COMMIT").write_text(commit + "\n")
    (dist / "NOTARIZED.json").write_text(json.dumps(receipt, indent=2) + "\n")
    (dist / "SHA256SUMS").write_text(
        "".join(f"{digest(path)}  {path.name}\n" for path in (dmg, zip_path))
    )
    print(f"Verified notarized distribution: {dist}")


if __name__ == "__main__":
    main()
