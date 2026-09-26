#!/usr/bin/env python3
"""Build, notarize, tag, and publish an Apple Silicon gyit GitHub release."""

import argparse
import json
import pathlib
import plistlib
import re
import subprocess
import sys
import tempfile

from release_macos import (BUILD, ROOT, check_profile, check_signature,
                           digest, signing_identity)


DIST = BUILD / "release" / "dist"
ASSET_NAMES = ("SHA256SUMS", "NOTARIZED.json")


def run(*args, capture=False):
    return subprocess.run(
        [str(arg) for arg in args], cwd=ROOT, check=True,
        capture_output=capture, text=capture,
    )


def release_assets(dist, commit):
    if (dist / "SOURCE_COMMIT").read_text().strip() != commit:
        raise RuntimeError("release artifacts do not belong to the current commit")
    receipt = json.loads((dist / "NOTARIZED.json").read_text())
    if receipt.get("status") != "Accepted" or not receipt.get("id"):
        raise RuntimeError("release artifacts lack an accepted notarization receipt")
    images = list(dist.glob("gyit_*_arm64.dmg"))
    if len(images) != 1:
        raise RuntimeError("expected exactly one arm64 disk image")
    image = images[0]
    match = re.fullmatch(r"gyit_(\d+\.\d+\.\d+)_arm64\.dmg", image.name)
    if not match:
        raise RuntimeError(f"unexpected disk image name: {image.name}")
    version = match.group(1)
    archive = dist / f"gyit_{version}_arm64.zip"
    if not archive.is_file():
        raise RuntimeError(f"missing app archive: {archive.name}")
    checksums = {}
    for line in (dist / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (\S+)", line)
        if not match or match.group(2) in checksums:
            raise RuntimeError("invalid or duplicate checksum entry")
        checksums[match.group(2)] = match.group(1)
    expected = {path.name: digest(path) for path in (image, archive)}
    if checksums != expected:
        raise RuntimeError("release artifact checksums do not match")
    return version, (image, archive, *(dist / name for name in ASSET_NAMES))


def remote_tag_commit(tag):
    output = run("git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}",
                 f"refs/tags/{tag}^{{}}", capture=True).stdout
    refs = dict(line.split("\t", 1)[::-1] for line in output.splitlines())
    return refs.get(f"refs/tags/{tag}^{{}}", refs.get(f"refs/tags/{tag}"))


def verify_notarized_assets(image, archive, team):
    run("xcrun", "stapler", "validate", image)
    run("spctl", "--assess", "--type", "open", "--context",
        "context:primary-signature", image)
    with tempfile.TemporaryDirectory(prefix="gyit-release-") as directory:
        run("ditto", "-x", "-k", archive, directory)
        root = pathlib.Path(directory)
        app = root / "gyit.app"
        extension = app / "Contents" / "Extensions" / "gyitfs.appex"
        if not any(entry.name == "gyit.app" for entry in root.iterdir()):
            raise RuntimeError("archive app bundle must use lowercase gyit.app")
        app_metadata = plistlib.loads((app / "Contents" / "Info.plist").read_bytes())
        if (app_metadata.get("CFBundleIdentifier") != "com.semistrict.gyit" or
                app_metadata.get("CFBundleName") != "🍑gyit" or
                not (app / "Contents" / "Resources" / "gyit.icns").is_file()):
            raise RuntimeError("archive app has incomplete gyit branding")
        identity = signing_identity()
        check_profile(app, team + ".com.semistrict.gyit", "com.apple.developer.fskit.mount")
        check_profile(extension, team + ".com.semistrict.gyit.filesystem",
                      "com.apple.developer.fskit.fsmodule")
        for bundle in (extension, app):
            check_signature(bundle, identity)
        run("xcrun", "stapler", "validate", app)
        run("spctl", "--assess", "--type", "execute", app)
        command = root / "gyit"
        run("codesign", "--verify", "--strict", command)
        details = run("codesign", "-dvv", command, capture=True).stderr
        if (f"Authority={identity}" not in details or
                "flags=0x10000(runtime)" not in details or
                "Timestamp=" not in details or
                run("xcrun", "lipo", "-archs", command, capture=True).stdout.strip() != "arm64"):
            raise RuntimeError("standalone command lacks a valid arm64 Developer ID signature")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--team", required=True, help="Apple Developer team identifier")
    parser.add_argument("--notary-profile", required=True, help="existing notarytool Keychain profile")
    parser.add_argument("--reuse-artifacts", action="store_true",
                        help="publish already verified artifacts for the current commit")
    args = parser.parse_args()

    if run("git", "status", "--porcelain", capture=True).stdout.strip():
        raise RuntimeError("commit source changes before publishing")
    commit = run("git", "rev-parse", "HEAD", capture=True).stdout.strip()
    repo = json.loads(run("gh", "repo", "view", "--json",
                          "nameWithOwner,defaultBranchRef", capture=True).stdout)
    default_branch = repo["defaultBranchRef"]["name"]
    branch = run("git", "branch", "--show-current", capture=True).stdout.strip()
    if branch != default_branch:
        raise RuntimeError(f"publish from the default branch ({default_branch})")

    if not args.reuse_artifacts:
        run(sys.executable, ROOT / "scripts" / "release_macos.py",
            "--team", args.team, "--notary-profile", args.notary_profile)
    version, assets = release_assets(DIST, commit)
    tag = f"v{version}"
    verify_notarized_assets(assets[0], assets[1], args.team)

    existing = remote_tag_commit(tag)
    if existing and existing != commit:
        raise RuntimeError(f"remote tag {tag} points to another commit")
    local = subprocess.run(["git", "rev-parse", "-q", "--verify",
                            f"refs/tags/{tag}^{{commit}}"], cwd=ROOT,
                           capture_output=True, text=True)
    if local.returncode == 0 and local.stdout.strip() != commit:
        raise RuntimeError(f"local tag {tag} points to another commit")

    run("git", "push", "origin", f"HEAD:refs/heads/{default_branch}")
    if not existing:
        if local.returncode != 0:
            run("git", "tag", "-a", tag, "-m", f"gyit {version}", commit)
        run("git", "push", "origin", f"refs/tags/{tag}")

    notes = (
        f"🍑gyit {version} for Apple Silicon (macOS 27 or later).\n\n"
        "Mount public GitHub repositories at `/Volumes/gyit/github.com/owner/repo` "
        "with the gyit app. The download also contains the signed `gyit` command.\n\n"
        "The app and command are Developer ID signed and Apple notarized. "
        "Verify the download with `SHA256SUMS`.\n\n"
        f"Source commit: `{commit}`.\n"
    )
    result = run("gh", "release", "create", tag, *assets, "--verify-tag",
                 "--title", f"🍑gyit {version}", "--notes", notes,
                 "--repo", repo["nameWithOwner"], capture=True)
    print(result.stdout.strip())


if __name__ == "__main__":
    main()
