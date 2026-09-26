# Native macOS mount

The Swift app and FSKit extension wrap the Go GitHub filesystem. The only mount
resource is `https://github.com`, mounted as `/Volumes/gyit` with a `github.com` directory. The app owns
mounting; no Terminal Full Disk Access or local repository picker is required.

Build on Apple Silicon with Xcode selected:

```sh
python3 scripts/build_macos.py
python3 scripts/test_macos.py
```

The default ad-hoc signature supports compilation and bridge tests. Installable
builds require an Apple Developer account in Xcode:

```sh
python3 scripts/build_macos.py --team YOUR_TEAM_ID
```

Install `.build/macos/DerivedData/Build/Products/Debug/gyit.app` in Applications.
Open System Settings → General → Login Items & Extensions → By Category →
File System Extensions, and enable `gyitfs`. The app and extension
use `com.semistrict.gyit` identifiers; a previous installation with an older
identifier needs to be replaced and the new extension enabled once.

The app requires macOS 27. Click **Mount 🍑gyit**, then enter a path such as
`torvalds/linux` or `owner/repo@feature%2Flogin`. Opening it starts background
setup. Read `NOTICE` for progress; the complete file tree replaces it when ready.
Touch the synthetic `NOTICE` to retry a failed setup. Repository files remain
read-only, including any real `NOTICE` after setup succeeds.
Only public repositories are supported for now. The extension uses the actual
Git binary from the installed Xcode or Command Line Tools, since `/usr/bin/git`
is an xcrun launcher that cannot run inside App Sandbox.

Prepared repository data lives in the extension sandbox's Application Support
`gyit/repositories` directory. Its shared bounded mmap cache lives under Caches
`gyit/decoded-v1`. The 4 GiB limit is shared across all repositories and revisions
and applies only to the disposable cache. Repository storage is durable, has no
configured size limit, and is never evicted. Both currently use local disk; the
repository store can later be backed by S3 without changing this separation.

The bridge test uses a small local Git remote and checks NOTICE replacement,
file contents, directory permissions, parent IDs, symlinks, and write rejection.
Check Finder metadata against an existing mounted repository with:

```sh
python3 scripts/test_macos.py --mount /Volumes/gyit \
  --path github.com/owner/repo --path github.com/owner/repo/README.md
```

For a direct distribution on Apple Silicon, install a Developer ID Application
certificate and sign in to the same team in Xcode. Save a notary credential in
your local Keychain with `xcrun notarytool store-credentials gyit`, or use an
existing profile for that team. Commit the source tree, then run:

```sh
python3 scripts/release_macos.py --team YOUR_TEAM_ID --notary-profile gyit
```

The release script archives the app, exports it with Developer ID profiles for
the app and FSKit extension, checks hardened runtime and arm64 architecture,
signs a standalone `gyit` command and disk image, submits the image to Apple,
staples the tickets, and verifies Gatekeeper acceptance. It writes the signed
image, a stapled app ZIP, checksums, and source commit under
`.build/macos/release/dist`. It does not replace the running development app
or publish a download. Provisioning profiles, Keychain credentials, and build
products remain outside tracked sources. Apple requires Developer ID signing,
hardened runtime, a secure timestamp, and an authorized distribution profile
for restricted entitlements.

To publish a GitHub release from the default branch, run:

```sh
python3 scripts/publish_macos_release.py --team YOUR_TEAM_ID --notary-profile gyit
```

This rebuilds and notarizes from clean committed source, checks the artifacts,
pushes the source commit and matching version tag, then uploads the disk image,
app ZIP, checksums, and notarization receipt using `gh`. It does not change
repository visibility. If publication fails after notarization, rerun with
`--reuse-artifacts` to use the same verified files without another submission.
