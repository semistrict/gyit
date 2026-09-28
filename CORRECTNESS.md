# Ordinary-build integration verification — September 25, 2026

> Historical notes for the retired storage implementation. Its compatibility
> readers, importer, and format-specific verification harnesses have been removed.
> Current storage is documented in [OBJECT_STORE.md](OBJECT_STORE.md); current
> checks are `go test ./...` and the progressive tests in `internal/repo/`.

The archive importer and readers are now part of the normal source tree.
`go build ./cmd/gyit` includes them without overlays or experimental environment
switches. Qualified sources automatically use archive import and publish format
8; unsupported source shapes use reachable-object conversion and report why.
The ordinary binary also reads existing format-9015 publications.

A fresh full Linux import through the ordinary executable took **90.896101
seconds** and produced **20,433,584,926 bytes** in 2,102 local-store files. It
published stable format **8** with 11,839,497 non-tag objects. This is an actual
`gyit import` invocation using the default native-zlib cgo build and automatic
archive selection, without an overlay or importer experiment flags. It finished within the 180-second watchdog.

The source revision is `f0100363d8c374bd8e9ea7c9ba02744f0b802ca4`. The retained
new store is `.testdata/linux-store`, with generation token
`963522e7618c4ae801eea2d3fbb84618f70a8673f991912ccf3dfd176516f5fa`.
No verification step clones, fetches, repacks, or changes the source. The earlier
96.479487-second development-build import is historical evidence; the ordinary
CLI measurement above supersedes it for this source tree.

## Fresh format-8 publication

Independent core verification passed in **9.747322 seconds**. All 949 refs,
including two non-commit refs, and 944 commit tips match Git. The reachable
history count is 1,483,466. Selected commit fields, ordered parents, trees, blobs,
and history boundaries match. The source and published HEAD remained unchanged;
import scratch was empty and no process group remained.

| Fresh-store check | Scope and result | Wall seconds |
| --- | --- | ---: |
| Complete catalog | Every one of 11,839,497 non-tag object identities, types, and sizes matches independent Git facts. All 116,589 main-index pages and 14,806,430 rows pass checksum, ordering, and child-bound checks. | 19.233 |
| Bounded reader | 83 blob cases, 13 directories, and 18 revisions pass full/partial bytes, EOF, pagination, modes, related versions, corruption rejection, and concurrent reads. Peak retained cache is 30,964,803 bytes against 33,554,432 bytes. | 11.548 |
| Command parity | All 98 cases across HEAD, v4.4, and v2.6.24 match native Git stdout and exit status through the CLI dispatcher and real protobuf socket. | 37.970 |
| Linux FUSE | All 45 operations pass on two independent 32 MiB mounts: exact selected bytes, directory names/modes/sizes, read-only enforcement, stable inodes, pinned open handles, isolated switches, and invalid-switch rejection. | See mounted timings below |

The mounted verifier ran in Lima using an ordinary Linux arm64 binary built with
`CGO_ENABLED=0`. It therefore also verifies the pure-Go reader against data
produced by the native-zlib importer. The 90.896101-second import measurement
applies to the native build, not to a pure-Go import.

Read-phase timings include oracle preparation and process overhead; they are not
isolated cold-cache benchmarks. Source/store identity and scratch cleanup pass.
The format-9015 compatibility checks below are separate backward-compatibility
evidence. After copying the FUSE reports, its mounts, process groups, sockets,
temporary root, and owned VM report directory were removed. The existing demo
mount remains present. No cloud resources were created. Final SHA-256 checks
confirmed that all build-manifest inputs are unchanged, the top-level `./gyit`
matches the verified executable, and the Linux binary matches its FUSE report.

## Ordinary-build regressions

`go test -race ./... -count=1 -timeout=210s` passed in **172.057 seconds** including
command overhead; the repository package took **167.782 seconds**. This suite
uses the regular source tree and includes archive selection, source qualification,
reverse-index staging, bounded reader behavior, corruption rejection, cancellation,
and atomic publication regressions.

The subsequent portable decoder additions passed native and pure-Go prefix
tests, pure-Go repository readback/corruption tests, and focused race checks for
`internal/packcodec`, `internal/packfile`, and `internal/packmeta`. Both ordinary
native and `CGO_ENABLED=0` builds succeed; the latter also passed the mounted
Linux checks above. Pure-Go import performance has not been qualified.

The publication tests add a genuinely new commit/tree/blob set after the initial
import. It is absent before the update and readable after publication. Two
concurrent writers produce one CAS winner and one conflict; old snapshots remain
readable. Pre-CAS failures and cancellation preserve the published HEAD.

## Existing-store compatibility

The ordinary build was checked against the retained format-9015 store
`.testdata/correctness-ordered-linux-v2`, whose HEAD token is
`b2d80725a2981a0be1ffc62b321a47bd9dfab5935ad29c465e5e1a6a61b7ec2c`.
These checks ran while the normal race suite was active, so their durations are
compatibility-test observations, not isolated performance benchmarks.

| Check | Scope and result | Wall seconds |
| --- | --- | ---: |
| References and history | All 949 refs, including two non-commit refs, and 944 commit tips match Git. Reachable history count, selected commit fields, ordered parents, trees, blobs, and history boundaries match. | 12.978 |
| Complete catalog | All 11,839,497 non-tag identities, types, and sizes match independently saved Git metadata. All 116,589 main-index pages have valid checksums, ordering, and child bounds. | 20.483 |
| Bounded reader | 83 blob cases, 13 directories, and 18 revisions pass exact full/partial reads, EOF, pagination, modes, related versions, corruption rejection, and concurrent reads. Peak retained cache is 30,964,818 bytes against 33,554,432 bytes. | 6.252 |
| Command parity | All 98 cases across HEAD, v4.4, and v2.6.24 match native Git stdout and exit status through the CLI dispatcher and a real protobuf socket. | 41.058 |

The source and published HEAD remained unchanged. Owned scratch and command
fixtures were removed, and no test process group remained. The first command
attempt was blocked by the sandbox's Unix-socket bind restriction before any
case ran; rerunning the same command with local socket permission passed all 98.

The catalog comparison covers 3,196,537 blobs, 7,159,494 trees, and 1,483,466
commits. Its independent source inventory also contains 944 tag objects, whose
bodies are outside this catalog's object-view coverage. The complete main-index
walk includes 14,806,430 rows in total.

## Operations exceeding one second

Machine-readable reports flag every measured operation over one second. These
were all command cases exceeding one second in the fresh format-8 store run:

| Revision / case | gyit seconds | Git seconds |
| --- | ---: | ---: |
| HEAD / blame | 3.365 | 0.737 |
| HEAD / annotate | 1.785 | 0.719 |
| HEAD / merge-base | 4.219 | 0.061 |
| HEAD / log-glob | 2.669 | 0.120 |
| HEAD / blame-first-parent | 1.794 | 0.724 |
| HEAD / blame-porcelain | 1.375 | 0.540 |
| HEAD / rev-list-range | 3.359 | 0.060 |
| HEAD / merge-base-is-ancestor | 1.713 | 0.052 |
| HEAD / merge-base-not-ancestor | 1.686 | 0.059 |
| v4.4 / merge-base | 1.658 | 0.052 |

Commands run sequentially with one bounded cache per selected revision; caches
are not reset between cases. Command startup briefly overlapped the end of the
reader phase. These are correctness-run observations, not isolated cold-cache
benchmarks. The fresh catalog's independent sorting/hash phase took 8.876 seconds
and its catalog walk 10.336 seconds. Native Git's reachable-commit count took
6.754 seconds during core verification. Import, oracle preparation, and complete
suite durations also exceed one second and remain separately recorded.

The older-store compatibility run occurred under concurrent race-test load;
its distinct timings and additional over-one-second cases remain in its report.

In Lima, initial root listing **including stat of every entry** took **18.54 ms**
and **19.60 ms** on the two mounts. Switching to v4.4 took **20.16 ms** and
switching back took **17.19 ms**. Listing and stat of every entry in
`include/linux` took **1.810–2.875 seconds** across five checks; all five are
flagged. This is more work than listing names alone. These are local-store reads
through the VM's shared filesystem, not S3 latency measurements.

## Corrections retained from the earlier verification

- Share the configured cache and global-size table across command views.
- Preserve refs whose peeled targets are trees or blobs.
- Store committer metadata and format blame porcelain correctly; stop blame
  attribution across regular-file/symlink type changes.
- Match Git's patch alignment and indentation choices, and disambiguate merge
  parent abbreviations without exceeding control-protocol frame bounds.
- Suppress `show` output when the requested paths did not change.
- Read current path attributes after a mount switch even while an older handle
  remains open; retain that handle's snapshot for file reads.
- Permit complete replacement imports while retaining the actual prior HEAD
  token for the final atomic CAS.

## Limits of this evidence

- Catalog coverage is exhaustive for commit/tree/blob identity, type, and size.
  Historical payload bytes, commit display fields, and history values are sampled,
  not exhaustively compared across the roughly 162 GB expanded source.
- Format-8 readers require at least 32 MiB configured cache. The retained-data
  bound includes the global size-table reservation and shared LRU. Separately
  bounded in-flight decoding buffers, query buffers, and process RSS are outside it.
- Source-independent reader children receive no source configuration and cannot
  invoke Git through PATH. This is not an operating-system sandbox denying every
  possible source filesystem path.
- Real S3 end-to-end behavior is unverified here. Store tests exercise its HTTP
  conditional-write contract; publication races use local storage. Full-store
  mounted switching and tiny concurrent publication remain separate checks.
- Archive updates rebuild a complete generation. This does not establish
  incremental archive-update speed or safe garbage collection while old readers
  exist. Reachable-object conversion retains its older incremental behavior.
- A failure during or after the backend's final HEAD commit can have an ambiguous
  publication outcome; pre-CAS failure atomicity does not promise otherwise.
- Open handles retain file bytes; independently pinned old-handle `fstat`
  metadata is not promised by the current FUSE bridge.
- The supported command case set does not imply complete Git compatibility.
  Raw commit/tag object output, arbitrary log formats, rename-aware/three-dot
  diff, blame's default file-rename following, and advanced blame
  movement/whitespace options remain outside it.

## Repeating ordinary-build checks

The portable runner uses ordinary `go test -c` and `go build`; source changes
invalidate its SHA-256 binary manifest and require another build. It reuses an
existing source clone and saved independent Git inventory. Retained-store read
checks never reimport. The commands below check the fresh format-8 store; select
`.testdata/correctness-ordered-linux-v2` to repeat old-format compatibility checks:

```sh
python3 scripts/verify_linux.py prepare --source .testdata/linux-repo.git
python3 scripts/verify_linux.py build
python3 scripts/verify_linux.py verify --store .testdata/linux-store
python3 scripts/verify_linux.py catalog --store .testdata/linux-store
python3 scripts/verify_linux.py reader --store .testdata/linux-store
python3 scripts/verify_linux.py commands --store .testdata/linux-store
```

`python3 scripts/verify_linux.py import --store NEW_EMPTY_DIRECTORY` times the
actual built `gyit import` command, kills its process group after 180 seconds,
and follows successful publication with independent core verification. Every
read phase also has a watchdog; timeout, incomplete reports, leftover process
groups, or residual scratch are failures. Reports and failed stores are retained.
See the retired Linux verification notes for adopting existing facts,
selecting other paths, and running the two-mount verifier with an ordinary Linux
binary and independently prepared oracle.

## Primary artifacts

Ordinary build, fresh import, and retained-store compatibility, under `.build/linux-correctness/`:

- `build.json` and `fixture.json`
- `run-import-wfg8by3v/import.json`, `verify.json`, and `process.json`
- `run-catalog-mpih1v49/catalog.json`
- `run-reader-le5w1efk/reader.json`
- `run-commands-jjiwhlg3/commands.json`
- `fuse-run/report.json`, `cleanup-verification.json`, and retained mount logs
- `run-verify-48bgp0nv/verify.json`
- `run-catalog-w14165up/catalog.json`
- `run-reader-g1mmtlns/reader.json`
- `run-commands-6effueiv/commands.json`
- Each phase's `process.json`, `stdout`, and `stderr`

Normal race suite, under `.build/correctness-verification/`:

- `regressions-normal-20260925-183835/result.json` and `output`

Historical development-build evidence remains under
`.build/correctness-verification/`, including `run-import-20260924-214348/`,
`run-catalog-20260924-215136/`, `run-reader-20260924-215217/`,
`run-commands-20260924-215515/`, and `fuse-run-final/final.json`.
Those older measurements are not represented as ordinary-build results. The
original 4x clone target was not achieved by the 96.48-second development import;
the user subsequently accepted that performance and requested integration.
