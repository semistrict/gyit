# Full-scan performance

The current target is native `git status` on the larger, 8,401-file fixture,
within 2× a normal checkout at the same revision through **direct virtio-fs**.
The earlier complete Python traversal benchmark remains available with its 4×
gate. Neither benchmark excludes tracked files or untracked-directory scanning.
The macOS 27 harness uses `VZCustomVirtioDevice`:

```text
Linux filesystem calls → Linux virtio-fs → VZ custom device → Go-FUSE protocol
                                                              ├─ gyit store
                                                              └─ normal host checkout
```

There is no FSKit mount in this path. With the original `--baseline custom`,
both devices use the same Swift adapter, Go-FUSE protocol implementation,
300-second positive metadata TTL, and read-only guest mount. `--baseline apple`
instead serves the ordinary checkout through Apple’s built-in directory sharing,
with Apple’s default cache policy. gyit leaves setup NOTICE entries and negative
names uncached.
Guest-local disk is measured separately; it is much faster than either share.

## Repeatable direct VM fixture

Requires Apple Silicon, macOS 27 and its SDK, Go, Swift, Python 3 and Lima.
Use a clean, self-contained `langchain-ai/open-swe` checkout at
`.testdata/scan-checkout`. Fixture data and build outputs are ignored and reused.
For example, create the checkout once:

```sh
mkdir -p .testdata
if [ ! -d .testdata/scan-checkout ]; then
  git clone https://github.com/langchain-ai/open-swe .testdata/scan-checkout
fi
```

Prepare a disposable Ubuntu ARM64 boot disk once:

```sh
limactl start -y --name=gyit-direct scripts/virtiofs-direct.yaml
python3 scripts/prepare_direct_virtiofs.py --vm gyit-direct
python3 scripts/build_virtiofs.py
```

The preparation script stops the named VM before making a private APFS clone of
its disk. It leaves that VM stopped. The direct harness boots the private copy
with 4 CPUs and 8 GiB RAM, attaching custom Virtio devices tagged `gyit` and
`checkout`. The guest mounts them at `/mnt/gyit` and `/mnt/checkout`.
It boots a minimal shell, without needing networking, a guest agent, or the
installed macOS app. A fixture lock prevents simultaneous use of the disk.

```sh
python3 scripts/benchmark_direct_virtiofs.py --label first --guest-disk
python3 scripts/benchmark_direct_virtiofs.py --label repeat
python3 scripts/benchmark_direct_virtiofs.py --label empty --empty-cache
```

Each invocation boots a fresh VM and backend, then powers it off. A timeout also
terminates the VM. JSON results and console transcripts go to `.build/virtiofs`.
The primary result exits nonzero if either scan mode exceeds 4×, the filesystem
results differ, native Git/content checks fail, or the VM fails to shut down.
The guest-local comparison is reported separately and does not set that gate.

Durable imported history lives in `.testdata/direct-data`; decoded cache lives
in `.testdata/direct-cache` with the normal global 4 GiB limit and disk reserve.
`--empty-cache` uses and then removes a new cache directory, keeping durable data.
Neither mode flushes the host OS page cache. Import readiness is checked before timing. Native-status mode also verifies
revisions before timing; the standalone scanner checks traversal digests without
running Git, and the harness verifies revisions afterward. In the default
`--scan-mode both`, the first stat pass follows the names scans. Use separate
boots with `--scan-mode names` and `--scan-mode stat` for independent first scans. These are fresh-mount measurements,
not claims of a completely cold machine. The optional guest-local checkout is
cloned once into the private VM disk and reused. Changing the source fixture
requires matching local remote/import fixtures; mismatched revisions fail.

`--metadata-ttl 0` is a diagnostic for server work with metadata caching disabled
on both sides, not the primary performance configuration.

## Scan and protocol correctness

`scripts/benchmark_scan.py` walks every worktree entry, excluding only the root
`.git` on both sides. It measures names/types and a traversal that stats every
entry, checks matching path/type/size digests, alternates run order, and checks
both first-pass and warm-median ratios. It runs without Git installed and works
on ordinary directories. It includes hidden files and never follows symlinks;
it does not read file contents. Sorting and digest construction are outside
the timer; enumeration, stat calls, and Python traversal bookkeeping are timed. Native `git log`,
`git status --porcelain=v1 --untracked-files=all`, and a file read are checked
through the mounted devices after timing. The baseline must be clean.

The actual Linux FUSE wire protocol is also exercised on the Darwin host:

```sh
go test -modfile=.build/virtiofs/virtiofs.mod -tags=gyit_virtiofs \
  ./internal/githubmount -run TestDirectVirtioProtocol -v
```

This checks Linux attribute layouts and error numbers, read-only behavior,
file reads, bounded READDIRPLUS pages, response overflow, and directory rewind.
The build script applies a Linux-wire-ABI overlay to an ignored copy of the
pinned Go-FUSE dependency. Source inputs are under `scripts/virtiofs-overlay`;
normal CLI/FSKit builds retain the unmodified upstream dependency.

## Current measurement and limitation

On the 1,990-entry / 189-directory open-swe fixture at
`89ff499efa11be6bcaf62fe9763b26805f2171b4`, directory-page reuse reduced a fresh
names scan from roughly 190 ms to 66–72 ms and a subsequent stat scan from
164 ms to 46–48 ms. The ordinary checkout through the same custom device took
roughly 53–62 ms and 48–50 ms respectively. One empty-decoded-cache run took
174 ms for the first names scan (3.08×); warm scans were approximately equal
to the shared checkout. These are local fixture results, not a Linux-scale claim.

The change reuses attributes already decoded in each 128-entry directory page
when replying to READDIRPLUS. Previously the bridge looked up each returned
name again from the repository root. It retains at most that page and one last
entry per open directory; it adds neither a whole-tree RAM cache nor durable
metadata. Publication generations prevent reuse of stale setup entries.
The existing content-addressed storage and atomic publication remain unchanged.

The custom-device runner is a benchmark harness, not an installed app feature.
Linux-scale repositories and sustained concurrent workloads remain to be measured.
The legacy `benchmark_virtiofs.py` / `virtiofs.yaml` experiment shares a macOS
FSKit mount through Apple's directory-sharing service. It is retained only as
a secondary comparison and is not the primary target.

## Native status on the larger fixture

Reuse the existing full-history medium repository fixture. These commands clone
only missing local fixture directories; no network clone is needed:

```sh
if [ ! -d .testdata/status-checkout ]; then
  git clone --quiet --no-hardlinks .testdata/medium-repo.git .testdata/status-checkout
fi
mkdir -p .testdata/status-remotes/acme
if [ ! -d .testdata/status-remotes/acme/large.git ]; then
  git clone --quiet --bare --shared .testdata/medium-repo.git .testdata/status-remotes/acme/large.git
fi
python3 scripts/build_virtiofs.py
python3 scripts/benchmark_direct_virtiofs.py --status --label large-status \
  --checkout .testdata/status-checkout --data .testdata/status-index-data \
  --cache .testdata/status-index-cache --remotes .testdata/status-remotes \
  --repository acme/large
```

Repeat with another label, then add `--empty-cache`. Run performance measurements
without overlapping compilation or regression suites. `--guest-disk` adds a
secondary comparison, after the shared comparison has already warmed gyit.
The synthetic `acme/large` path is only a local test transport alias; both trees
must resolve to the exact same Git commit.

`scripts/benchmark_status.py` times the actual Git executable with
`status --porcelain=v1 --untracked-files=all`. Both sides disable fsmonitor and
the untracked cache so every run performs a directory scan. Both use
`GIT_OPTIONAL_LOCKS=0`, since the primary mounts are read-only. Trace2 counters
must show at least one stat per tracked entry; native index flags are audited to
reject assume-unchanged or skip-worktree. Those audits happen **after** timing,
so they do not prewarm the first index read. Setup/full-history import and a
HEAD revision check happen before timing. Nonempty status output fails.

The first valid large baseline was 7.45s versus 339ms (22×), with 8,401 content
rehashes caused by missing index stat fields. The virtual index now contains
attributes matching the served files, including mount-specific inode/ownership
fields, without suppressing stat calls. Parent inodes retain immutable snapshot
and object identities, avoiding repeated root-to-file resolution. Four bounded
transport workers overlap independent requests on both comparison devices.

At `595cc91e8cbb1c2ca822d0311dcf12709410c582`, final isolated measurements
meet the 2× target against the same-transport checkout:

| Cache state at VM boot | First gyit status | First checkout status | First ratio | Warm median ratio |
| --- | ---: | ---: | ---: | ---: |
| Existing decoded cache, run 1 | 475 ms | 390 ms | 1.22× | 0.86× |
| Existing decoded cache, run 2 | 478 ms | 379 ms | 1.26× | 0.85× |
| Empty decoded cache, run 1 | 678 ms | 363 ms | 1.87× | 0.87× |
| Empty decoded cache, run 2 | 641 ms | 396 ms | 1.62× | 0.86× |
| Empty decoded cache, run 3 | 676 ms | 387 ms | 1.74× | 0.85× |

Each row starts a fresh VM and backend and performs five native status calls.
All 50 primary status calls statted every one of the 8,401 tracked entries;
none needed content rehashing, and every status was clean. Host OS caches were
retained. These are not measurements of an entirely cold machine or remote S3.
The JSON/transcript labels are `large-final-cached-1`, `large-final-cached-2`,
`large-cold-cacheparallel`, `large-empty-confirm-1`, and `large-empty-confirm-2`.

The final cold-path improvements validate decoded trees once before admitting
them to the bounded disk cache, then look up individual names without allocating
all siblings. Independent cache files now write concurrently; in-flight writes
reserve bytes and entry slots within the same global limit. The process owner
lock remains held until both live mappings and pending writes have finished.
Neither change adds a separate RAM metadata cache or a new durable layout.

A complete directory walk on the same larger fixture (`large-final-walk`)
matched path/type/size digests: first names scan **301 / 270 ms = 1.12×**;
subsequent stat scan **206 / 268 ms = 0.77×**. Warm ratios were 0.77–0.78×.

**Guest-local disk is still substantially faster.** After fixing the benchmark's
clock/index setup, it took **10.3 ms first / 8.2 ms warm**, versus **240 ms /
234 ms** for the already-warm gyit mount: 23.3× first and 28.7× warm. This is a
separate comparison, not a claim of meeting 2× local-disk performance.
The minimal VM previously restarted at the Unix epoch; the reused local index
had timestamps that Git treated as racy, causing redundant per-file work.
The runner now sets the guest clock, settles a newly cloned checkout, and uses
`git update-index --refresh --force-write-index` before the optional local
comparison. This does not set assume-unchanged or skip-worktree; Trace2 confirms
exactly 8,401 stats per timed local status, just as in the primary comparison.

For CPU diagnostics, set `GYIT_VIRTIO_CPU_PROFILE` to an absolute output file
when launching the benchmark, then inspect it with `go tool pprof`. Profiled
runs are diagnostic and should not establish the performance gate. The runner
is a standalone benchmark harness; this work does not install or restart the
macOS app.

## Non-Git scans against Apple directory sharing

`--baseline apple` serves the ordinary host checkout with
`VZVirtioFileSystemDeviceConfiguration`, `VZSingleDirectoryShare`, and a read-only
`VZSharedDirectory`. That baseline does not use our Swift queue adapter or
Go-FUSE loopback. gyit still uses the custom Virtio device in the same VM.
Apple's default caching policy is left intact; our metadata TTL remains 300s.

The standalone scanner requires no Git installation or `.git` metadata:

```sh
python3 scripts/benchmark_scan.py --mount /path/to/mount \
  --checkout /path/to/reference --mode stat --max-ratio 2 --output scan.json
python3 -m unittest discover -s scripts -p test_benchmark_scan.py
```

For the repeatable VM comparison, run each mode in its own fresh VM:

```sh
python3 scripts/benchmark_direct_virtiofs.py --baseline apple \
  --scan-mode stat --label apple-stat \
  --checkout .testdata/status-checkout --data .testdata/status-index-data \
  --cache .testdata/status-index-cache --remotes .testdata/status-remotes \
  --repository acme/large
```

Repeat with `--scan-mode names`, and add `--empty-cache` for each mode's empty
cache measurement. Each invocation makes five scans, alternating order, and
rejects mismatched path/type/size digests. Setup is outside the timer. The VM
harness checks Git revision/content correctness afterward; the scanner itself
never runs Git. The legacy generic traversal gate is 4×; the ratios below must
be evaluated explicitly against the newer 2× target.

On the 9,358-entry tree (8,401 files/symlinks and 957 directories), two independent
VM boots per condition produced:

| Mode | gyit cache at boot | First gyit scan | First Apple scan | First ratio | Warm median ratio |
| --- | --- | ---: | ---: | ---: | ---: |
| Names/types | Existing | 326–330 ms | 212–220 ms | 1.50–1.54× | 1.74–1.79× |
| Names + stat every entry | Existing | 321–329 ms | 255–259 ms | 1.26–1.27× | 1.52–1.54× |
| Names/types | Empty | 682–954 ms | 190–210 ms | 3.59–4.54× | 1.75–1.78× |
| Names + stat every entry | Empty | 700–725 ms | 291–300 ms | 2.33–2.49× | 1.51–1.54× |

**Empty-cache scans miss 2× against Apple.** The earlier completed 2× native
status result was against our own loopback adapter, not Apple's implementation.
The spread in first-pass empty-cache directory traversal is retained here rather
than selecting the faster sample. Host OS caches were not cleared. Results and
transcripts are named `apple-{names,stat}-{cached,empty}-{1,2}` under
`.build/virtiofs`. Every timing was below one second in these eight runs.

The initial `apple-status-cached-1` experiment is **not a valid native-status
performance baseline**: Apple exposes stat attributes that differ from those in
the host checkout's index, so Git rehashed all 8,401 files. A future native-status
comparison must first create an index with matching guest-visible stat data.
The non-Git scans above are unaffected by index metadata.

## Backend isolation

The opt-in Go benchmark separates serving by directory identity from resolving
each path, with a third variant that resolves every entry for stat. It imports
the existing local fixture once into a separate reusable store:

```sh
GYIT_SCAN_SOURCE="$PWD/.testdata/scan-checkout" \
GYIT_SCAN_DATA="$PWD/.testdata/scan-backend" \
go test ./internal/repo -run '^$' -bench BenchmarkSnapshotScan -benchmem
```

Use a new fixture data directory when the source revision changes. Import is
outside the timed scan. Backend timings alone do not establish mounted scan
performance, and warm results alone do not satisfy the goal.
