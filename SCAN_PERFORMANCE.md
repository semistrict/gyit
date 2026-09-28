# Full-scan performance

## GCE `find` scan and live cache coherence (2026-09-27)

The latest scanner times the actual `find . -name DOES_NOT_EXIST` command on
9,358 entries (957 directories) at revision
`595cc91e8cbb1c2ca822d0311dcf12709410c582`. Both worktrees have no root `.git`.
After all timed passes, full path/type/size digests must match. A newly created
ext4 image supplies the guest-local disk baseline; its empty `lost+found` is
removed before boot. All scans use the same nested-KVM guest and executable.

Directory-listing reuse and following prepared directory pointers avoid repeated
READDIR requests and global-index lookups. No new durable storage format or
unbounded RAM cache was added. The comparison still uses native GCS.

| Policy | First gyit find | Ordinary virtio-fs | First ratio | Warm gyit / virtio-fs |
|---|---:|---:|---:|---:|
| Before changes, immutable attribute TTL | 669 ms | 348 ms | 1.92× | 589 / 91 ms = 6.45× |
| Fixed snapshot, 300s attribute TTL, repeat 1 | 638 ms | 358 ms | 1.78× | 105 / 86 ms = 1.22× |
| Fixed snapshot, 300s attribute TTL, repeat 2 | 643 ms | 400 ms | 1.61× | 110 / 92 ms = 1.20× |
| Immediate revalidation, zero attribute TTL | 1,809 ms | 359 ms | 5.04× | 1,697 / 89 ms = 19.07× |
| One-second TTL, stateless directory opens, back-to-back scans | 570 ms | 382 ms | 1.49× | 12 / 85 ms = 0.14× |
| One-second TTL, stateless directory opens, 1.1s pause between scans | 585 ms | 335 ms | 1.75× | 137 / 90 ms = 1.52× |

Every row starts a fresh VM/backend with an empty decoded cache. Five passes
alternate baseline order. Setup/import/VM boot are outside the scan timer;
host page caches remain populated. virtiofsd uses `--cache=always`. Counters for
startup, traversal, and the post-timing content check together are eight GCS
reads and 1,747,999 transferred bytes. The one-second policy meets the 2×
virtio-fs criterion even after attributes expire between scans. Guest-local ext4
took 126 ms first and 13 ms warm in that run; gyit does not meet the alternative
4× local-disk target. This is a scan benchmark, not an import or Git benchmark.

Linux `fs/fuse/readdir.c:fuse_readdir_cached` revalidates directory mtime with
AUTO_INVAL_DATA and resets cached listings when it changes. gyit now exposes
publication generations as directory mtimes and refreshes retained snapshot
entries on generation changes. Regular `/dev/fuse` mounts also send inode and
entry invalidation notifications off the publishing request goroutine. Linux
virtio-fs does not negotiate the optional Virtio notification queue, so live
virtio-fs attributes expire after at most one second, the accepted freshness
delay. Longer attribute TTLs are only allowed for immutable prepared snapshots.

The stock kernel also supports `FUSE_NO_OPENDIR_SUPPORT`: after one ENOSYS
response it omits OPENDIR and RELEASEDIR while keeping directory caching and
mtime revalidation. The virtio-fs adapter enables this and creates temporary
Go-FUSE handles only inside READDIR/READDIRPLUS. A two-name continuation hint
per live directory inode avoids replaying every earlier entry between requests;
rewinds or concurrent cursor misses use the existing seek fallback. This removes
two guest round trips per directory without extending cache lifetimes. No
custom kernel or additional Go-FUSE fork is needed for this optimization.

`TestMountedBackgroundPublication` checks cached NOTICE replacement, additions,
deletions, changed contents, and old open handles on real Linux FUSE.
`scripts/verify_virtiofs_updates.py` separately checks real nested-KVM virtio-fs:
it warms root/subdirectory caches, publishes a local upstream commit, invokes
`gyit update` inside the guest, and verifies refreshed listings and a preserved
open handle to the removed file. It also checks changed file size and contents,
and fails unless the refreshed state appears within 1.25 seconds (one-second
TTL plus scheduling tolerance). This works without host notifications.
The helper removes all of its guest processes and temporary fixture data.

Repeat the actual find comparison with:

```sh
sudo python3 scripts/benchmark_nested_kvm.py \
  --server /path/to/gyit-vhost --store gs://BUCKET/REPOSITORY \
  --sha FULL_SHA --checkout /path/to/clean-worktree \
  --cache /path/to/cache --kernel /boot/vmlinuz-VERSION \
  --initrd /boot/initrd.img-VERSION --output /path/to/results \
  --mode find --empty-cache --guest-disk
```

The default one-second attribute TTL tests the live freshness policy. Add
`--pause-between-runs 1.1` to expire it before each later scan; pauses are outside
the timer. Use `--metadata-ttl 0` for immediate revalidation or
`--metadata-ttl 300` only for the fixed-snapshot comparison. This benchmark
rejects mismatched trees and fails unless both first and warm scans meet 2×
virtio-fs, or both meet 4× guest-local disk. Evidence is in
`.build/find-performance/`, including the unsuccessful live-policy measurement.


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


## GCE nested KVM and native GCS (2026-09-27)

This experiment uses a native GCS adapter (`cloud.google.com/go/storage`),
Application Default Credentials from the VM's read-only service account, and
GCS generation preconditions for atomic publication. The real-bucket adapter
check exercises 16 concurrent CAS writers across two clients (exactly one wins),
create-only publication, missing objects, empty objects, resumable uploads,
CRC32C and cross-chunk byte ranges. No S3 compatibility endpoint is involved.

Configuration:

- GCE `n2-standard-8`, Intel Cascade Lake, Ubuntu 24.04, nested virtualization
  enabled, 100 GiB SSD boot disk, `us-east4-a`; private GCS bucket in `us-east4`.
- Host kernel `7.0.0-1011-gcp`; QEMU 8.2.2 with `-enable-kvm -cpu host`;
  guest kernel `6.8.0-142-generic`, four vCPUs and 8 GiB RAM.
- Direct Go-FUSE vhost-user server versus virtiofsd 1.10.0 sharing a normal
  checkout. Both run on the same host and serve the same guest read-only.
- Go server uses the pure-Go Linux amd64 build. Its metadata TTL is 300 seconds;
  the baseline uses `virtiofsd --cache=always`. Each run starts a fresh guest and
  server; host OS caches remain intact.
- Same large fixture at `595cc91e8cbb1c2ca822d0311dcf12709410c582`:
  9,358 entries, including 957 directories. Five alternating-order pairs per
  run; identical traversal digests and a post-timing file-content check required.
- Durable repository data is in GCS. The server has only a 4 GiB bounded decoded
  cache locally (about 28 MiB populated by this scan), plus process state.
  Import, upload, server startup and VM boot are outside the scan timer.

| Decoded cache before run | Scan | gyit first scan | virtiofsd first scan | First ratio | Warm ratio |
|---|---|---:|---:|---:|---:|
| Existing | names | 0.604–0.625 s | 0.336–0.395 s | 1.53–1.86× | 4.85–5.22× |
| Existing | stat | 0.654–0.662 s | 0.400–0.439 s | 1.51–1.63× | 4.04–4.31× |
| Empty | names | 69.918–71.801 s | 0.337–0.359 s | 199.73–207.60× | 5.09–5.42× |
| Empty | stat | 70.763–72.210 s | 0.475–0.501 s | 144.09–148.86× | 3.25–4.31× |

Two fresh-VM repeats per row: **80 matching full traversals** across eight VMs.
Every cold first scan exceeds one second. Server startup takes 0.20–0.50 seconds,
separately from scan timing.

Cold scans make 3,120 adapter Get calls and transfer 17.4 MB, including startup
and the post-timing content check. Existing-cache runs make only two startup
Get calls (0.80 MB), with no further reads from GCS during traversal. These are
adapter-level counts; SDK retries are not counted separately. Request latency
from thousands of small range reads dominates the cold result. The remaining
warm gap exists even when traversal makes no GCS requests. **Neither condition
satisfies the full 2× goal.**

The benchmark also exposed an upstream transport limitation: Go-FUSE's
experimental vhost-user path assumed each READ payload was one contiguous guest
buffer. Real Linux guests split it across pages. The harness now uses the shared
flat protocol adapter to gather/scatter those buffers; its regression test covers
fragmented headers, multi-page content, offsets and EOF. Earlier runs with read
errors or AppleDouble files in the copied baseline are excluded. The runner
terminates the diskless VM after the guest's completion marker because the
experimental backend can stall QEMU during device teardown.

This is a scan benchmark, not an import-throughput result, native `git status`
measurement or comparison against Apple's macOS implementation. The ordinary
GitHub auto-import app still uses local durable storage. Reproduction and cleanup
instructions are in [GCE_BENCHMARK.md](scripts/GCE_BENCHMARK.md). Raw results are
retained locally under `.build/gce/results/`.

The test VM, boot disk, GCS bucket and dedicated service account were deleted
after measurement and their absence verified. No cloud test resources remain.

## Progressive format on GCE/GCS (2026-09-27)

The progressive implementation was deployed to a new disposable host using the
same N2/Cascade Lake configuration, nested QEMU/KVM guest, native GCS adapter and
9,358-entry / 957-directory fixture at
`595cc91e8cbb1c2ca822d0311dcf12709410c582` as the earlier benchmark.
The fixture contained one complete snapshot; acquisition from GitHub and full
history import were outside this scan experiment.

Direct publication from the packed snapshot fixture to GCS took **28.62 seconds**:
0.70 seconds for packs/index publication and 27.91 seconds for filesystem
metadata. Both publication as a whole and metadata preparation exceed one second.
A separate GCE integration suite checked default-branch/branch/tag updates, SHA
pinning, failed-update preservation and log parity with Git against GCS. That
suite took 11.64 seconds in total; it is not a per-command latency measurement.

After publication, the VM service account was reduced to bucket objectViewer.
The independent vhost reader received the GCS prefix and commit ID, without an
acquisition repository or local durable-store copy. Its decoded cache was
separate from the writer's. Server startup took 0.10–0.20 seconds.

| Reader cache before guest | Scan | First gyit scan | First ordinary checkout | First ratio | Warm median ratio |
|---|---|---:|---:|---:|---:|
| Empty persistent cache | stat | 24.901 s | 0.402 s | 61.89× | 3.87× |
| Existing persistent cache | names | 0.530 s | 0.347 s | 1.53× | 5.23× |
| Empty isolated cache | stat | 24.565 s | 0.477 s | 51.51× | 3.78× |
| Empty isolated cache | names | 24.510 s | 0.384 s | 63.81× | 5.18× |

Every cold scan exceeds one second. Warm gyit medians were 0.42 seconds for
names and 0.44–0.45 seconds for stat, versus 0.08 and 0.12 seconds respectively
for the ordinary checkout through virtiofsd. **The 2× scan target remains unmet.**
The first row's output directory is named `stat-cached`, but its recorded
`cache_initially_empty` is true; it must not be reported as an existing-cache run.

Four fresh guests each ran five alternating-order pairs: **40 matching
traversals**, including path/type digests in names mode and file/symlink sizes
in stat mode, with a file-content comparison after each guest's scans. There
were no GCS adapter errors. Each cold run made 1,024 Get calls and returned
807,294 bytes (including startup and the content check), with 23.5–23.9 seconds
of aggregate request latency. The existing-cache run made one 120-byte startup
Get, with no subsequent GCS reads during traversal. Fewer small metadata reads
reduce cold time from the prior 70–72 seconds to about 25 seconds, but serial
remote request latency still dominates. Warm transport overhead is also still
present. No scan optimization was claimed from deployment alone.

Raw results and the cleanup record are under `.build/gce-progressive/`.
The command/update integration suite runs on the host; this benchmark does not
implement or claim guest-to-host CLI control-socket forwarding.

The disposable GCE VM, boot disk, GCS bucket and dedicated service account were
removed after collecting results; all four were verified absent.

## Cold-scan diagnosis on GCE/GCS (2026-09-27)

The dominant problem is request granularity. The writer packs directory pages
into 256 KiB objects, but `Progressive.directoryPage` issues a separate range
Get for every compressed page. Sequential traversal pays one network round trip
per page. Batching writes alone did not batch reads.

A new retained N2 standard-8 instance in us-east4-a used the same prepared
9,358-entry snapshot, native GCS adapter, and nested-KVM benchmark. Every reader
started with an empty decoded cache. The direct storage probe recursively calls
the real `Snapshot.ReadDir` and hashes paths, modes and sizes. It counts 958
directories including the root (the guest scan reports 957 descendants).

| Direct repository traversal | Scan time | Directory Gets | Directory bytes |
|---|---:|---:|---:|
| Current GCS range reader | 21.234 s | 1,010 | 557,036 |
| Diagnostic whole-directory-container reads | 0.360 s | 3 | 557,036 |
| Diagnostic whole-directory and index-container reads | 0.209 s | 3 | 557,036 |
| Current range reader against a local copy of the same store | 0.166 s | 1,010 local reads | 557,036 |

All four traversals produced the same digest. In the unmodified GCS reader,
502, 444 and 64 requests addressed just three directory objects. Those reads
consumed 20.718 seconds; median request latency was 20.15 ms, p95 29.32 ms.
Eight additional index reads cost 0.179 seconds. These measurements exclude
opening the snapshot, which took 0.208 seconds for the original GCS reader.
Fetching the ordinary index containers wholesale also transfers extra unrelated
index bytes: opening went from 26,406 index bytes to 1,157,273 bytes. That variant
is a diagnostic control, not the proposed production policy.

The whole-directory-container experiment was then run through the actual
virtio-fs adapter, in a fresh diskless QEMU/KVM guest for each variant:

| Backend / reader | First stat scan | Ordinary checkout through virtiofsd | First ratio | Warm ratio | Store Gets |
|---|---:|---:|---:|---:|---:|
| GCS, current range reader | 23.032 s | 0.782 s | 29.44× | 3.83× | 1,024 |
| GCS, diagnostic directory-container reads | 0.964 s | 0.423 s | 2.28× | 4.18× | 17 |
| Local store, current range reader | 0.734 s | 0.402 s | 1.83× | 4.22× | 1,024 |

All three transferred 806,824 Store bytes, including snapshot startup and the
post-scan content comparison. Aggregate Store read time was 21.993 seconds,
0.448 seconds and 0.0165 seconds respectively. Each guest ran five alternating
pairs of traversals: all 30 path/type/size digests matched, and the file-content
comparison succeeded in each guest. First-run baseline timings vary between
fresh guests; these are individual causal experiments, not latency percentiles.

The experiment changes only how directory-object ranges are fetched. It uses
a diagnostic-only compressed RAM buffer capped at 32 MiB; this fixture used
557,036 bytes. It is not installed in the application and is not a proposed
second production cache tier. No production reader behavior changed during this
investigation.

The next production change should fetch a bounded directory metadata container
once and populate the existing globally bounded **uncompressed** disk cache
with its decoded pages. Concurrent requests should share the container fetch;
the temporary compressed transfer buffer should then be discarded. Decoding all
pages requires bounded framing/validation of the concatenated page records.
Keep immutable container/page references and incremental publication; a complete
repository download or a monolithic per-snapshot metadata object is unnecessary.
Following already-stored child directory references could avoid the remaining
index requests, but those are a secondary cost in this experiment.

The 2× target is still unmet with remote storage, and even the local-store warm
mount remains approximately 4× slower than virtiofsd. That residual cost needs a
separate transport/local-processing profile; it does not explain the original
20+ seconds of cold latency.

Raw probes, per-request traces, diagnostic sources and guest results are in
`.build/gce-cold-investigation/`. The direct probe source is
`probe/main.go`; the modified benchmark-only server is in `vhost-probe/`.
`run-probes.sh` and `run-kvm.sh` record the invocations. The local control retains
a separate durable store copy solely to isolate network cost; the GCS variants
still read from GCS with independent empty decoded caches.

At the user's request, these resources are **retained**, with no automatic VM
termination timer:

- Project: `echophase-connectome-test`
- Zone: `us-east4-a`
- VM: `gyit-cold-d5f2c4`
- Bucket: `gs://echophase-connectome-test-gyit-cold-d5f2c4`
- Service account: `gyit-cold-d5f2c4@echophase-connectome-test.iam.gserviceaccount.com`

The VM's prepared fixture, durable-store control copy, binaries and results remain
under `/home/ramon/`; nested guest results are under `/var/tmp/gyit-cold-results/`.
The previous section's deletion record refers to the earlier instance only.

## Production container reader (2026-09-27)

`Progressive.directoryPage` now reads a whole immutable directory container on
its first cache miss. It walks the existing Zstandard frame boundaries, checks
and decodes the frames, and populates the same bounded uncompressed page cache.
There is no storage-format change or retained compressed cache. Concurrent
misses for different pages in the same container share a fetch. Scratch decoding
is limited to 4 MiB per container, with at most eight active cache loaders;
containers with unusually high expansion fall back to single-page reads. A full
or disabled decoded cache still returns the requested page correctly.

The production vhost binary was tested against the **existing** GCS publication
on the retained VM, with fresh guests and empty decoded caches:

| Scan | First gyit | Ordinary checkout through virtiofsd | First ratio | Warm median ratio | Store Gets |
|---|---:|---:|---:|---:|---:|
| stat | 0.886 s | 0.441 s | 2.01× | 4.13× | 17 |
| names | 0.871 s | 0.332 s | 2.62× | 4.60× | 17 |

The previous unmodified-reader stat scan was 23.032 seconds and 1,024 Gets.
The production stat scan is about 26× faster. Both new runs transferred 806,824
Store bytes, including startup and the post-scan file comparison. All 20 paired
traversals matched their checkout path/type (and stat-mode size) digests; both
file-content comparisons succeeded. The 2× target is **not yet met**. Warm
transport/local-processing overhead remains outside this container-read change.

Regression tests cover one fetch per container, persistent decoded-cache reuse,
concurrent readers of different pages, zero/tiny caches, corrupt and truncated
frames, invalid references, multiple Zstandard blocks, cancellation and the
high-expansion fallback. `BenchmarkProgressiveColdDirectoryScan` exercises 512
pages with a fresh real disk cache: 68.4 ms/op and 1 container Get/op over three
iterations on the development machine. It is a local reader microbenchmark,
not an object-store latency result.

The updated GCE binary is `/home/ramon/gyit-vhost-production`. The reproducible
runner is `.build/gce-cold-investigation/run-production.sh`, with results in
`production-kvm.tar.gz` and `/var/tmp/gyit-cold-results/production-{stat,names}`
on the VM. The cloud resources remain retained. This turn did not replace the
installed macOS application.
