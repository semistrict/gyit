# Performance acceptance target

For every operation, compare gyit against the fastest equivalent native Git
workflow, with a target of no more than **2× its elapsed time**. Compare the same
revision, requested output, and starting data availability; report cold and warm
results separately. Include acquisition time when the requested data is missing.
Use shallow and partial fetches when they satisfy the operation instead of
comparing a small request against an unnecessary full clone. Record commands,
environment, bytes transferred where measurable, and output equivalence.
Report timeouts as incomplete measurements, not completed timings. Historical
targets and results below do not establish compliance with this target.

## Accepted Linux ten-commit log result (September 27, 2026)

The requested workload was changed to `gyit log --oneline -n 10`. With the real
GCS-backed GitHub automount in a nested-KVM guest, the final three measured cold
runs took **0.427, 0.414, and 0.418 seconds**. Paired native Git initialization,
commit-only depth-ten fetch, and ten-line log took **0.165, 0.183, and 0.174
seconds**, respectively: **2.58×, 2.27×, and 2.40×**. All ten output lines matched
Git byte-for-byte. These results were explicitly accepted; they do **not** meet
the standing 2× target. No further performance tuning is required for this task.

Acquisition is included: each run restores the original snapshot-only manifest
in an isolated test prefix, starts a fresh backend/cache/guest, and fetches
missing commits during the timed CLI command. The already mounted snapshot and
background history worker are retained in the workload. The baseline runs on
the GCE host, while gyit runs in the nested guest. The regional GCS bucket and
host are both in `us-east4` (host zone `us-east4-a`). Upstream server caches are
uncontrolled. Successful cold results are separate from earlier timed-out runs.

Warm CLI runs remain roughly **58–93 ms**, versus **3.4–3.5 ms** for host Git.
Guest CLI startup alone measured **39–70 ms** in diagnostic runs. This overhead
remains unresolved; the accepted cold result must not be presented as universal
2× performance.

The fixes bound foreground acquisition to commit-only shallow batches, separate
its Git acquisition lane from background fetching, read grouped object-index
pages through the existing bounded uncompressed disk cache, warm decoded small
commit packs during import, and overlap immutable uploads before atomic HEAD
publication. GCS returns the assigned CAS token without an extra HEAD read.
A separate control-file bug caused Go's poller to wait indefinitely after an
EAGAIN between response frames; the client now owns retries, and a real FUSE
regression test verifies delivery across a delayed frame.

Repeat the end-to-end check with `scripts/benchmark_history_nested_kvm.py` on the
retained host, using an isolated copy of the depth-one store and restoring its
seed HEAD only while that benchmark server is stopped. The script retains the
strict 2× gate and therefore exits nonzero for these accepted results. It records
full commands, timings, exact-output hashes, warm results, and startup probes.
Artifacts are under `.build/gce-cold-investigation/git-history-comparison/`,
notably `mounted4.txt`, `mounted5.txt`, `mounted6.txt`, and `seed-head.pb`.
Remote copies are under `/home/ramon/history-runs/trial4` through `trial6`.
The interactive mount uses a separate namespace and remains available.

## Linux three-commit history comparison (September 27, 2026)

On the retained GCE host, two fresh bare repositories fetched the mounted Linux
revision with `git fetch --depth=3 --filter=tree:0 --no-tags origin <revision>`.
The revision was `72d3fcf802c45d00b300f25b848a93c3a2bd7c7e`.
Fetch plus `git log --oneline -n 3 FETCH_HEAD` took **0.183 s** and **0.148 s**;
initialization and remote configuration add about 4 ms. Both returned the same
three lines and stored four commit objects, with about 3 KiB of pack/index data.
This is stored size, not a measured network-byte count. Starting instead with
a depth-one commit-only repository, `--deepen=2 --filter=tree:0` plus the log took
**0.498 s**, excluding the initial depth-one acquisition.

The mounted gyit three-commit request was interrupted after approximately two
minutes without completing. It therefore fails the 2× target by a wide margin;
this is an interrupted observation, not a completed timing or an output-parity
check. Native Git ran on the host; gyit ran through the nested guest mount and
GCS-backed host service. These are two trials, with upstream caches uncontrolled.
The native workflow establishes a roughly **0.30–0.37 s** initial budget for this
small cold-history request, subject to repeated end-to-end measurement.

The foreground gyit object-demand fetch currently uses `--filter=blob:none`
without a depth bound, even for missing commit objects. That requests trees and
can traverse much more ancestry than the requested log needs. Background history
ingestion also shares its acquisition lock. A longer command timeout does not
solve this acquisition problem.

Raw commands and timings are retained in
`.build/gce-cold-investigation/git-history-comparison/output.jsonl`; the bounded
experiment script is `compare.py` in that directory. Remote artifacts remain in
`/home/ramon/git-history-comparison-1790550657` on the retained test host.

# Historical import result

On September 25, 2026, the ordinary `gyit import` executable imported the complete
cached Linux source in **90.896101 s**, with the existing demo VM running normally.
It published all **11,839,497 non-tag objects** in stable format 8. Its persistent
local store occupies **20,433,584,926 bytes** across **2,102 files**. This includes
copied source data and metadata; the narrower upload counter does not describe
total store size. Independent persisted-store verification ran afterward and
took 9.747 seconds, outside the import timing.

The latest comparable local clone without hardlinks took **10.525487 s**. The
ordinary command is approximately **8.64x** that baseline. It satisfies the later
10x time target, not the original 4x target. It uses the default command build,
automatic archive selection, and no importer feature flags or source overlays.
The run had a 180-second process-group kill deadline. Source files remained
unchanged, staging was cleaned up, and no child process group remained.

The preceding development build measured 96.479487 seconds and 20,486,472,961
store bytes. Those results are historical; the current measurement above comes
from the integrated executable. One run of each does not establish a general
performance improvement. Current artifacts are under
`.build/linux-correctness/run-import-wfg8by3v/`; the retained store is
`.testdata/linux-store`.

Correctness results, scope, repeat commands, and operations exceeding one second
are recorded in [CORRECTNESS.md](CORRECTNESS.md). Earlier targets and experiments
below remain historical evidence. Do not suspend the VM or resume speed tuning
as part of this verification task.

## Host contention check

An idle Linux guest's virtualization process was consuming approximately five
host CPU cores. Only that demo VM was temporarily suspended during the following
timed operations, with an independent resume watchdog and immediate restoration
after each operation. The guest, mount service, and terminal session were retained.

That method has since been disabled: during a later, longer v5.4 test the OS
killed the suspended VM process. The existing VM disk and demo were recovered,
and the two-window terminal session was recreated; its prior in-memory state was
lost. The interrupted comparison was discarded. Do not suspend the VM to repeat
these measurements. The recovered VM's idle host CPU use is near zero.

| Operation | Controlled host time | Scope |
| --- | ---: | --- |
| Local Git clone, checkout included, no hardlinks or alternates | 11.289599 s | Full cached source; clone removed afterward |
| Import intact history through v4.4 | 59.633314 s | 4,476,313 objects, 53.37 GB expanded; 384 file reads checked against Git |
| Native reachability enumeration | 53.965493 s | All 11,834,852 commit-tip reachable objects; no conversion or publication |

The subsequent clone with the recovered VM running took **10.525487 s**, including
checkout, with no hardlinks or alternates. Its report is
`.build/live-host/clone/timings.tsv`; the private clone was removed afterward.

All exceed 1 s. The bounded import uses original source packing for traversal and
body reads, but a private bounded inventory. It is not a full-import measurement
or an upper bound. The earlier roughly 70 s v4.4 result had the VM running, so the
difference is a host-load comparison, not a code speedup. Reports and the repeat
baseline are under `.build/quiet-host/`.

# Import compression work sharing

A retained dispatch trace of full Linux history contains 138.24 GB of blob
content. One of 15 lanes receives 19.67 GB (2.134 times the mean), including
11.31 GB from versions of `MAINTAINERS`. The importer now shares stable encoding
work from overloaded lanes while preserving serial compression/dependency choices.

Measurements below used the cached original Git pack on the same host, 15 source
workers, depth-one deltas, and four candidates. All listed operations exceed 1 s.
The replay times include blob ingestion, compression and durable pack writes;
exhaustive read verification runs afterward and is excluded from replay timing.

| Bounded workload | Serial | Work sharing | Qualification |
| --- | ---: | ---: | --- |
| Every eighth `MAINTAINERS` version: 2,715 blobs, 1.41 GB | 8.70 s | 2.44 s | One file's history, not complete import |
| Mixed replay: 101,061 blobs, 6.43 GB, all 635 blobs over 1 MiB | 7.93 s | 4.96 s | Every blob and compressed dependency verified |
| Same mixed replay, reversed run order | 7.49 s | 4.90 s | Same encoded-payload signature |
| Complete v3.4 history, forward run order | 32.52 s | 34.41 s | Pool disabled on balanced lanes |
| Complete v3.4 history, reversed run order | 32.99 s | 32.23 s | Run-order effect; no established whole-import gain |
| Recent shallow slice: 63,207 commits, 648,389 objects, 11.61 GB | 18.76 s | 18.52 s | History indexing dominates; cannot extrapolate full history |

The intact-history comparisons verify all 765,673 chunks/dependency chains and
301,533 history records; the recent slice verifies all 259,207 chunks and 63,454
history records. Each also checks 384 cold file reads against Git. These timings
precede final constructor/estimate hardening; final validation is recorded in
`.build/speculative-encode/`. No new full import has run, and these component gains
do not establish either a full-import deadline or a universal speedup.

# Initial measurements

The Linux snapshot measurements below used the original JSON metadata format.
The format at that point used protobuf with packed index pages (version 3).
The format used by those later conversion measurements was version 6; current
archive imports write version 8.
Historical format-2 full-history results are retained below as a baseline.

Measured on 2026-09-19 using the existing shallow Linux checkout at
`40288c9206c17eb66a603262e06a58d300d0f279`. It contains **one commit**,
101,670 reachable objects, and 95,391 unique blobs. This is a Linux-sized working
tree test, **not a full Linux-history benchmark**.

The source contained 1,641,390,137 uncompressed object bytes. Output packs used
335,314,579 bytes. A complete local object store, including catalog metadata,
occupied approximately 369 MiB on disk.

| Operation | Measurement | Scope |
| --- | --- | --- |
| Initial import to local filesystem | 31.95 s | macOS ARM64; fsync on object writes |
| Importer peak RSS, local run | 458,473,472 bytes | Includes mapped staging pages; not mount memory |
| Initial import to local S3 emulator | 15.06 s | RustFS in the existing local container runtime |
| Cold snapshot open plus root listing | 6.53 ms | 41 entries, 6 GETs, 68,637 bytes, **zero pack reads** |
| First file prefix read | 5 GETs, 78,686 bytes | Includes metadata; one compressed data chunk |
| Warm checkout open | 1.54 ms | 1 HEAD GET, 155 bytes; commit pages already cached |
| No-op incremental import | 90.86 ms | No new Git objects or data packs |
| Actual Linux FUSE root `ls -l` | 0.04 s | Local store through the VM shared filesystem; after the readiness lookup |
| FUSE process RSS after root listing and one nested source read | 14,256 KiB | ARM64 Linux; 8 MiB cache budget; not a full-tree traversal |

These timings are development observations, not service-level guarantees. A
remote object store adds network latency to catalog traversal. The first file
read's original timing included the Git comparison process, so only its I/O
counts are recorded above; the reproducible probe now times the read separately.

Real FUSE integration was tested in the existing ARM64 Linux VM with
`fusermount3` and `/dev/fuse`. The test uses two simultaneous read-only mounts,
publishes a new generation, switches one mount, checks stable path inode numbers,
checks fresh bytes and directory entries, and checks that an already-open file
handle and the second mount retain old data.

Before production claims, measure full Linux history, repeated large updates,
cold remote-region root listings, directory-heavy builds, total mount RSS under
many concurrent handles, and failure recovery. In particular, storage amplification
without cross-version deltas and remote upload throughput remain open.

## Cached medium repository: protobuf format 2

The repeatable integration test was run against a complete bare clone with HEAD
`595cc91e8cbb1c2ca822d0311dcf12709410c582`. The protobuf run reused the existing
ignored clone without cloning or fetching, and imported into a fresh temporary
local filesystem store.

| Measurement | Result |
| --- | --- |
| Commits | 40,985 |
| Imported objects | 477,069 |
| Unique blobs | 183,627 |
| Uncompressed object bytes | 11,903,666,409 |
| Compressed data-pack bytes, excluding metadata | 2,457,647,576 |
| Native Git clone on disk | Approximately 640 MiB |
| Full import | 16 min 1.16 s |
| Cold open plus root listing | 2.61 ms; 45 entries; 8 GETs; 345,142 bytes; no pack reads |
| Unchanged reimport | 398.03 ms; no new objects or data packs |
| Entire test, including verification and temporary-store cleanup | 980.34 s |

The test compared all 8,401 HEAD file paths, modes, and blob IDs against Git,
verified complete Git hashes for 17 deterministically selected files, and opened
a historical commit without fetching file data. The cached clone was retained;
the temporary imported store was removed.

The original JSON run of the same fixture took 15 min 8.81 s to import. These
were sequential development runs under uncontrolled host load; they do not
establish a serialization-speed comparison. Sampling the protobuf run showed
the long publication phase dominated by filesystem durability flushes while
writing index pages. Switching serialization alone does not remove that cost.

Storage amplification also remains material: the compressed data packs alone
are much larger than the native Git pack, before adding catalog metadata. This
fixture exercises full history, but it is not a full Linux-history benchmark.


## Packed protobuf index: format 3

The same cached complete fixture was imported into a fresh local store with the
five-minute performance assertion enabled. No clone or fetch was performed.
The source SHA, object counts, uncompressed bytes, and compressed data-pack bytes
match the format-2 baseline above.

| Measurement | Result |
| --- | --- |
| Full import, including scratch cleanup | **1 min 37.79 s** (previously 16 min 1.16 s) |
| Reachability enumeration | 2.07 s |
| Object decoding, compression, and staging | 82.59 s |
| Directory size enrichment | 9.61 s |
| Index build and durable pack upload | 3.23 s |
| Cold open plus root listing | 1.26 ms; 45 entries; 8 GETs; 373,030 bytes; zero file-data pack reads |
| Unchanged reimport | 367.60 ms; no new objects or data packs |
| Entire test, including verification and temporary-store cleanup | 101.09 s |

The importer now batches protobuf index pages into immutable packs of at most
8 MiB. Page references record the pack key, offset, length, and page checksum.
Readers continue to fetch and cache individual pages. Durable filesystem writes
and the final conditional HEAD publication are retained; every index pack must
finish uploading before publication.

A deterministic 20,000-record regression reduced durable metadata writes from
160 to 1. In the local reproducer, index build time fell from 1.32 s to 11 ms.
The full-history run is about **9.8 times faster** than the recorded baseline,
although host load was not controlled across these development runs.

Validation again compared all 8,401 HEAD file paths, modes, and blob IDs against
Git, checked 17 complete file hashes, opened a historical commit without file
data reads, and verified an unchanged reimport. Regression tests cover pack
boundaries, range reads, checksum failures, final-pack upload failure without
HEAD changes, and successful retry. Format-3 stores require a fresh import from
earlier formats. Full Linux history and remote S3 timing remain unmeasured.


## Bounded compression pipeline and native C experiment

Compression now overlaps with Git reads and staging. A fixed ring holds at most
two chunks per worker; each encoder has its own context. The importer consumes
completed chunks in submission order, keeping staging and pack writes on one
goroutine. All outstanding chunks drain before publication. The default selects
`min(4, GOMAXPROCS)` workers. `GYIT_IMPORT_WORKERS=numcpu` makes the cached-fixture
test use `runtime.NumCPU()`; this host reports 15 CPUs.

Initial full-history concurrency probes (CPU profiling enabled) measured:

| Encoder | Workers | Import time |
| --- | ---: | ---: |
| Go SpeedFastest | 4 | 68.76 s |
| Go SpeedFastest | 15 (`NumCPU`) | 111.99 s |
| Go SpeedFastest, repeat | 4 | 103.16 s |

Background builds and validation work were active during these development
probes. The four-worker variation is large, so these numbers do not isolate the
causal effect of worker count. The last two runs passed content verification but
failed an experimental 75-second limit; the normal opt-in budget remains five
minutes. There was no demonstrated benefit from 15 workers, so four remains the
default cap.

The experimental native build used system **libzstd 1.5.7**, level 1, through
cgo. Four Go workers each own one reusable C context; internal C threading is
disabled. Frame checksums and independent chunk decoding are preserved. The
reader still uses the Go decoder. This requires no storage-format change.

A focused single-worker benchmark used 32 sampled HEAD blobs, totaling 380,755
bytes. Git reads and corpus setup were excluded. Each source was limited to one
1 MiB chunk. Three two-second runs measured:

| Encoder | Throughput range | Median | Compressed / raw |
| --- | ---: | ---: | ---: |
| Go SpeedFastest | 446–502 MB/s | 461 MB/s | 0.2302 |
| Native C level 1 | 732–846 MB/s | 754 MB/s | 0.2249 |

That is about 1.6 times the compression throughput on this sample, with slightly
smaller output. It does not establish a universal comparison across compression
levels or corpora. See the repeatable benchmark commands in README.md.

The native full-history import, without CPU profiling, took **58.86 s**. It
processed the same 477,069 objects and 11,903,666,409 raw bytes. Data packs occupied
**2,415,196,229 bytes**, versus 2,457,647,576 for Go SpeedFastest (1.7% smaller).
Cold open plus root listing took 1.11 ms, with 8 metadata GETs and zero file-data
pack reads. All 8,401 HEAD paths and 17 sampled complete file hashes were checked;
unchanged reimport took 377.82 ms. The whole test took 62.16 s.

A subsequent Go run with four workers and CPU profiling disabled took
**60.06 s** (63.40 s for the whole test). It performed the same content checks,
with a 1.35 ms cold root listing and 459.10 ms unchanged reimport. These two
unprofiled runs are the closest end-to-end comparison: **58.86 s native versus
60.06 s Go**. The roughly 2% gap is within plausible host variation; it does not
establish a material end-to-end speedup. The stronger native compression result
is from the focused codec benchmark. The pipeline overlaps compression with
other importer work, so codec throughput does not translate directly into total
import speed.

The native library API and per-context concurrency rules are documented in the
[libzstd 1.5.7 manual](https://facebook.github.io/zstd/doc/api_manual_v1.5.7.html).
The native backend and build option were removed because the measured speedup
did not exceed the required **2x** threshold. Pure Go is the sole implementation;
the native measurements above are retained as historical evidence.

## Controlled verification of delta-compression savings

The same **183,627 blob objects** from the cached fixture were repacked twice
using Git 2.50.1. Both runs used zlib level 6, identical object IDs and path hints,
`--no-reuse-object`, `--depth=50`, `--threads=4`, and `--delta-base-offset`.
The only packing option changed was `--window=0` versus `--window=10`.
`--no-reuse-object` also prevents reuse of the original pack's existing deltas.

| Representation of the identical blob set | Bytes | Decimal size |
| --- | ---: | ---: |
| Fresh Git pack, delta search disabled | 2,158,821,546 | 2.159 GB |
| Fresh Git pack, delta search enabled | 399,258,136 | 399.3 MB |
| Current application data packs (Go Zstandard), from the import benchmark | 2,457,647,576 | 2.458 GB |

Disabling deltas increased the controlled Git pack size **5.407 times**.
Enabling them saved 1,759,563,410 bytes, or **81.5%**. The application data packs
are **13.8% larger than Git's delta-disabled pack**, supporting missing deltas as
the dominant source of the storage gap. That remaining difference also includes
codec/settings, per-file versus independent-chunk compression, checksums, and
framing; this experiment does not isolate those contributions individually.
All three rows exclude catalog/commit/tree metadata and external pack indexes.
The Git rows include their small pack/object headers.

`git verify-pack -v` validated both new packs and their exact object-ID sets.
The delta-disabled pack had zero deltas; the enabled pack had **167,380**
delta-encoded blobs and maximum blob chain depth 50. Pack creation took 131.24 s
without deltas and 43.58 s with deltas; these are observations, not timing targets.

The original cached pack was preserved. It contains 153,535 delta-encoded blobs
(83.6% of its 183,627 blobs), with maximum blob chain depth 19. Its blob entries
occupy 593,345,868 bytes; its entire pack, including other object types, is
648,028,826 bytes. The fresh enabled pack is smaller because its packing differs
from that original pack; it must not be mistaken for the original clone's size.
Source pack sizes and modification times were checked unchanged, and all
experimental packs and indexes were removed.

The relevant options are documented in
[git-pack-objects](https://git-scm.com/docs/git-pack-objects).

## One-level chunk deltas (2026-09-21)

Same cached full-history fixture: 40,985 commits, 477,069 imported objects,
183,627 blobs, 184,487 chunks, and 11,903,666,409 raw object bytes. Fresh local
object-store destinations, pure Go Zstandard SpeedFastest, four compression
workers, profiling disabled. Clone time is excluded. Both modes use format 4
and the same final reader. Measurements are individual sequential runs on the
same machine with warm OS filesystem caches, not cold physical-disk benchmarks.

| Measurement | Independent compression | One-level deltas |
| --- | ---: | ---: |
| Full import | 55.90 s | 85.87 s |
| File-data pack bytes | 2,457,647,576 | 1,350,864,602 |
| Index bytes | 1,307,803,053 | 1,326,244,211 |
| Total stored object bytes | 3,765,954,377 | 2,677,612,563 |
| Delta chunks | 0 | 124,388 (67.4%) |
| Cold open + root listing | 1.031 ms | 1.195 ms |
| Root metadata GETs / payload GETs | 8 / 0 | 8 / 0 |
| Root bytes fetched | 373,030 | 372,621 |
| Cold partial read p50 / p95 | 0.439 / 0.567 ms | 0.601 / 0.886 ms |
| Warm partial read p50 | 0.096 ms | 0.096 ms |
| Cold read p50 with injected 5 ms per GET | 45.165 ms | 45.225 ms |
| Warm version-open p50 | 0.457 ms | 0.467 ms |
| Unchanged reimport | 0.358 s | 0.396 s |

Deltas save **45.0% of file-data bytes**, or **28.9% of total stored bytes**;
import takes approximately **30 seconds longer** (54% in these runs), still
well under the five-minute limit. Total size includes manifests and all index
packs; it excludes empty local lock files and filesystem allocation overhead.
The index remains about 1.3 GB and now accounts for nearly half the store.
This implementation does not achieve the previously measured 5.4x native-Git
pack reduction: that experiment allowed delta chains up to depth 50, whereas
this format allows only a single full base and uses bounded candidate matching.

Read measurements use 65 deterministic HEAD file samples, each reading up to
4 KiB at the file midpoint. The entry/OID is resolved before timing, then the
repository cache is reset to an empty 8 MiB cache. Timings include blob/chunk
index reads, payload reads, decompression and reconstruction. Eighteen samples
use deltas; the others remain full chunks. The independent run makes 455 GETs
(65 payload GETs) and transfers 6,769,756 bytes in aggregate. The delta run makes
473 GETs (83 payload GETs) and transfers 7,463,887 bytes: **10.3% more bytes for
these cold reads**, despite substantially smaller total history storage.
Warm repeats make zero GETs. The extra base and delta fetches run concurrently.
The 5 ms per-GET case is a latency-injected local backend, **not a remote S3
measurement**. Warm version-open timings resolve the same SHA repeatedly; the
real Linux FUSE test separately verifies actual switches and stable inodes.

Both full-history runs compare all 8,401 HEAD paths to Git, verify 17 full blob
hashes, open a historical commit, and check a no-op reimport. Focused tests cover
insertions/deletions, partial reads crossing chunks, incremental reuse of the
original full base across multiple publications, simultaneous cold reads,
parallel range requests, corruption, memory accounting, and failure cleanup.
A real Linux FUSE run uses a delta-backed update while two mounts remain active,
checks the switched file, unchanged inode IDs, and an old pinned open handle.

Reproduce, reusing the ignored clone and cleaning each temporary destination:

```sh
GYIT_MEDIUM_TEST=1 GYIT_NO_DELTAS=1 go test ./internal/repo \
  -run '^TestMediumRepository$' -v -count=1 -timeout=10m
GYIT_MEDIUM_TEST=1 GYIT_NO_DELTAS=0 go test ./internal/repo \
  -run '^TestMediumRepository$' -v -count=1 -timeout=10m
```

Local measurement logs: `.build/delta-baseline-final.log` and
`.build/delta-enabled.log`. The cached source remains unchanged. Format 4 stores
require a fresh import of earlier experimental formats.

### Isolating the delta-depth limit

A follow-up repacked the identical 183,627 blobs using the same inputs and Git
options as the controlled comparison above, but with `--depth=1` instead of
`--depth=50`. Both use window 10, zlib level 6, four threads, and
`--no-reuse-object`, so existing deltas cannot bypass the depth limit.

| Data representation (metadata excluded) | Stored bytes |
| --- | ---: |
| Git, maximum depth 50 | 399,258,136 |
| Git, maximum depth 1 | 854,176,479 |
| Application, maximum depth 1 | 1,350,864,602 |

Git's depth-one result is 2.139x its depth-50 result, but remains 36.8% smaller
than our depth-one data packs. It contains 169,228 delta blobs and verified
maximum depth 1. Packing took 108.449 seconds; this timing is not directly
comparable with the application's import because the operations and codecs differ.

This shows the chain-depth limit is a substantial cost, but does not explain
the entire gap. The application currently tries one full anchor per exact
path/chunk hint, visits versions in object-ID order, uses a fixed-probe matcher
with 16-byte minimum copies, and rejects deltas unless their compressed size
plus 200 bytes is less than 80% of the full-frame size. Git searches a candidate
window after sorting by type, path heuristic and size. Different codecs,
whole-blob versus chunk encoding, and delta instruction formats also remain
confounded. This experiment does not isolate each of those remaining costs.
Better candidate selection and matching can be investigated without changing
the depth-one reader contract. The application's 1.326 GB catalog is a separate
cost not included in this table.

`git verify-pack -v` validated the full blob set and depth bound. Source pack
sizes and modification times remained unchanged; all temporary packs/indexes
were removed. Local harness and log: `.build/verify-depth-one.py` and
`.build/verify-depth-one.log`. Git's sorting/window/depth behavior is documented
in [git-pack-objects](https://git-scm.com/docs/git-pack-objects).

## Better base selection and configurable depth (2026-09-21)

The importer now groups each path's objects by descending size instead of object
ID order, compares real compressed deltas against a bounded candidate pool, and
persists candidates for incremental imports. `--delta-depth` supports 1..8
(default 1); `--delta-candidates` supports 1..8 (default 4). The codec and 20%
savings threshold are unchanged. The reference allowance is 200 bytes per
embedded dependency, favoring shorter chains when extra dependencies do not pay.

Same complete fixture and hardware as above; 477,069 imported objects and
184,487 chunks in every run, with four compression workers. The matrix ran
sequentially using fresh temporary local stores, reusing the source without
fetching, cloning, or modifying its object data. Each setting was measured once;
small time differences are not evidence of a general ordering between depths.

| Maximum depth | Candidates | Import seconds | Data bytes | Index bytes | Total stored bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1, previous object-ID ordering | 1 | 85.87 | 1,350,864,602 | 1,326,244,211 | 2,677,612,563 |
| 1, size ordering | 1 | 96.72 | 1,076,947,610 | 1,329,646,676 | 2,407,098,036 |
| **1, new default** | **4** | **103.93** | **1,068,706,898** | **1,330,924,043** | **2,400,134,691** |
| 2 | 4 | 163.70 | 960,312,378 | 1,350,565,396 | 2,311,381,522 |
| 4 | 4 | 157.66 | 918,758,365 | 1,378,889,759 | 2,298,151,874 |
| 8 | 4 | 152.51 | 858,938,178 | 1,417,989,942 | 2,277,431,870 |

Better ordering supplies most of the depth-one improvement. Four candidates
save another 8,240,712 data bytes versus one candidate in this fixture, while
also allowing incremental imports to retain an older, better base when a newer
version is unrelated. The new default saves 20.9% of data and 10.4% of total
storage versus the previous implementation. All imports remain below five
minutes. Maximum observed chain depths exactly reached the requested 1/2/4/8.

All dependency locations are embedded in a chunk's index record. Missing ranges
are fetched concurrently, then reconstructed and checksum-verified from the
oldest required base. Cached intermediate versions shorten a read's chain. A
cold chunk needs at most depth + 1 payload GETs, capped at nine. Reader payload
cache remains bounded; transient per-flight memory and request concurrency grow
with depth. Writer candidate bytes and match tables remain capped at 8 MiB per
worker, with bounded in-flight buffers and up to 64 total candidates per worker.

| Depth / candidates | Cold read p50 / p95 (ms) | Payload GETs across 65 samples | Total bytes fetched | p50 with injected 5 ms per GET |
| --- | ---: | ---: | ---: | ---: |
| 1 / 1 | 0.470 / 0.780 | 77 | 7,587,096 | 40.992 ms |
| 1 / 4 | 0.466 / 0.813 | 77 | 7,616,318 | 43.962 ms |
| 2 / 4 | 0.533 / 1.005 | 80 | 8,316,346 | 40.878 ms |
| 4 / 4 | 0.466 / 0.757 | 82 | 9,461,591 | 40.837 ms |
| 8 / 4 | 0.499 / 0.846 | 82 | 11,113,972 | 44.702 ms |

These are the same known-OID midpoint reads, with empty 8 MiB repository caches,
used above; bytes include metadata. 53/65 HEAD samples are full chunks in every
setting. Sampled depths are `[53,12]`, `[53,12]`, `[53,9,3]`, `[53,8,3,1]`, and
`[53,8,3,1]` respectively, so these latency percentiles do **not** measure the
worst depth-eight case. Deterministic round-trip tests separately exercise
actual depth 1/2/4/8 chains, including simultaneous cold reads and exact payload
request bounds. All warm repeats make zero GETs. Root listings remain
1.04–1.63 ms, with eight metadata GETs and zero payload GETs in every setting.
Injected latency is not a remote S3 benchmark.

Depth eight saves another 5.1% of total storage versus the new depth-one default,
but transfers 45.9% more bytes for these cold reads. Larger embedded chains also
increase metadata size. Depth one remains the default; deeper settings are an
explicit storage/read-cost tradeoff. No compression-level change was included.

Reproduce the full comparison (about twelve minutes here, all destinations
removed after their individual tests):

```sh
GYIT_DELTA_MATRIX=1 go test ./internal/repo -run '^TestDeltaMatrix$' -v -count=1 -timeout=30m
```

Or select a setting:

```sh
GYIT_MEDIUM_TEST=1 GYIT_DELTA_DEPTH=4 GYIT_DELTA_CANDIDATES=4 \
  go test ./internal/repo -run '^TestMediumRepository$' -v -count=1 -timeout=10m
./gyit import --repo /path/to/repo --store /path/to/objects --delta-depth 4 --delta-candidates 4
```

The importer writes format 5. Version 4 stores remain readable, and their full
anchor records are reused when upgrading. Old executables must be updated
before they can open new publications; existing pinned mounts retain their
immutable data. Local log: `.build/selection-matrix.log`.

## Blame history index (2026-09-21)

Workload: `README.md` lines 1–40 at `595cc91e8cbb1c2ca822d0311dcf12709410c582`, using the existing medium fixture. Both variants use the same current blame algorithm and a 32 MiB cache; the legacy variant disables only the optional acceleration index. These are repository-operation timings, excluding CLI process startup and output formatting. Each table entry is the median of three one-iteration Go benchmark runs.

| Environment | Repository cache | Legacy walker | History index | Speedup |
| --- | --- | ---: | ---: | ---: |
| host | fresh | 4.170 s | 0.089 s | 46.7× |
| host | reused | 4.045 s | 0.054 s | 75.4× |
| lima | fresh | 15.690 s | 0.504 s | 31.1× |
| lima | reused | 16.130 s | 0.062 s | 259.8× |

| Per blame, fresh repository cache | Legacy | Indexed |
| --- | ---: | ---: |
| Object-store GETs | 24,413 | 831 |
| Fetched bytes | 337,115,362 | 16,836,448 |
| Cumulative Go allocated bytes | 9,929,335,832 | 214,157,064 |
| Go allocations | 98,603,025 | 1,416,265 |

With a reused cache, indexed blame makes zero GETs. Fresh means a new repository cache after resolving the revision; OS and filesystem caches were not flushed. Lima accesses the store through its host-shared filesystem, so these results are not remote S3 measurements. Allocated bytes are cumulative allocation volume, not peak live memory or RSS.

Backfilling the existing store added 7,236,117 bytes (about 6.90 MiB / 0.30%) and took 4.73 seconds, without new Git objects or blob payloads. A fresh complete fixture import took 121.66 seconds, including about three seconds for the history phase, remaining below the five-minute target. No-op reimport took 1.24 seconds with no new objects or packs. Root listing remained eight metadata GETs with no payload GETs.

The earlier decoded-page cache experiment was reverted: it increased metadata requests and did not improve Lima end-to-end time. This implementation instead changes what metadata is fetched, using packed parent records and conservative changed-path filters.

Reproduce (the source clone and imported store must already exist; no cloning or fetching):

```sh
GYIT_MEDIUM_PARITY_TEST=1 go test ./internal/controlcli -run '^TestMediumCommandParity$' -v -count=1
go test ./internal/repo -run '^$' -bench '^BenchmarkBlameMedium$' -benchtime=1x -count=3
GOOS=linux GOARCH=arm64 go test -c -o .build/repo-history.test ./internal/repo
limactl shell default env GYIT_BENCH_STORE=$HOME/src/gyit/.testdata/lima-store \
  $HOME/src/gyit/.build/repo-history.test \
  -test.run '^$' -test.bench '^BenchmarkBlameMedium$' -test.benchtime=1x -test.count=3
```

Set `GYIT_BENCH_STORE` or `GYIT_PARITY_STORE` to select another imported store. Raw logs: `.build/history-benchmark-host-final.log`, `.build/history-benchmark-lima-final.log`, `.build/history-medium-import.log`, `.build/medium-parity-final.log`. The integration suite compares actual binary output byte for byte, including blank-line attribution; diff rename detection is explicitly disabled on both sides. Supported cases are listed in `internal/controlcli/medium_parity_test.go`.

Validation for this change: the complete race-enabled suite (`go test -race
./...`), the opt-in full medium import, exact binary-output integration suite,
and Linux FUSE tests passed. A separate temporary mount also verified exact
blame output before/after switching, automatic socket discovery and stable
inodes; it was unmounted and removed. The temporary native Git worktree was
removed as well. Structured review reported no actionable findings:

```sh
autoreview --mode local \
  --prompt-file .build/history-index-scope.md \
  --output /tmp/gyit-history-index-review.log \
  --json-output /tmp/gyit-history-index-review.json
```

## File history

`BenchmarkLogFileMedium` queries the latest 20 changes to `README.md` at the
same pinned medium-fixture commit used by the blame benchmark, with a 32 MiB
cache. It reports object GETs, fetched bytes, time and allocations. It reuses the
existing fixture without cloning or importing:

```sh
go test ./internal/repo -run '^$' -bench '^BenchmarkLogFileMedium$' -benchtime=1x -count=3
```

On the macOS arm64 host, the initial tree-comparison implementation took 4.133 s
fresh / 4.057 s reused (one baseline sample each). Skipping Bloom-negative file
history along an uncontested frontier reduced this to 46.6 ms fresh / 26.3 ms
reused (medians of three samples). Fresh GETs fell from 27,709 to 430 and fetched
bytes from 492,561,914 to 9,762,218; repeated queries use zero GETs. The fresh and
reused figures refer to the application cache, not the operating-system cache.
Directory filtering retains exact tree comparisons because the existing Bloom
filters contain complete file paths, not directory prefixes.

The ordinary suite checks that 2,048 unrelated commits require at most 64 GETs
and 1 MiB for file history. The opt-in command parity suite compares file,
directory, and first-parent history output byte-for-byte with native Git.

### Following renames and matching pathspecs

```sh
go test ./internal/repo -run '^$' -bench 'BenchmarkLog(Follow|Pathspec)Medium$' -benchtime=1x -count=3
go test ./internal/repo -run '^$' -bench '^BenchmarkLogEditedRename$' -benchtime=1x -count=3
```

Both medium benchmarks keep the same pinned commit and 32 MiB cache. Follow
queries the latest 20 changes to `README.md`; the pathspec benchmark queries the
latest 20 matching `:(glob)docs/**/*.md`. The edited-rename benchmark constructs
256 possible source files and follows one renamed-and-edited file, verifying two
commits are returned. Fixture construction/import is outside the timed section.

A single macOS arm64 run measured follow at 50.3 ms fresh / 35.7 ms reused, with
430 fresh GETs (9,762,218 bytes) and zero repeated GETs. The wildcard query took
2.89 s fresh / 2.87 s reused and fetched 20,703 / 20,330 ranges: broad patterns
cannot use the exact-path Bloom filter, so a 32 MiB cache does not retain its
entire metadata working set. Edited-rename discovery took 31.0 ms, 268 GETs and
125,973 fetched bytes. These are application-cache measurements; the OS cache
was not evicted. The ordinary suite separately asserts exact renames fetch zero
file-payload ranges and compares changed-content, binary, CRLF, empty, copy,
ambiguous-source, symlink, and merge histories byte-for-byte with native Git.

## Read-only views (2026-09-21)

All times below use the existing medium fixture and a 32 MiB application cache.
Fresh means a new repository cache after opening the selected snapshot; reused
means the same request was run once before measurement. Neither resets the OS
page cache. These are local-backend measurements, not S3 latency estimates.
Output goes to a discard writer; complete CLI/RPC parity is checked separately.

| Command | Host fresh | Lima fresh | Lima reused | Over 1 second |
|---|---:|---:|---:|---|
| `show` | 258 ms CLI first run | 89 ms | 8 ms | No |
| `ls-tree HEAD` | 1 ms | 2 ms | 3 ms | No |
| `ls-tree -r HEAD` | 195 ms | 807 ms | 144 ms | No |
| `ls-files` | 180 ms | 997 ms | 147 ms | Near threshold |
| `cat-file -p HEAD:README.md` | 1.3 ms | 20 ms | 2 ms | No |
| `grep -n the -- README.md` | 0.5 ms | 7 ms | 0.3 ms | No |
| `grep -l the` (whole repository) | 1.45 s | 9.91 s | 10.03 s | **Yes** |
| `branch -a` | 6 ms | 30 ms | — | No |
| `branch -av` (4,670 branches) | 230 ms | 1.56 s | — | **Yes** |
| `tag` | 6 ms | 26 ms | — | No |
| `show-ref` | 7 ms | 18 ms | — | No |
| `rev-parse --short HEAD` | 10 ms | 28 ms | — | No |
| `rev-list -100 HEAD` | 15 ms | 38 ms | 10 ms | No |
| `rev-list --count HEAD` | 16 ms | 85 ms | 14 ms | No |
| `merge-base HEAD HEAD~100` | 45 ms | 150 ms | 40 ms | No |
| `shortlog -sne HEAD` | 770 ms | 1.31 s | 880 ms | **Fresh** |

The object/query Lima measurements are one iteration per temperature; the
verbose-branch figure averages three fresh-cache runs (1.37–1.77 s).
Do not interpret differences of a few milliseconds as statistically meaningful.
The complete CLI parity runner also logs `SLOW (>1s)` for existing history
queries: broad directory, glob, and exclude-only/path-exclusion log filters
exceeded a second in the current host run. Single-file log and follow remain
below the threshold.

Whole-repository grep fetches roughly 264 MB with 17,383 GETs fresh and 260 MB
with 17,253 GETs reused. Its working set exceeds the cache, so repeating it
still reads most data. Streaming one decoded chunk at a time improved host
runtime from 2.76 s to approximately 1.45 s. An index is needed to avoid the scan.
Full shortlog decodes all author metadata; retaining pending commit records
within a 2 MiB allowance removed duplicate decoding (host 1.5 s to 0.77 s).
Verbose refs use bounded routing and subject caches (Lima 2.66 s to 1.56 s).
Transient allocations remain high for these broad queries even though retained
memory is bounded; allocation volume is reported by the benchmarks.

Repeat without cloning or fetching:

```sh
GYIT_MEDIUM_PARITY_TEST=1 go test ./internal/controlcli -run TestMediumCommandParity -count=1 -v
go test ./internal/repo -run '^$' -bench 'Benchmark(ObjectViewsMedium|ReferenceViewsMedium|ViewGraphMedium|ShowMedium)$' -benchtime=3x -count=1
GOOS=linux GOARCH=arm64 go test -c -o .build/repo-views.test ./internal/repo
limactl shell default env GYIT_BENCH_STORE=$HOME/src/gyit/.testdata/lima-store $HOME/src/gyit/.build/repo-views.test -test.run '^$' -test.bench 'Benchmark(ObjectViewsMedium|ReferenceViewsMedium|ViewGraphMedium|ShowMedium)$' -test.benchtime=3x
```

Native parity includes real protobuf streaming/CLI, object/graph/reference
queries, synthetic rename and merge cases, quiet negative exits, reference
metadata migration, pinned publications during updates, and nested FUSE cwd
socket discovery. Unsupported semantics fail explicitly as documented in README.

## Full Linux import stopped; bounded fixes (2026-09-21)

The full bare source has 1,483,466 reachable commits and roughly 151 GiB of
uncompressed objects, despite its 6.38 GiB Git pack. The first import rejected a
historical `100664` tree mode; canonical permissions and raw-tree preservation
were fixed. The corrected full import was stopped on request after 25m27s,
without publishing HEAD. Its progress reached 4,239,549 objects and its staging
database had grown to roughly 59 GiB. No completed full-history import or
full-history operation timings are claimed.

Version 6 removes global per-entry tree records for new trees. It writes compact,
individually compressed directory pages, shares identical pages within an import,
and records sizes during the initial size pass. Directory sorting spills to disk
above 4,096 entries. Temporary sort keys now use binary hashes and object IDs,
and no-longer-needed ordering files are removed before object conversion. Blob
input interleaves independent compression-worker lanes while preserving each
path's descending-size order and candidate ownership.

Local macOS ARM64, same Go toolchain, default four compression workers; OS caches
were not flushed. The synthetic directory benchmark has 256 revisions of 2,048
files, changing one file per revision. Times are medians of three single imports:

| Directory-history import | Before | After |
| --- | ---: | ---: |
| Time | **1.176 s (>1 s)** | 0.514 s |
| Total store bytes | 63,934,073 | 1,856,117 |

A separate fixture borrows real Linux `drivers/net` trees from 512 first-parent
commits, arranged into a temporary linear history. It includes 55,680 reachable
objects. Both binaries receive the identical fixture; it is not full Linux
history. Final comparison, one run per binary:

| Linux directory subset import | Before | After |
| --- | ---: | ---: |
| Time | **9.234 s (>1 s)** | **6.856 s (>1 s)** |
| Total store bytes | 168,709,196 | 138,294,070 |
| Peak scratch-file bytes, sampled every 10 ms | 184,978,465 | 87,964,912 |
| Uploaded blob bytes | 104,223,046 | 104,223,046 |

Isolating scheduling with the compact directory layout held constant gave
**8.702 s -> 7.180 s** (both over one second). A synthetic blob microbenchmark
showed only a small improvement, so the real-directory comparison matters.
Full root listings matched between the two binaries. Tests separately compare
raw tree output and directory pagination to Git, cover historical modes and
non-UTF-8 names, and verify old readers and incremental publications.

The full import remains stopped. These changes reduce metadata expansion and
serialization but still decompress and convert every reachable blob. They do not
establish a full Linux import time or remote S3 navigation latency.

Reproduce with `BenchmarkImportDirectoryHistory`, `BenchmarkImportBlobHistory`,
and `scripts/benchmark_linux_subset.py` (see README). Measurement artifacts are
in ignored `.build/directory-baseline.txt`, `.build/directory-final.txt`,
`.build/linux-subset-combined-final/`, and `.build/linux-worker-isolation/`.

## Linux import budget and bounded source batching (2026-09-21)

The measured local baseline is **15.476334 s (>1 s)** for `git clone
--no-hardlinks`, including checkout, from the cached full-history bare source.
Pack inodes were checked: none were shared. The temporary checkout was removed.
On macOS, checkout warns about Linux paths that differ only by case; the object
clone itself is complete. The corresponding hard import deadline is
**61.905336 s**. No full import has been started since this budget was established.
The full runner now requires this matching baseline and a supported estimate
inside the deadline; it kills the importer process group with SIGKILL at the
limit, without a shutdown grace period.

A profile of the same 55,680-object Linux directory subset exposed per-object
pipe exchanges with Git. Imports now queue at most 128 object names, use
`cat-file --batch-command --buffer`, and consume responses in exactly the
previous order. The queued hints are also bounded. Checksums, incremental object
filtering, encoding choices, immutable publication, and reader code are unchanged.
The importer requires Git 2.36 or newer for this batch protocol.

Two other changes retain the same protobuf bytes: encoding delta operations
with the protobuf wire runtime avoids allocating generated messages per span,
and long copy matches are compared in blocks before locating the exact mismatch.
On 439 sampled Linux blobs, median same-base heuristic encoding fell from
137.38 ms to 119.74 ms. Allocation fell from 35.2 MB / 240,191 allocations to
3.58 MB / 30 allocations per sample pass. This includes reusable compression
buffers and excludes fixture preparation. These are codec measurements, not a
full-import estimate.

Final bounded A/B, one import per binary on the same 512-version directory
fixture, with caches left warm:

| Measurement | Before these changes | After |
| --- | ---: | ---: |
| Import | **7.203 s (>1 s)** | **6.196 s (>1 s)** |
| Total store bytes | 138,294,070 | 138,294,070 |
| Sampled peak scratch-file bytes | 87,964,912 | 87,964,912 |
| Blob pack content hashes | Identical | Identical |

An earlier repeat was 7.161 s → 6.179 s. Root listings also matched. The full
race suite exercises lazy reads, historical tree modes, stable publications,
incremental imports, multi-chunk deltas, and cancellation with unread source
objects. The source protocol change does not move import work into readers.

A four-process Git decoder experiment added bounded prefetch queues, but only
changed this subset from 6.324 s to 6.239 s. That is not a convincing gain, so
that extra machinery was removed from production source. It remains an ignored
experiment for later investigation.

The native-delta composition prototype is **not used by imports**. It verifies
439 blob targets against native Git object IDs and emits the existing protobuf
format. Its selected target frames total 2,766,329 bytes versus 3,028,071 for the
same-base heuristic. However, cold payload bytes including the necessary full
base rise from 6,852,972 to 6,895,288; delta selections also increase from 344 to
370. That violates the no-read-regression requirement, so this representation
change remains an opt-in experiment. It does not establish full-import timing.

Reproduce the bounded native experiment using an existing SHA-1 pack and its
index/reverse index; it never clones, fetches, or imports the source:

```sh
python3 scripts/sample_native_deltas.py \
  --pack /path/to/existing/objects/pack/pack-HASH.pack \
  --output .testdata/native-delta-sample
GYIT_NATIVE_DELTA_BENCH=1 go test ./internal/repo \
  -run '^TestNativeDeltaSampleWireMatchesGit$' \
  -bench '^BenchmarkNativeDeltaSample$' -benchtime=3x -count=3 -v
```

The directory A/B runner accepts `--expect-same-payloads` to require identical
blob pack hashes. Artifacts: `.build/linux-batch-final/`,
`.build/native-delta-buffer-baseline.txt`, `.build/native-delta-prefix-results.txt`,
`.build/import-batching-race.txt`, and `.build/native-delta-final-correctness.txt`.
The full importer still expands and converts the reachable data; these bounded
improvements do **not** justify running the full import under the deadline.

## Revised 10x budget and scalable metadata staging (2026-09-21)

The user revised the target from 4x to **10x** the same measured local clone.
That iteration used a **154.763340 s** limit, calculated from the unchanged
15.476334 s baseline. The latest instruction restores the stricter **4x** limit,
**61.905336 s**. The runner recomputes the limit from the baseline duration; its old
`import_limit_seconds` report column does not override the current multiplier.
No full-history import has been attempted under either budget. The estimate gate
and process-group SIGKILL deadline remain mandatory, without a grace period.

A bounded metadata benchmark samples existing Linux pack-index object IDs. It
reads object types/sizes and orders synthetic path hints; it never walks full
history or reads/imports file contents. A fixed permutation supplies unsorted
input. Each preparation has a 15-second context deadline.

| 1,000,000 sampled objects | Original temporary B-trees | Bounded sorting and mapped size lookup |
| --- | ---: | ---: |
| Preparation time | **11.640 s (>1 s)** | **3.950 s (>1 s)** |
| Go allocation | 21,865,900,848 bytes | 676,737,368 bytes |
| Ordered output SHA-256 | Identical | Identical |

These are one run per implementation with identical sampled IDs/hints and warm
OS caches. On 500,000 permuted objects, replacing just the ordering B-tree took
4.570 s to 2.079 s; the allocation profile still attributed about 1.5 GB to blob
size B-tree nodes. The mapped size table reduced that allocation, but the smaller
case stayed around 2.2 s. The stronger gain appears as the input grows.

The retained staging changes are:

- Object ordering, object/chunk/anchor records, and history index records use
  external sorting with a 32 MiB run budget and at most eight merge inputs.
  Duplicate keys retain the most recent value, preserving the previous upsert
  behavior. The immutable catalog builder consumes that sorted stream directly.
- Blob sizes, commit presence, shared directory references, and history parent
  positions use disposable, fixed-width mapped hash tables. They are private to
  the importer and can be reclaimed by the OS under memory pressure. None is
  opened by a mount or included in its cache accounting.
- Tree and commit parsing reuse input buffers instead of allocating a new
  4 KiB or 32 KiB reader for every object.
- The stored protobuf representation, compression decisions, index page shape,
  complete history accelerators, reader code, and publication CAS are preserved.

A synthetic directory-heavy import (256 revisions, 2,048 files) improved from
median **0.469 s to 0.350 s** during this work. That comparison precedes the last
history-staging and parser-buffer changes.

The complete 55,680-object Linux directory subset remains dominated by other
work. Final same-fixture comparison:

| Measurement | Before this metadata work | After |
| --- | ---: | ---: |
| Import | **5.853 s (>1 s)** | **5.782 s (>1 s)** |
| Store bytes | 138,294,070 | 138,294,070 |
| Sampled peak scratch-file bytes | 87,964,912 | 11,525,360 |
| Blob pack hashes and root listing | Identical | Identical |

This is not a convincing end-to-end speed improvement for that blob-heavy
subset, and it does not support a full import within the revised deadline.
Further work must address source object conversion and its pipeline costs.

Reproduce metadata sampling with the cached single-pack Linux source:

```sh
GYIT_LINUX_METADATA_BENCH=1 go test ./internal/repo -run '^$' \
  -bench '^BenchmarkPrepareObjectHintsLinuxSample/permuted/1000000$' \
  -benchtime=1x -count=1 -timeout=30s
```

Artifacts include `.build/linux-metadata-million-baseline.txt`,
`.build/linux-metadata-million-size-map.txt`, `.build/metadata-spill.mem`,
`.build/linux-metadata-final-subset/`, `.build/metadata-final-race.txt`, and
`.build/parser-reuse-tests.txt`.

## Import buffer reuse and rejected decoding experiments (2026-09-21)

The full import remains gated: none of these measurements establishes a finish
within the clone-relative deadline. All real-data experiments below use the
same bounded 512-version Linux directory history, never the full import.

Increasing the compression ring from 8 to 64 slots at the default four workers
lets source reads advance past compression stalls. Worker ownership, input
order, candidate selection and sink order stay the same. Directory conversion
also reuses at most 4,096 entry records and their name/OID buffers, plus the
encoded page buffers. Wide directories still spill to disk. Mount memory
budgets and reader code are unchanged; the larger ring belongs to the importer.

The synthetic 256-version, 2,048-file directory benchmark, three single-iteration
runs per build, measured these medians for directory reuse alone:

| Measurement | Before | After |
| --- | ---: | ---: |
| Import | 0.379 s | 0.347 s |
| Allocated bytes | 267,539,240 | 139,322,064 |
| Allocations | 3,367,258 | 1,268,055 |

A bounded Linux A/B with both changes measured **5.566 s to 5.217 s** (both
**over 1 s**). Both stores contained **138,294,070 bytes**, with identical blob
pack hashes and root listings. This is a modest improvement, not a full-import
prediction. Artifacts: `.build/directory-pool-{baseline,name-results}.txt` and
`.build/linux-pool-ring64-subset/`.

Rejected experiments remain under the ignored `.build/` directory:

- A direct local pack reader returned the same SHA-verified bytes for 55,168
  sampled source objects (1,856,961,230 bytes). Git took **1.994 s**, the standard
  Go inflater **4.142 s**, and the existing Go compression dependency's inflater
  **3.480 s**. All exceed 1 s. Each reader had a 96 MiB decoded-base cache;
  increasing Git's cache to 256 MiB only reduced its time to 1.947 s. The sample's
  512 synthetic commits are absent from the original pack and excluded equally.
  The direct reader is not integrated.
- Four source Git processes with the larger compression queue regressed the
  subset from **4.929 s to 5.170 s** (both over 1 s).
- Four tree-conversion workers helped the synthetic wide-directory case, but
  regressed the real subset from **4.895 s to 5.215 s** with the same larger
  compression queue (both over 1 s). Tree conversion remains serial.

These results rule out adopting those prototypes; they do not rule out other
source-decoding or parallel-conversion designs. Prototype artifacts are in
`.build/source-probe/`, `.build/linux-parallel-ring64-subset/`, and
`.build/linux-tree-parallel-ring64-subset/`.

## Parent capture, directory parsing and history overlap (2026-09-21)

The latest full-run constraint is **4x**, or **61.905336 s** against the existing
15.476334 s clone baseline. The guard rejects estimates above that limit and
uses process-group SIGKILL at the deadline, with no graceful-shutdown extension.
No bounded result below establishes a credible full-run estimate.

Import captures traversable parents in the existing object enumeration and
keeps the separate parent traversal only for legacy upgrades. Directory parsing
also avoids temporary strings for modes and duplicate-name checks. Neither
change alters the stored representation or mounted read path. A synthetic
256-version, 2,048-file directory fixture reduced allocations from about
1,267,536 to 188,247; the earlier real directory sample showed no convincing
elapsed-time improvement. A 16-version root-tree sample measured 4.032 s to
3.822 s, with identical blob pack hashes and root listings. Both exceed 1 s.

Complete history indexing now overlaps object conversion. Both must finish
before publication; failure cancels and joins the other task. Bounded results:

| Sample | Before overlap | After overlap | Store bytes |
| --- | ---: | ---: | ---: |
| 100,000 synthetic commits, unchanged empty tree | 1.673 s | 1.272 s | 120,647,514 |
| 512 versions of a Linux directory, 55,680 objects | 6.853 s | 6.794 s | 138,294,070 |

All four imports exceeded 1 s. The real-data result is effectively unchanged;
do not extrapolate the synthetic gain to the full repository. Real-data blob
pack hashes and root listings matched exactly. The synthetic fixture checks
the imported object count and empty root; it does not verify every history
record. Public import tests separately check graph parity, shallow boundaries,
legacy upgrades, cancellation, failure propagation and publication ordering.

The directory harness refuses samples over 250,000 objects or 4 GiB decoded,
and the commit harness caps synthetic history at 100,000 commits with a 30 s
hard deadline for each import. Artifacts: `.build/linux-history-overlap-subset/`,
`.build/history-overlap-100k/`, `.build/linux-root16-parent-tree/`, and
`.build/tree-mode-{baseline,results}.txt`.

Additional rejected prototypes: native C source inflation improved an isolated
decoder but regressed whole imports; compression batching regressed 4.761 s to
5.573 s; limiting execution to six Go threads regressed 4.720 s to 4.856 s.
All exceed 1 s. None of those prototypes is part of production code.

## Parallel type/size prepass (2026-09-22)

The million-object metadata probe separated source queries from staging. One
warm serial run spent **3.198 s** in Git, **0.586 s** staging records, and
**0.148 s** merging. An earlier run spent 11.295 s in Git, and another reached
the probe's 15 s deadline; timings are sensitive to host and cache state.

For inputs over 8 MiB, the importer now partitions names/hints into temporary
files, using up to four concurrent Git type/size queries. Each worker gets
256-line batches to distribute expensive delta lookups. All workers finish
before parsing their output; failures cancel and join the group. The parser
reads the output files directly, avoiding a concatenation copy. Small inputs
retain a single query. Object conversion order, blob encoding, published
format, reader code and mount memory budgets are unchanged.

| Bounded probe | Serial | Parallel | Result |
| --- | ---: | ---: | --- |
| 1,000,000 sampled Linux object IDs | 3.937 s | 1.852 s | Identical ordered output digest |
| 55,680-object Linux directory import | 4.868 s | 4.760 s | Identical blob pack hashes, root listing and store bytes |

All four measurements exceed 1 s. The metadata probe improved by about 2.1x;
the directory import is effectively unchanged. The ordered digest remains
`aa279c34478016becb263c1a2b4fbc59737eedfd3a29fc09cabc22a8440db372`.
The synthetic 100,000-commit import measured 1.161 s to 1.394 s in one pair;
reversing the binary order measured new 1.174 s versus old 1.178 s. All exceed
1 s; there is no convincing whole-import gain on that small case.

A normal integration test imports 65,537 SHA-256 commits including a merge,
compares the entire parent graph with Git, checks file listing sizes and bytes,
reimports unchanged history and checks temporary-file cleanup. Its baseline
runtime was 4.775 s (**over 1 s**). Existing benchmarks remain opt-in and bounded.

Rejected ignored prototypes: direct native C prefix inflation improved a
100,000-object metadata query from 0.421 s to 0.289 s, insufficient to justify
the additional reader; continuous input to Git for fresh object conversion
regressed a real-data subset from 4.812 s to 4.952 s (both **over 1 s**).

Artifacts: `.build/metadata-{serial,parallel}-million-final.txt`,
`.build/metadata-parallel-final-subset/`,
`.build/metadata-parallel-{final,reversed}-commits/`, and
`.build/metadata-parallel-focused-race.txt`. These results still do not establish
a credible full-import bound within the mandatory 61.905336 s deadline. No full
Linux import was attempted.

## Direct blob ingestion pipelines (2026-09-22)

The full-import guard is **4x** the recorded 15.476334 s local clone, giving a
61.905336 s hard deadline. The runner requires a matching baseline and an
upper-bound estimate supported by bounded experiments, and kills the entire
process group without a grace period. No full Linux import was attempted.

Large fresh imports now keep source reads, verification, compression and pack
writes in the same worker. Multi-chunk blobs still stream once through the
foreground reader, with chunks routed to their original candidate lanes. The
existing codec and delta selection are shared by both pipelines. All workers
must finish before index publication or cleanup. The default remains at most
four workers; `gyit import --workers 8` selects an explicit count.

A 512-revision directory fixture borrowed cached Linux objects and expanded to
1,857,094,302 bytes across 55,680 objects. Each import had a 25 s process-group
kill deadline and each temporary fixture/store was removed afterwards:

| Comparison | Before | After | Verification |
| --- | ---: | ---: | --- |
| Previous importer vs direct pipelines, four workers | 4.790 s | 4.024 s | All 40,148 compressed chunks and complete dependency chains identical |
| Direct pipelines, four vs eight workers | 4.115 s | 2.947 s | Same complete chunk comparison |
| Direct pipelines, eight vs fifteen workers | 2.710 s | 2.503 s | Same complete chunk comparison |

**Every import above exceeded 1 s.** Separate pairs include run-to-run variation;
these measurements do not establish a full-history import bound. Each pair also
compared root listings and verified 384 cold file reads against native Git.
The four-worker final comparison retained 138,289,401 bytes versus 138,294,070;
only pack placement/index references changed. Reader code, page fanout, chunk
sizes, delta depths and payload encodings are unchanged.

Normal tests exercise concurrent blob uploads, SHA-1 and SHA-256 repositories,
empty and multi-chunk files, historical reads, unchanged reimports, cancellation,
source corruption and joining all writers after a failed upload. The parallel
upload test failed against the previous importer at its 5 s deadline before the
pipeline change. `BenchmarkImportBlobPipeline` covers a small synthetic fixture;
the bounded real-data comparison can be repeated with:

```sh
python3 scripts/benchmark_linux_subset.py \
  --source .testdata/linux-repo.git --baseline .build/gyit-metadata-parallel \
  --gyit .build/gyit-blob-pipeline --versions 512 --workers 15 --timeout 25 \
  --output .build/blob-pipeline-comparison
```

The retained script compares listings and reports storage/time; the additional
complete chunk and cold-read checks above used the ignored diagnostic
`.build/native-readcheck/probe` inside the same temporary fixture. Artifacts are
`.build/blob-pipeline-final-recent-512/`,
`.build/blob-pipeline-workers8-recent-512/`, and
`.build/blob-pipeline-numcpu-recent-512/`. Whole-history source expansion, tree
conversion and indexing still need a credible bound before the final run.

## Parallel history diff generation (2026-09-22)

The history importer now enumerates commits in its existing parent-before-child
order and computes raw first-parent diffs in batches of 2,048 commits, with at
most eight workers. A fixed window bounds outstanding batches. Batch output is
spooled to temporary files and consumed in enumeration order; cancellation joins
all Git processes before removing those files. The history parser, block format,
commit positions, changed-path filters, and mounted readers are unchanged.

Run the bounded real-history benchmark with:

```sh
GYIT_LINUX_HISTORY_BENCH=1 go test ./internal/repo -run '^$' \
  -bench '^BenchmarkHistoryStreamLinux$' -benchtime=1x -count=3
```

It selects 16,384 cached Linux commits, excludes their outside parents, and
verifies that the resulting traversal contains exactly the selected commits.
Each measured stream has a 15-second context deadline. No repository import,
fetch, or complete history traversal is requested. The comparison includes
reverse/topological enumeration, raw diffs, output hashing, and the parallel
pipeline's temporary-file handling.

| Stream | Three measurements | Median |
| --- | --- | --- |
| Original serial stream | 1.094, 1.126, 1.110 s | 1.110 s |
| Ordered parallel stream | 0.428, 0.429, 0.422 s | 0.428 s |

**Each serial measurement exceeded 1 s.** Every stream produced the same SHA-256,
`c0639e71733a39df25cce0a6c28592e58a06f3956457d678a78237659072773a`.
Tests also compare complete streams against Git for merges, independent roots,
SHA-256 repositories, unusual path bytes, type changes, incremental exclusions,
source failures, and abandoned/canceled reads.

The synthetic historical-blob fixture regressed from 1.310 s to 1.561 s for a
complete import (**both over 1 s**). All 8,390 file chunks/dependency chains and
8,423 history entries/blocks were identical, with 257 cold file reads verified
against Git. This fixture contains cheap synthetic diffs and does not demonstrate
a whole-import speedup. Phase results do not establish a full Linux import bound;
no full import was attempted. Artifacts: `.build/history-stream-real-benchmark.txt`
and `.build/history-parallel-historical4096/`.

## Parallel directory conversion (2026-09-22)

Fresh imports now convert trees up to 1 MiB with at most eight workers. A shared
pool limits queued/active tree bodies to 32 MiB; larger trees retain the existing
streaming path. Each worker owns its parser, compressor, 8 MiB pack buffer, and
wide-directory scratch file. Workers share a 16-part temporary page registry so
identical pages retain one canonical reference. Pack uploads happen outside its
locks. Disjoint pack-number sequences preserve a common prefix and compact
routing pages. All workers and uploads finish before indexes and HEAD publish.

The source reader still verifies every native tree checksum. Directory page
fanout, inline file sizes, encodings, and reader code are unchanged. Regression
tests cover simultaneous directory uploads, failure cleanup without publication,
corrupted source trees, pack rollover without overwriting earlier ranges, and
existing paged/wide-directory read contracts.

A globally spaced sample selected 19,092 real Linux trees from 32,768 pack
entries: 57,137,387 raw bytes, 1,557,699 directory entries, and 143,742 distinct
referenced blobs. The probe loads only tree bodies and actual blob-size metadata;
it does not fetch file bodies or publish an incomplete repository. All sampled
trees reconstructed byte-for-byte, including original modes, and every inline
file size matched native Git metadata.

Serial conversion measured 0.489–0.518 s across three runs. The final parallel
implementation measured 0.237–0.284 s across three runs; a later adjacent pair
measured 0.514 s serial and 0.146 s parallel. The sample's published data was
48,029,889 bytes before and about 48,028,000 bytes after. These are conversion
phase measurements, not full-import predictions. A mutex profile motivated the
shared-registry change; an earlier pool with one registry lock stalled workers.

A repeatable complete-import benchmark uses a synthetic 1,024-version directory:

```sh
go test ./internal/repo -run '^$' -bench '^BenchmarkImportDirectoryWorkers$' \
  -benchtime=1x -count=1
```

One run measured 0.840 s with one worker and 0.708 s with eight. Allocations rose
from 154 MB to 715 MB per import, including the additional blob/source workers
selected by this option; these figures are cumulative allocations, not peak RAM.

The real directory-history import fixture remained variable. Old/new order gave
4.615/7.676 s; reversing the order gave new 5.413/old 6.670 s. **All exceeded 1 s.**
No whole-import speedup is established. All 40,148 compressed file chunks and
complete dependency chains, all 514 history records/blocks, and 384 sampled
cold file reads matched across builds. No full Linux import was attempted.

Artifacts: `.build/tree-sample/`, `.build/tree-sharded/`,
`.build/tree-pipeline-import-benchmark.txt`, and
`.build/tree-pipeline-{final,reversed}-recent512/`. The real-tree probe remains an
ignored diagnostic, while the synthetic benchmark and regression tests are in
the normal source tree.

## Independent blob work queues (2026-09-22)

Fresh bulk imports now spool blob job metadata into one local protobuf log per
worker. A busy worker no longer blocks the dispatcher from supplying other
workers. File bodies remain in the existing bounded buffers or go directly from
Git to their worker. Raw-chunk markers preserve each worker's exact encoding
order; publication still waits for every worker, upload, index, and history task.
The generated scratch protocol is separate from the published repository format.

The recent directory-history fixture contains 55,680 objects and 1.857 GB of raw
objects. At 15 workers, the hardened implementation measured 1.798 s versus
1.895 s before; reversing run order gave new 2.087 s versus old 2.433 s. **All
exceeded 1 s.** The earlier prototype showed a larger 25% gain, so that figure
should not be substituted for the production measurements. Every one of the
40,148 compressed chunks and its dependency chain matched, as did all 514
history records/blocks and 384 cold file reads against Git. Scratch usage grew
from about 11.5 MB to 14.7 MB for the additional metadata logs.

The earlier prototype's smaller historical fixture measured 0.837 s versus
0.799 s at 15 workers, a small import-time regression; at four workers it measured
1.048 s versus 1.175 s. Small jobs need not benefit from disk queues. Reader code,
cache budgets, published protocols, default worker count, and delta decisions
remain unchanged. These subset measurements do not establish a full Linux bound.

The existing bounded fixture runner supports explicit worker counts for both
builds, so wrappers are unnecessary:

```sh
python3 scripts/benchmark_linux_subset.py \
  --source .testdata/linux-repo.git --baseline .build/gyit-tree-parallel \
  --gyit .build/gyit-blob-job-spool-production \
  --baseline-workers 15 --workers 15 --versions 512 --timeout 25 \
  --output .build/queue-repeat
```

This runner verifies root listings and enforces the object/decoded-byte caps;
complete chunk/history comparisons above used the additional diagnostic reader
probe. Queue tests cover a producer finishing before its consumer starts,
concurrent final batches, opaque path bytes, generated-protobuf interoperability,
raw markers without spooled bodies, cancellation, truncation, and write failure.
Existing import integration tests exercise SHA-1/SHA-256, multi-chunk files,
source checksums, upload failure, cleanup, and publication under the race detector.


## Overlapped metadata and bounded frame encoding

A complete v3.4 history (300,359 commits, 1,371,073 trees, 765,669 blobs;
28,829,844,195 expanded bytes) took **25.781 seconds** with the hardened candidate,
versus **32.978 seconds** with the previous importer, both at 15 blob workers.
Both exceed one second. Candidate ran first. Every import had a 45-second
process-group kill deadline. The diagnostic retained the original Linux source
pack for traversal and body decoding, but used a bounded private pack for the
all-local inventory. This is a bounded history measurement, not a full Linux run.

The candidate overlaps directory/commit conversion with source enumeration and
avoids finishing full-frame compression when the existing delta must win.
It retains eight history workers, source object checksums, all history indexes,
and atomic publication. The default blob worker count remains four.

Repeatable codec regression checks:

```sh
go test ./internal/repo -run '^(TestCutoffCodecParity|TestPreloadedImport)' -count=1
(cd third_party/compress && go test ./zstd -run '^TestBoundedEncoder' -count=1)
```

The subsequent full Linux attempt falsified that forecast: the process group
was killed at the then-configured 154.763340-second limit (observed exit
154.847510 seconds), before publishing HEAD. No complete import time or full
read verification is available. The failed destination and scratch were removed.
Do not repeat the full run using the old estimate; the current guard is stricter.

The main structural difference from a local clone remains: the 6,448,271,473-byte
source pack represents 162,308,101,302 expanded object bytes (25.17x). gyit rebuilds
blob encodings, directory pages, object indexes, and changed-path history;
the no-hardlink local clone reuses existing packed representations. This is work
amplification, not a measured 25.17x elapsed-time attribution to compression.

A bounded recent-history diagnostic (63,207 reachable commits, 648,389 objects,
11,605,582,074 expanded bytes) took 16.186 seconds inside Import. Preparation
ended at 3.948 seconds, objects at 7.973, the foreground index at 8.827, and
publication began at 16.162 after awaiting history. Shallow boundaries in this
fixture create artificial history roots, so these times must not be scaled to
predict full-history time. They show that blob compression alone does not
account for the bounded import's critical path.
