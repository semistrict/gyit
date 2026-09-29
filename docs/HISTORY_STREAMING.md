# Progressive path history

The writer indexes all paths, beginning at the selected revision and walking
back through its parents. A file-log request never creates an index. The reader
can emit proven results while older history is still being acquired and indexed.

## Accepted startup performance

On 2026-09-28 the user accepted fresh startup at 3.69 s versus Git's 1.65 s
(2.23x). This supersedes the strict 2x startup acceptance threshold for this
change; it is not a claim that the implementation meets 2x in every workload.

The final candidate uses the same Git-indexed acquisition optimization and
raises the publication count cap from 4,096 to 8,192 commits, retaining the
one-second processing limit for subsequent publications. Two fresh native-GCS
runs at revision `f817e1690570aac60480babbd2243cc991ba2118` measured:

| Measurement | First run | Repeat |
| --- | ---: | ---: |
| Mount setup | 1.25 s | 1.38 s |
| First README history result, from setup start | 2.64 s | 2.79 s |
| First ten results, from setup start | 3.54 s | 3.62 s |
| Git blobless single-branch clone plus ten-entry file log | 1.68 s | 1.61 s |
| First-ten ratio | 2.11x | 2.25x |
| Snapshot and full commit/tree history ready | 6.45 s | 7.04 s |
| Retained durable bytes | 91,984,725 | 91,950,245 |

These are individual fresh-store runs on the existing GCE host, not a bound on
remote-network latency. Snapshot acquisition continues independently of history
results. The selected revision is pinned; the upstream branch tip moved slightly
between acquisitions, and the benchmark records both revisions. Historical file
contents remain available on demand after commit/tree history is ready.

Cold query-only times in the repeat were README 290 ms, AGENTS.md 133 ms and
package.json 212 ms (1.79x, 2.11x and 1.19x Git), plus 122–172 ms reader bootstrap.
The first run included a 585 ms package.json outlier (3.28x). These misses remain
recorded; accepting startup does not establish a universal cold-query bound.

The acquisition path avoids a second object-identity hash only when the recipe,
packed bytes and every delta base came from the same immutable Git-indexed view.
Store and mixed-origin reads retain identity checks. Within a trusted acquisition,
byte-identical tree-entry runs skip repeated mode/name validation, while still
checking entry boundaries; changed entries retain their parser checks. Real GCE
preparation fell from 1.28 s to 1.24 s for 11,516 commits. No cache limit or decoder
concurrency was increased. The publication regression exceeds the configured
commit cap and checks both the first incomplete publication and final coverage.

Tree-boundary tables and doubling decoder concurrency showed no real preparation
improvement and were discarded. A live native-GCS HTTP/gRPC comparison retained
HTTP: median reads/writes were 32.7/67.1 ms versus 39.9/71.9 ms for gRPC. Direct
connectivity was not established because its diagnostic required bucket metadata
permission unavailable to the VM identity; permissions were not broadened.

The benchmark prefixes are disposable and separate from the interactive mount's
`github` durable data. Measurements below record earlier experiments and their
then-current performance gaps; they do not replace these final measurements.

## Linux cold-history limitation

A later interactive test on 2026-09-28 exposed a remaining scale limitation:
`gyit log Kconfig` on a fresh Linux mount produced no output for minutes.
The full blobless acquisition had a roughly 1.87 GB pack and was actively
indexing, while shallow acquisition held 28,633 reachable commits. Its apparent
Kconfig result was a shallow-boundary artifact, not a proven change.

After full acquisition completed, native Git found the first three actual
Kconfig changes in 0.47 s; the latest was dated 2025-02-19. At inspection,
gyit's durable coverage contained 62,179 commits, was incomplete and had no
recorded ingestion error. The command still awaited enough all-path coverage
to reach and prove its next matching result. Newest-first publication alone
does not give prompt first output for paths unchanged far back in a large
history. The accepted smaller-repository startup timing does not cover this
case. The foreground stall is addressed below; cold reader-only GCS latency
remains unresolved.

## Linux foreground fallback work in progress

The reader now tries published raw commit/tree objects when a derived history
record is missing. It computes only the requested path's parent comparisons,
feeds the existing date-priority/TREESAME walker, and creates no index records.
An absent pathname is distinct from an unavailable object: unavailable objects
still wait for publication, resume the same request and never imply EOF.
Already-covered frames remain the preferred path, and all-path ingestion keeps
running independently. A derived-index error does not prevent a query whose
answer is already proven by published objects.

The regression first failed with `history ingestion failed: index worker paused`
after raw packs were fully published. It now compares ten-entry output with Git
for skewed merges, first-parent traversal, files, directories and absent paths,
using a Store that rejects all writes. A second regression streams available
results from a shallow input, waits at missing parent objects, and resumes after
object publication without starting an indexer. Existing publication tests make
raw packs unavailable to the reader explicitly, so faster fallback execution
cannot make those tests accidentally rely on timing.

An opt-in CPU benchmark uses existing Linux acquisition files, a fresh 32 MiB
decode workspace per run and no all-path index. It is an isolation benchmark,
not a claim that remote reads meet the target. At revision
`72d3fcf802c45d00b300f25b848a93c3a2bd7c7e`, `Kconfig` with a ten-result limit:

| Query-only measurement | Seconds | Total allocation |
| --- | ---: | ---: |
| Initial Go fallback | 1.814 | 1,262 MB |
| Last-parent/path reuse and borrowed tree parsing | 1.301–1.354 | 582 MB |
| Borrowed commit parsing, small buffer, no protobuf round trip | 1.176–1.203 | 366 MB |
| Native Git, same acquired revision, three runs | 0.550–0.584 | not measured |

Git's median was 0.572 s, so the latest Go median is 2.08x and still misses 2x.
Native first-result median was 0.130 s; Go first-result latency has not yet been
measured separately. Total allocation is not peak retained memory. Query state
retains one parsed parent and one path value; traversal still spills at its
existing memory limit. At this measurement the live mount still used v34;
deployment and later GCS measurements are recorded below. The full
Go suite, targeted race tests and WebAssembly build passed for this candidate;
the temporary GCE benchmark executable was removed and verified absent.

Run `BenchmarkRepositoryUnindexedFileLog` with `GYIT_HISTORY_SOURCE`,
`GYIT_HISTORY_SHA` and optionally `GYIT_HISTORY_PATH`. Native Git is invoked only
by the benchmark to establish expected output, never by the query implementation.
Temporary remote benchmark binaries are removed after measurements; no new
cloud instances, buckets or object-store test prefixes were created for this
CPU experiment.

## Published acquisition views and live v35 validation

Readers on the acquisition host can now use its existing pack files after
checking their durable publication markers. Unpublished local packs remain
invisible. Removing acquisition files leaves ordinary Store reads functional.
These views neither fetch nor write objects and do not create per-file indexes.
At most two views run concurrently across repositories, each with the existing
32 MiB decoder workspace. Views are released on completion/cancellation and
reopened at missing-object gaps when publication advances. An incomplete
all-path index no longer forces this reader through scattered index lookups;
a completed index remains preferred. Traversal parses only commit headers,
deferring author/message parsing until a result is emitted.

The GCS-only diagnostic found that one cold commit/path comparison performed
19 GETs and read 27.3 MB in 997 ms; its cached repeat took 34 microseconds.
That is a physical read-locality problem, not merely a slow commit parser.

`TestPublishedHistoryLogLatency` exercises the production reader with fresh
32 MiB caches, GCS publication checks, and retained local acquisition files.
Bootstrap took 344–505 ms. Query results were:

| Path / limit | First result | Total query | Native Git | Ratio |
| --- | ---: | ---: | ---: | ---: |
| Kconfig / 1 | 490 ms | 492 ms | 122 ms | 4.05x |
| Kconfig / 10 | 423 ms | 1,246 ms | 592 ms | 2.11x |
| COPYING / 1 | 1,061 ms | 1,068 ms | 472 ms | 2.26x |
| COPYING / 10 | 1,103 ms | 2,491 ms | 1,188 ms | 2.10x |
| README / 1 | 240 ms | 241 ms | 14 ms | 17.17x |
| README / 10 | 229 ms | 1,365 ms | 641 ms | 2.13x |

Each query used six metadata GETs totaling 7.1 MB. These numbers explicitly
depend on acquisition-file locality; they do not establish fast performance
for a reader with only cold GCS access. Acquisition itself is excluded because
it had already completed. All-path background indexing remained incomplete.

Backend v35 is deployed on the existing guest, SHA-256
`8c5957cc89e65af785c806cc4c45ef6e5a3e16c4ef16b7c25fd9ac47ec3e2723`.
The full Go suite, focused race tests, display-output parity and WASM build
passed. The actual mounted command, with the existing shared cache and retained
acquisitions, produced Kconfig's first result in 306 ms and ten results in
1.069 s on its first run after restart. A repeat measured 236 ms / 1.070 s;
native Git on the same host/revision subsequently took 132 ms / 550 ms.
COPYING took 918 ms / 2.262 s versus 472 ms / 1.164 s (there are only four
matching commits). README took 65 ms / 1.189 s versus 14 ms / 605 ms.
All six mounted result sequences matched native Git by full-ID digest.

The `-n 10` mounted examples are now within 2x in this run, but README's
first-result overhead and independent cold GCS readers still miss the target.
This is not an end-to-end fresh acquisition measurement or goal completion.
Evidence is in `.build/history-v35-mounted.log`,
`.build/history-v35-native-sequential.log`, `.build/history-view-gcs.log` and
`.build/history-object-access-gcs.log`. Both temporary remote diagnostic
executables were removed and verified absent. No new cloud instances, buckets
or GCS test prefixes were created; the existing test mount and data remain.

## Cold raw-object read amplification (v36)

A controlled native-GCS probe walks sixteen first-parent comparisons from the
same Linux SHA, each run with a fresh 32 MiB decoded cache and no acquisition
files. This measures raw comparison cost, not first matching result latency.
The live writer continued publishing, so exact global-index byte totals can
vary between runs. Bootstrap was 48–60 ms and is excluded below.

| Reader | Comparison time | GETs | Bytes returned |
| --- | ---: | ---: | ---: |
| Previous decoder | 6.150 s | 113 | 109,359,847 |
| Reuse decoded delta bases | 4.679 s | 96 | 109,308,234 |
| Also read narrow object-recipe index pages | 3.852 s | 120 | 2,626,360 |

Remote recursive delta bases now occupy ordinary uncompressed entries in the
existing bounded cache. Their key includes immutable pack identity, size and
offset. Offset entries never prove Git identity: the reconstructed requested
object must still pass its OID check. Cache hits still consume the decoder's
working-memory budget. Only recursive bases use this extra key; top-level
objects keep their ordinary verified-OID entries. Admission happens after
recursive decode, avoiding recursive singleflight deadlocks.

Object-recipe lookups now reuse verified pages or cached containers if present;
otherwise they fetch just the referenced page range, checking length and hash.
Directory scans and indexed-history prefetch retain container reads. Both paths
share the existing cache entries, so switching strategies does not create a
second persistent cache. The regression failed by reading 65,983 bytes for
395 bytes of needed pages; it also checks warm-container reuse and corruption.
The delta regression failed with four packed-member reads instead of three,
and checks both disk and RAM cache implementations, cancellation, memory
budgets, and final identity verification with a corrupt cached base.

Traffic fell about 42x, but serial request latency still dominates. This does
not establish the 2x file-log goal, and no fresh acquisition or all-path-index
completion timing was rerun here. Evidence: `.build/history-delta-before.log`,
`.build/history-delta-after.log`, `.build/history-delta-ranges.log`. Run
`TestStoreUnindexedHistoryAccess` with `GYIT_HISTORY_WALK=16` plus the existing
Store/SHA variables to reproduce. The full Go suite, focused race suite and
WASM build passed; later budget/corruption test additions passed separately.

Backend v36 is now running in the same `gyit-fresh` guest session, SHA-256
`283394284de322b2b9823f182b9f90ae42a949002d5e00a41321280e8674ca08`.
The mounted Kconfig query returned its first result in 332 ms and `-n 10` in
1.055 s. COPYING took 866 ms / 2.181 s; README took 45 ms / 1.164 s.
All six output-ID digests still match the native baseline at the same revision.
These mounted runs retain acquisition files and the shared cache; they are not
cold GCS-only measurements. `.build/history-v36-mounted.log` records the result.
The tmux shell is ready in the Linux repository with `gyit` on PATH and mouse
support enabled. The three remote before/after diagnostic executables were
removed and verified absent; no additional VM, bucket or GCS prefix was created.

The remaining raw-reader problem is the number of serial requests, not transfer
volume alone. The sixteen-comparison probe now makes 120 GETs. The next physical
layout work must let a bounded read cover many ancestry comparisons before
all-path indexing is complete; another parser-only change cannot remove these
network round trips. The goal remains active.

## Bounded prefix rebuilding from acquisition objects

The remote compaction walk itself was too expensive to repair old layouts.
A read-only 64-frame probe spent 9.75 seconds collecting only 192 visited
commits, using 279 index requests. The alternative maintenance method
`RebuildHistoryPrefix` prepares up to 65,536 commits using the same Go importer
and published acquisition objects in a private local Store. It links those
frames locally, uploads the path/display data and final graph containers, then
changes only the selected commit's global entry with one HEAD CAS.

This operation has no pathname parameter and never runs as a query-created
file index. The existing all-path ingestion frontier, coverage counts and
completion state are unchanged. Parent links expose the rebuilt frames;
boundaries and omitted links use the ordinary published index. Existing reader
publications keep working. The method requires an already-indexed entry and
verifies acquisition pack publication before using local source objects.

The private Store is temporary data staging, not another cache tier. It shares
the existing bounded decoded cache and is removed on success, cancellation or
failure. Global lookup pages and pre-compaction graph copies are not uploaded.
Directory traversal batches and immutable upload requests/bytes are bounded.
No native Git subprocess performs rebuilding or answers a mounted query.

Tests compare old and new readers with Git across sparse history, prefix
boundaries, merges, skewed timestamps, directories, absent paths and first-parent
mode. They check unchanged coverage, no reads of old remote graph/path data,
source-publication enforcement, CAS conflict, and cancellation before HEAD.
`BenchmarkHistoryPrefixRebuild` is opt-in via `GYIT_HISTORY_SOURCE`; acquisition
and its seed publication are outside the timed region.

A 32,768-commit Linux read-only probe reached its first upload attempt at
32.78 seconds, with 11 GCS index GETs / 494 KB and no graph, path or raw-pack
GETs. Durable writes were refused in that probe. This measures preparation,
not publication or query latency; evidence is
`.build/history-v40-prefix-readonly.log`. Background preparation also now uses
narrow index pages for its pinned coverage view; a regression exposed that
this direct view had bypassed the point-lookup fix.

## Deferred parent references and remaining cold-read gap

The bounded frame window cannot own the only reference to a pending merge
parent. A date-priority walk can visit more than eight other frames before that
parent becomes next. Previously its location disappeared with the frame, forcing
a global lookup or reporting missing coverage despite a valid immutable link.
Candidates now retain that location in the bounded query queue, including its
protobuf spill record. Destination identity is still checked when loading it.
A twelve-frame merge regression failed before the change; separate tests cover
in-memory and spilled candidate locations. This adds no durable repository data.

The real 32,768-commit prefix publication took 30.852 s: first upload at 29.276 s,
HEAD publication at 30.723 s, 191 write attempts / 44,771,244 bytes, and 17 index
GETs / 574,512 bytes. It fetched no old graph/path/raw-pack data and preserved
800,169 covered commits with incomplete history. This is maintenance preparation
and publication time, separate from the following reads.

Cold GCS-only Linux Kconfig still misses the goal. With the deferred-reference
fix, first result took 1.106 s plus 308 ms bootstrap, versus Git 119 ms (9.26x
query-only). The first-ten request returned only two results before its 10 s
whole-test deadline; Git returned ten in 538 ms. The reader had an empty 32 MiB
cache and no local acquisition access. These are individual diagnostic timings,
not evidence of meeting the latency requirement.

Disabling speculative prefetch reduced first-result requests from 55 to 30 but
left latency above one second; first-ten still timed out. The prefix primarily
covers first-parent chains. Sparse path traversal enters branches outside it,
returning to the old SHA-index/graph lookup path. Kconfig's first two changes are
in 2025, and the third is in 2020. Changing prefetch alone does not solve that
coverage and storage-layout problem. Evidence: `.build/history-v40-prefix-published.log`,
`.build/history-v40-cold-no-prefetch.log`, `.build/history-v41-cold-prefix.log`.

The restored source-assisted nested-KVM mount returned Kconfig first result in
311 ms, first ten in 1.077 s; COPYING in 886 ms / 2.151 s (four total matches);
and README in 37 ms / 1.131 s. All six result digests match the pinned Git
baseline. These use already-published local acquisition data and must not be
reported as cold GCS-only results. The CLI is on PATH in `gyit-fresh`.

Build this backend with `python3 scripts/build_vhost.py`, not a plain tagged Go
build: its generated Go-FUSE overlay is required for scatter reads into guest
buffers. The deployed backend passed `TestScatterReadAcrossGuestPages` and the
mounted checks. The four uploaded ad hoc diagnostic binaries were removed and
verified absent, with no remaining `TestPublishedHistory*` scratch directories.
No VM, bucket or separate GCS prefix was created for these measurements.

## Direct graph-frame links (validation in progress)

New history publications write graph frames in reverse preparation order and
include immutable destinations for already-written parent frames. A reader
uses those destinations before consulting the SHA-keyed global index. Parent
identities, timestamps, path masks and the traversal algorithm are unchanged.
A missing link remains a normal global-index lookup, including at publication
boundaries. Each frame remains limited to 64 KiB and 128 links; preparation
stages frames on disk and rewrites one at a time with periodic checkpoints.

Compaction rebuilds links against its new frames, instead of retaining links
into old fragmented containers. It can repack a bounded prefix of incomplete
history without advancing coverage, modifying the frontier, or claiming the
entire ancestry is compacted. A complete linked-compaction marker distinguishes
old packed data from the new layout. Readers pinned to old publications retain
valid references. New tests cover crossing staging checkpoints, partial-prefix
boundaries, legacy compaction, publication failures, and invalid destinations.

A first 512-frame repack attempt timed out after 180 seconds. It performed
196 graph GETs / 6.1 MB and 4,040 global-index GETs / 8.2 GB. A read-only probe
then found ten-level lookup paths, including several one-child internal pages
only 181 bytes long. A regression reproduces those redundant levels surviving
an incremental update; updated paths now promote the immutable child directly,
while unchanged snapshots retain their old structure. Mixed subtree depths
preserve ordered key ranges; existing copy-on-write reads remain valid.

History point lookups, index read-ahead, and compaction now request verified
page ranges instead of admitting each entire 2 MiB index container. The local
regression previously fetched 2,097,801 bytes for a sub-kilobyte index page.
Against the existing un-repacked GCS history, narrow reads alone still timed
out: 9.500 seconds querying, no results, 406 GETs / 28.9 MB, plus 362 ms reader
bootstrap, compared with Git at 123 ms. That is a reduced transfer volume, not
a successful latency result. `.build/history-v38-cold-ranges.log`,
`.build/history-v38-repack.log`, and `.build/history-v38-index-layout.log` retain
the diagnostic evidence.

The bounded retry also timed out at 180 seconds: 129 graph GETs / 4.3 MB and
5,767 global-index GETs / 21.5 MB. This confirms substantial transfer reduction
but leaves serial request count unresolved. Neither timeout proves the linked
layout's query latency; `.build/history-v39-repack.log` records the retry.
No second background writer ran during either maintenance attempt.

The v39 backend includes range reads, direct links for newly indexed graph
frames, and removal of unary nodes when their index paths are rewritten.
Its SHA-256 is `2bfe5bae3e51f04c3113f9f3074301c9f374cfcfad4e4e3a70be68481638f7bd`.
The existing VM, bucket and repository data are retained. A cold remote-only
reader remains above target; a successful mounted query using local published
acquisition files must not be reported as a cold GCS result.

The restored v39 nested-KVM mount returned the following results with local
published acquisition files retained. The native baseline uses the same Linux
revision and disallows network fetching. These runs exclude acquisition and
index-building time; ingestion is still incomplete.

| Path / count | Mounted first result | Mounted total | Git total |
| --- | ---: | ---: | ---: |
| Kconfig / 1 | 337 ms | 341 ms | 122 ms |
| Kconfig / 10 | 260 ms | 1.084 s | 544 ms |
| COPYING / 1 | 863 ms | 873 ms | 471 ms |
| COPYING / 10 (four matches) | 920 ms | 2.319 s | 1.141 s |
| README / 1 | 36 ms | 40 ms | 14 ms |
| README / 10 | 39 ms | 1.154 s | 612 ms |

All six full commit-ID sequences match Git. First-result ratios and the cold
GCS case remain above target. Evidence: `.build/history-v39-mounted.log` and
`.build/history-v38-native.log`. The local attachment remains
`tmux -L gyit-user attach -t gyit-gce`, with the shell in the Linux repository,
`gyit` at `/usr/bin/gyit`, and mouse support enabled. The temporary diagnostic
binary was removed and its absence and zero diagnostic scratch directories
were verified. No VM, bucket, or separate GCS benchmark prefix was created.


Cold GCS latency for the linked, repacked layout is not yet established. Local
correctness and reader-only fixtures do not prove the Linux performance goal.

## Merge preparation and ancestry locality (v37)

A raw Go traversal at the pinned Linux revision visits 4,340 commits before
Kconfig's first result and 21,670 before ten results. The isolated runs took
212 ms and 1.011 s, respectively. These figures explain why one or more remote
requests per visited commit cannot meet the goal. The temporary instrumented
build only counted traversal calls; it did not alter query decisions, and its
result sequences were checked against native Git.

Index preparation was comparing every merge parent across the entire tree.
Default path history stops at the first TREESAME parent. Therefore, once a path
is unchanged from parent zero, differences from other parents are irrelevant.
Preparation now computes the first-parent changes first and prunes all other
subtrees from later-parent comparisons. First-parent changes contain their
ancestor directories, so pruning happens before loading unrelated tree objects.
Small changes use the existing bounded map; spilled large changes retain the
previous bounded full traversal. No query-specific index is built.

On the same 64 Linux commits and an empty 32 MiB acquisition decode workspace:

| Preparation | Time | Recorded paths | Allocated bytes | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Compare every merge parent fully | 189 ms | 97,129 | 103,154,816 | 1,517,760 |
| Prune paths unchanged from parent zero | 19 ms | 524 | 8,747,160 | 46,855 |

This is about 10x less preparation time and 185x fewer path records, not an
end-to-end import or cold-query result. A separate bounded sample of 512
first-parent commits took 407 ms and recorded 26,379 paths. Neither measurement
includes acquisition, frame publication or GCS latency.

Preparation now pushes parent zero to the front of its disk-backed frontier
and queues the remaining parents at the back. This keeps first-parent runs in
adjacent graph frames instead of interleaving every merge's side branches.
The persisted frontier preserves this order across restart; side branches are
still covered, and previously complete ancestors still terminate incremental
work. Reader date order, merge decisions and missing-coverage waiting remain
unchanged. Branch coverage can arrive later than breadth-first preparation;
this is a locality tradeoff, not a claim of faster availability for every path.
Existing immutable frames retain their old layout; this change groups newly
prepared frames.

The subtree guard regression failed before the fix when preparation read an
unrelated subtree. It now verifies parent decisions and Git-equivalent file and
directory logs with and without first-parent traversal. The frame-locality
regression failed at the third record because a side branch interrupted the
first-parent run; it also verifies all seventeen fixture commits are covered.
Full-suite, focused race, and WASM checks passed. Protobuf documentation was
regenerated with buf to state that later-parent differences may be omitted for
paths proven unchanged from the first parent.

Evidence: `.build/history-merge-before.log`, `.build/history-merge-after.log`,
`.build/history-first-parent-preparation.log`, and
`.build/history-read-probe-{first,ten}.log`. The preparation benchmark accepts
`GYIT_HISTORY_PREPARE_LIMIT` and `GYIT_HISTORY_PREPARE_FIRST_PARENT=1` for bounded
experiments. The raw traversal benchmark now accepts `GYIT_HISTORY_COUNT`.
The cold GCS query target remains unproven; no new whole-repository import was
run for these measurements.

The deployed v37 backend SHA-256 is
`9afe1bea98bb0a931f00e769dff9efbfbd411e7dc9ea25ae2e603fe5f8f0cb87`.
The existing mount and all user data were retained. Mounted Kconfig first/ten
results were 334 ms / 1.063 s; all six mounted file-history sequences matched
the pinned native Git digests. These reads still benefit from retained local
acquisition files. The tmux shell remains ready in the Linux repository.

A separate v37 reader with a new 32 MiB decoded cache and local acquisition
access disabled timed out without its first Kconfig result: 341 ms bootstrap,
19.539 s query, 570 GETs, 462,534,025 bytes, versus Git's 118 ms. The context
capped the whole case at 20 seconds. This is a failing performance result, not
a passing target or an inference from the small comparison probe. Reproduce
with `TestPublishedHistoryLogLatency/Kconfig/n1`,
`GYIT_HISTORY_REMOTE_ONLY=1`, and the Store/source/SHA variables. The source is
used only by native Git's baseline in that mode. `.build/history-v37-cold-reader.log`
and `.build/history-v37-mounted.log` retain the evidence. The four completed
preparation/traversal diagnostic binaries were removed and verified absent.

A second cold check with a five-second total deadline classified the reads:
including bootstrap, global-index reads were 66 GETs / 109.2 MB;
path/display reads were 60 GETs / 14.0 MB; graph reads were 13 GETs / 0.23 MB;
raw pack data was one 32 KiB read. No result arrived before the deadline.
The query phase alone made 134 GETs / 112.3 MB in 4.471 s, versus Git's 129 ms.
See `.build/history-v37-cold-classes.log`. This changes the immediate priority:
the real indexed cold query is dominated by repeated global commit-location
lookups and speculative metadata loads, not by the raw delta decoder measured
in isolation. Direct references between adjacent graph frames are the next
layout improvement to evaluate. They must respect immutable publication and
compaction; a reader must still fall back for links outside a covered prefix.
The opt-in benchmark now accepts `GYIT_HISTORY_READ_TIMEOUT` and logs request
classes even on failure. Its temporary remote executable was also removed and
verified absent. No new GCS prefix, bucket or VM was created in this iteration.

## Earlier v34 validation and test mount

The final full Go suite, targeted race tests for tree boundaries/publication caps,
and browser WASM build passed. The GCE backend at that point was v34, SHA-256
`d49bf5354000f4216d2073c657bf69df8e6b078ee1ed16cdf564d7d91bd587cb`.
The existing nested-KVM virtio-fs guest was restarted against its retained native
GCS store; `gyit` resolves to `/usr/bin/gyit`, and ten-entry logs for README.md,
AGENTS.md and package.json each exited successfully with ten results. This mount
smoke check supplements the Git-parity and partial-publication suite; it does not
repeat the fresh-store timing measurement.

Attach locally with `tmux -L gyit-user attach -t gyit-gce`. The remote session is
`gyit-fresh`. All temporary prefixes for the final profiling, preparation and
batch-size runs, plus adapter-test prefixes, were removed and explicitly listed
to verify absence. Temporary remote benchmark executables and the profile were
also removed and verified absent. The existing VM, bucket and interactive
mount's `github` data are retained for testing.

## Physical storage

All names below are relative to one repository's object-store prefix.

| Key / logical index key | Contents |
| --- | --- |
| `HEAD` | Protobuf manifest pointing to the global index; updated by CAS |
| `history/v2/commit/<sha>` in the global index | `HistoryBatchLocation`: immutable graph frame plus ordinal |
| `history/v2/ingestion/<tip>` in the global index | `HistoryIngestion`: pending frontier, covered count, completion, error |
| `index/progressive-history-v2-graph-<id>/<number>` | Packed zstd graph frames, without commit messages |
| `index/progressive-history-v2-graph-packed-<id>/<number>` | Compacted graph frames, with the same contents and new immutable locations |
| `index/progressive-history-v2-<id>/<number>` | Packed zstd path-posting, display, and frontier frames |

Graph and data objects are at most 256 KiB. Each referenced frame decodes to at
most 64 KiB of protobuf. There is no object per file or per commit.

The separate global lookup index targets 2 MiB containers. Its former 8 MiB
packing target repeatedly evicted useful data from small caches; a few hot pages
could require fetching the same large objects again. The reader retains the
8 MiB format bound, including single pages that exceed the packing target.

A graph frame covers up to 64 commits. It contains binary commit and parent IDs,
parent timestamps, a 2 KiB path Bloom filter, and references to a path dictionary
and a display frame. The dictionary uses sorted full paths, packed commit
ordinals, and bitmaps indicating which parents have different path values.
Directory paths and deleted paths are indexed too. A missing posting means
unchanged **only within covered commits**. Bloom filters can cause unnecessary
reads, but never omit a real change.

Author identities and messages live in separate display frames; their records
are interpreted when an entry is emitted. Bounded read-ahead may fetch display
containers earlier for batches whose path Bloom filter matches the query.
Separating graph containers prevents sparse file-log
queries from downloading unrelated commit messages. Path pages target 8 KiB and
use a bounded 32-way tree for large batches.

A history walk visits SHA-sorted lookup pages in a different order from commit
ancestry. When a lookup reads a branch page, it can prefetch sibling containers
whose key ranges intersect the history namespace. Each query admits at most
16 speculative index-page requests across eight index containers. Workers may
follow another index level within those caps and prefetch at most eight graph
containers referenced by the leaves. They do not walk graph ancestry themselves.
All index, graph and path-data speculation shares six active cache loads and
an eight-request queue; two of the eight cache-load slots remain available for
foreground readers. Foreground lookups still validate every reference and
decide traversal. Quitting cancels outstanding speculation.

In three cold-reader GCE runs against the existing GCS publication, this reduced
AGENTS.md query time from the preceding 181 ms measurement to 144/124/119 ms
(2.26x/1.98x/1.89x Git). README measured 282/294/264 ms (1.74x/1.78x/1.62x), and
package.json 315/293/283 ms (1.77x/1.63x/1.60x). These are query-only times with a
fresh 32 MiB decoded cache; bootstrap is separate. The same containers were
read, with more requests overlapped. The first AGENTS.md run still missed 2x;
fresh acquisition/first-ten latency remains a separate unresolved target.

A fresh-prefix rerun returned the first README result at 3.04 s and ten at
4.54 s versus Git 1.55 s (2.94x). Snapshot/history completed at 5.85 s, with
92,528,719 retained durable bytes. Cold query-only ratios on that new store were
1.81x/1.98x/1.59x for README/AGENTS.md/package.json, plus 103–162 ms bootstrap.
Fresh first-ten latency remained the unresolved target.

With joint pack/history publication and initial-batch grouping, a pinned-revision
comparison measured 4.59 s versus Git 1.71 s (2.68x), compared with 5.19 s for the
separate-publication build in that comparison. A subsequent run with large-history
compaction restored measured 5.09 s versus 1.61 s (3.17x), first result 3.98 s,
and snapshot/history completion 6.60 s. Acquisition timing varied between runs;
these results do not establish a consistent startup speedup. The final run
retained 92,471,535 durable bytes. Cold query-only ratios were 1.93x/2.39x/1.78x
for README/AGENTS.md/package.json, plus 129–153 ms bootstrap. Startup and the
AGENTS.md cold query still miss 2x. `GYIT_STARTUP_SHA` pins the benchmark revision
so upstream commits cannot silently change the comparison.

A subsequent instrumented run of the same build took 4.48 s versus Git 1.60 s
(2.80x); profiling overhead and network variability make this diagnostic, not
evidence of an improvement. The first useful history publication spent 311 ms
building its lookup index, 68 ms waiting for metadata uploads, and 79 ms updating
HEAD. Its writer-lock wait was below trace resolution. It also waited 70 ms for
pack staging after its history payload uploads finished. Across the full test,
history preparation used 4.78 CPU seconds, including 3.61 CPU seconds comparing
trees; pack staging used 0.89 CPU seconds. The remaining startup investigation
therefore needs to distinguish acquisition and pre-publication preparation,
rather than assuming writer-lock contention or treating CPU totals as wall time.
The deployed nested-KVM mount uses this build; its retained repository and cache
are not a fresh-start benchmark. Disposable measurement prefixes are removed
after each run.

Direct pack staging reduced `BenchmarkPackImport` (49,152 objects, fresh local
store each iteration) from 99–102 ms to 72–73 ms and from 171 MB to 64 MB of
allocated bytes per import on Apple Silicon. Its fresh GCS run still took
4.96 s to ten results versus Git 1.66 s (2.99x), retaining 94,116,843 bytes.
With equal-entry skipping, the older pinned revision measured 5.09 s versus
1.63 s (3.12x), retaining 94,103,895 bytes. Acquisition timing varied; these
component improvements have not established a consistent startup speedup.

An isolated acquisition comparison identified a separate revision effect.
Fetching the older SHA into an empty bare source took 2.09/2.18/2.18 s; fetching
HEAD took 1.43/1.41/1.44 s. Fetching the current tip by SHA took 1.42/1.37/1.40 s,
matching HEAD's 1.44–1.48 s in that comparison. Thus the SHA command itself was
not the cause. The baseline's full clone acquired the current branch before
querying the older revision, while gyit acquired that older revision directly.
No speculative default-branch acquisition was added to change this tradeoff.

The current-tip fresh GCS run returned its first result at 3.36 s and ten at
4.12 s versus Git 1.64 s (2.52x); snapshot/history completed at 6.83 s with
93,710,707 retained durable bytes. Cold
query-only ratios were 2.00x/2.47x/1.83x for README/AGENTS.md/package.json, plus
131–184 ms bootstrap. Both the old-revision startup and current-tip startup
still miss 2x, as does the cold AGENTS.md query.

Allocating a decoded object once from its bounded pack-header length reduced
the 4 KiB/64 KiB/1 MiB decode benchmarks by approximately 20–25%, with allocation
falling from 10.6/138/2228 kB to 4.3/65.7/1049 kB. The decoder still reads the
stream ending after filling the buffer, rejecting extra data, short bodies,
truncation and bad zlib checksums; the existing per-object and aggregate decode
budgets apply before allocation. The corresponding current-tip GCS run emitted
its first README result at 2.97 s and ten at 4.15 s versus Git 1.52 s (2.72x).
Snapshot/history completed at 6.87 s, retaining 91,729,223 durable bytes. Cold
query-only ratios were 2.14x/2.27x/1.65x for README/AGENTS.md/package.json, plus
117–246 ms bootstrap. First-page latency remains above target. Benchmark logs
now include both the queried SHA and the native clone's tip, making a historical
revision versus current-tip acquisition comparison explicit.

Read-ahead now excludes every alias of the foreground path container, including
display frames and later path frames in that same container. Previously an alias
could occupy a speculative worker waiting on the foreground load, delaying an
independent container. A gated regression requires five independent reads to
start together (one foreground plus the existing four speculative workers).
With 3 ms simulated request latency, that benchmark fell from 9.10–9.20 ms to
4.62–5.28 ms. Shared request limits and cancellation remain unchanged.

The corresponding fresh GCS run returned its first result at 3.40 s and ten at
4.08 s versus Git 1.69 s (2.41x). Snapshot/history completed at 6.59 s, retaining
93,665,098 durable bytes. Cold query-only ratios were 2.41x/2.68x/1.79x for
README/AGENTS.md/package.json, plus 133–153 ms bootstrap. The queried revision
was the preceding tip while the native clone acquired the newer tip. The
synthetic scheduling improvement therefore does not establish an end-to-end
speedup; fresh startup and two cold queries still miss the target. The temporary
GCS prefix and uploaded benchmark binary were removed after measurement.

Acquisition decoding now locates packed bytes without first inflating a delta
just to obtain its result size. The actual decoder still validates encoded
lengths, delta bounds, checksums and final object identity; metadata-only size
queries retain their size lookup. A regression requires one inflater start per
packed member. The cold 64 KiB delta microbenchmark remained about 68 us, while
allocations fell from 53 to 42. Its fresh GCS run was 4.05 s versus Git 1.65 s
(2.45x), so this alone did not close the startup gap.

The acquisition cache also gives a decoded object one LRU entry with both its
pack-offset and verified object-ID names. Both names disappear on eviction;
each entry has at most one alias, and the 32 MiB byte limit is unchanged. This
removes a duplicate allocation and duplicate budget charge. With a warm delta
base and a cold 64 KiB result, the benchmark fell from 38.9–39.4 us / 223 kB
allocated to 35.9–36.2 us / 149 kB. Tests cover decoded content, identity checks,
repeat reads, accounting and eviction of both names.

The corresponding fresh GCS run returned first/ten results at 3.29/3.96 s
versus Git 1.67 s (2.38x); snapshot/history completed at 6.47 s and durable
data occupied 93,635,549 bytes. Cold query-only ratios remained
2.22x/2.76x/1.91x for README/AGENTS.md/package.json, plus 138–190 ms bootstrap.
Both compared the same current tip. These instrumented runs do not establish a
consistent end-to-end gain, and the performance target remains unmet.
Their temporary GCS prefixes and remote benchmark files were removed.

A replay against the retained medium-repository fixture also ruled out changing
FIFO ingestion to a date-priority queue as a fix for this workload: both reach
the same ten README changes after 8,930 commits. Ingestion order remains unchanged.

The read-ahead window now has six shared active loads, retaining two foreground
slots. A sparse path can otherwise have five useful containers and no initial
foreground data read: four workers force a second round trip. Its gated
regression previously started only four of five reads; the fixed-latency
benchmark fell from 8.79–9.11 ms to 4.68–4.80 ms. Read-ahead also follows bounded
index branches to prefetch graph references found in leaf pages. Tests cover
both one- and two-level indexes, blocked authoritative lookups, shared budgets,
and cancellation. Hints cannot emit results before the authoritative lookup.

Metadata-container decoding now reuses one Zstandard decoder across its
independent frames. Each frame retains its checksum and decoded-size bound;
the entire decoded container remains capped at 4 MiB. A 48-frame benchmark fell
from 272–276 us / 1.71 MB allocated to 174–179 us / 435 kB. Frame independence,
corruption, truncation, expansion fallback and cancellation remain tested.

On the retained GCS store, three cold-reader runs with these changes measured
README 167–194 ms (1.01–1.16x Git), AGENTS.md 121–128 ms (1.87–1.95x), and
package.json 205–224 ms (1.12–1.22x). A fresh-store run still returned its first
README result at 3.28 s and ten at 4.02 s versus Git 1.63 s (2.46x).
Snapshot/history completed at 6.33 s with 94,037,374 durable bytes. That fresh
store's cold query-only ratios were 1.41x/2.35x/1.23x, plus 123–168 ms bootstrap.
Thus the retained-store results do not establish the target for a fresh layout;
startup and the fresh AGENTS.md query remain unresolved.
A subsequent read-only trace of that fresh layout measured AGENTS.md at
135/162/161 ms (2.12x/2.51x/2.52x). README included one 545 ms outlier before
288/267 ms; package.json was 284–299 ms. The trace exposes an additional serial
index-container read before path data begins, unlike the retained layout's
parallel index reads. The next investigation is the lookup tree's depth and
packing, rather than another increase in speculative concurrency. Temporary
GCS data, benchmark binaries, profile and native comparison clone were removed.

A wider global index was tested and rejected. Raising fanout from 128 to 512
reduced a 100,000-record test from three page levels to two, but increased a
single-record durable rewrite from 49,277 to 140,008 bytes. Fresh GCS first-ten
runs measured 4.61 and 4.28 s against Git's 1.69 s (2.74x and 2.54x), with no
consistent benefit to justify the write amplification. Fanout remains 128.
Leaf pages now also split at a 128 KiB encoded-byte target, allowing one larger
record within the existing 8 MiB hard bound. Tests retain prior snapshots and
check small-update bytes, large-record splitting and exact wire-size accounting.

The trace did reveal a separate scheduling error: aliases of the same index
container could exhaust the speculative queue before a second container was
admitted. Index read-ahead now admits independent containers first, then explores
additional pages within them under the same six-active/eight-queued and lifetime
budgets. A gated regression requires the second container to start while the
first remains blocked; the old scheduler fails that test.

With that fix and the original fanout, the fresh run returned first/ten README
results at 3.22/3.97 s versus Git 1.59 s (2.51x), storing 94,198,353 bytes.
The background completion notice appeared at 5.87 s, but the test's durable
completion observation returned at 15.64 s; that discrepancy is unresolved.
Cold query-only results were README 236 ms (1.42x), AGENTS.md 126 ms (1.95x),
and package.json 211 ms (1.17x), plus 120–170 ms bootstrap. Three subsequent
profiled cold-reader runs measured README 213–296 ms (1.30–1.79x), AGENTS.md
117–140 ms (1.83–2.14x), and package.json 204–227 ms (1.13–1.27x). These are
not a consistent 2x result. Startup remains the principal target miss; the next
measurement should separate its acquisition, preparation and publication waits.
All three temporary GCS prefixes, six remote benchmark binaries, the remote
profile and comparison clone were removed and their absence verified. The
local profile is retained. The user test mount runs the original fanout with
the scheduling fix and bounded leaf pages; all three file-log smoke queries
returned ten entries through nested-KVM virtio-fs.

A subsequent startup trace separated the publication delays: history payload
waits reached 277 and 386 ms, while the final graph-compaction CAS took 5.40 s
in one run. The trace also exposed incorrect progress reporting: completion was
announced before `SetState` returned, and its errors were discarded. Completion
notices now follow successful durable publication; failures reach the background
worker's paused status. Gated tests cover both blocked and failed publications,
and reopen a separate reader to verify a successful notice's durable state.

Immutable uploads now admit up to twelve requests under a separate 24 MiB
copied-buffer budget. Three maximum-size objects still exhaust that byte budget;
small 256 KiB history objects can overlap instead of waiting behind a fixed
three-request queue. Tests cover both limits and cancellation. A benchmark with
48 small objects and 3 ms request latency fell from 54.1–54.3 ms to 14.1–14.6 ms
without increasing total copied bytes. This does not establish an end-to-end
startup improvement: the next unprofiled fresh GCS run returned first/ten README
results at 3.28/4.01 s versus Git 1.70 s (2.36x). Completion was durably observed
at 6.22 s, just after its notice at 6.16 s; retained storage was 93,708,418 bytes.
Cold query-only ratios were 1.30x/2.06x/1.11x, plus 159–215 ms bootstrap.
The target remains unmet. The earlier traced run, with a slower acquisition,
took 5.59 s to ten results (3.30x) and 12.04 s to completion; these runs are not
a controlled before/after speedup claim. Its CPU profile attributes 4.11 s to
history preparation, including 2.68 s in tree comparison, and 0.83 s to index
lookups. Further work must distinguish acquisition variance, preparation CPU,
and avoidable manifest publications rather than infer a gain from one ratio.
Both temporary GCS prefixes and all four remote benchmark/profile files were
removed and verified absent; local profiles remain for further diagnosis.
The nested-KVM user mount runs these changes, with the CLI on PATH and ten
entries verified for each of the three file-history smoke queries.

The following revision removes the separate completion-status publication.
Snapshot readiness comes from the root tree's directory reference, and history
readiness comes from its ingestion record, both read through the same immutable
index root. Failed or blocked HEAD publication cannot advance those flags;
existing readers keep their earlier view. A new commit reusing a prepared tree
inherits snapshot readiness without inventing history coverage. The fresh GCS
run measured first/ten results at 3.39/4.11 s versus Git 1.67 s (2.46x), complete
background work at 6.42 s, and 93,604,305 durable bytes. Cold query-only ratios
were 1.61x/2.14x/1.21x. Removing redundant status writes has not demonstrated
a startup improvement.

A build-overlay experiment reduced the publication batch from 4096 to 2048
commits. It returned first/ten results at 2.93/4.19 s versus Git 1.61 s (2.60x),
completed at 6.31 s, and retained 96,501,778 bytes. Cold query-only ratios were
1.20x/3.44x/1.19x. This individual run does not establish a first-page benefit;
the production batch size remains 4096. These measurements used disposable
prefixes, separate from the retained user mount.

History writers now seed the existing decoded cache with their newly uploaded
containers, including compacted graph containers. Admission uses the reader's
4 MiB expansion bound and the same cache budget; it creates no second cache.
An oversized decoded container still falls back to individual durable frames.
Cached bytes do not publish coverage: blocked/failed HEAD CAS tests run with
writer admission enabled. A small ingested-repository regression verifies zero
history GETs on the first query with either memory or disk caching, and identical
results from a separate uncached reader.

The corresponding fresh GCS measurement returned first/ten results at
2.72/4.17 s versus Git 1.57 s (2.65x for ten), finished background work at 5.98 s,
and retained 93,660,352 bytes. Cold query-only ratios were 1.54x/2.00x/1.25x,
plus 146–214 ms bootstrap. This run does not establish a first-ten improvement;
startup remains above target. Comparing FIFO and date-priority traversal of the
local fixture gave identical coverage ranks for its ten README changes:
2492, 3096, 4238, 5869, 7275, 7777, 8410, 8412, 8890, 8930. Reordering that
ingestion queue cannot reduce the required work for this fixture.

The nested-KVM user mount now runs the cache-admission and derived-completion
changes. The CLI is on PATH; all three file-history smoke queries returned ten
entries in the guest. The disposable cache-admission benchmark prefix and
remote binary were removed and their absence verified.

A separate build overlay deferred background snapshot preparation until history
ingestion finished, to test resource contention. First/ten results were
3.07/3.83 s versus Git 1.63 s (2.35x); complete readiness took 9.46 s and durable
data was 93,633,633 bytes. Cold query-only ratios were 2.22x/2.11x/1.20x.
The experiment still missed the target and delayed snapshot readiness, so it
was not adopted. Concurrent snapshot/history preparation remains enabled.

The next current-build profile measured 3.98 s to ten results versus Git
1.61 s (2.47x), complete readiness at 6.10 s, and 93,568,795 durable bytes.
History preparation used 3.95 CPU seconds, including 2.71 seconds comparing
trees; decoding used 1.52 seconds. The first 2550-commit publication spent
72 ms waiting for payloads, 75 ms on pack staging, 219 ms building the index,
and 88 ms waiting for metadata uploads. Later 4096-commit publications spent
58–63 ms waiting for payloads and 15–27 ms building their indexes.

A local diagnostic found 2033 overlapping requests among 114,479 object loads
(1.8%). Increasing the acquisition workspace from 32 to 128 MiB reduced
uncached decodes only from 114,981 to 114,406 in its comparison. Neither an
extra coordination mechanism nor a larger workspace was adopted.

Decoded objects now allocate their one-byte cache header with their payload,
including delta results, eliminating a full payload copy on cache admission.
The cached-base decode benchmark fell from 35.7–36.7 microseconds / 149 kB to
31.1–31.8 microseconds / 75 kB. Object identity, length, checksum, delta bounds,
cache eviction, and decode concurrency checks remain in place. The fresh GCS
comparison returned first/ten results at 3.25/3.97 s versus Git 1.67 s (2.38x),
with full readiness at 6.49 s and 93,604,002 durable bytes. Cold query-only
ratios were 1.62x/2.07x/1.13x,
plus 158–213 ms bootstrap. This does not establish a consistent startup gain;
the first-page target and the AGENTS.md cold-query target remain unmet.
The buffer change is deployed to the nested-KVM user mount; all three file-log
smoke queries returned ten entries with the CLI on PATH. Both new GCS prefixes
and all remote benchmark/profile files were removed and verified absent.
Local diagnostic overlays and profiles remain under the ignored `.build` tree.

History path staging now uses the existing bounded external sorter instead of
inserting every path/ordinal pair into a temporary B-tree. The reader and durable
protobuf format are unchanged. Each frame retains at most a 1 MiB sort run;
larger inputs spill and merge in sorted order before path pages are written.
A regression exceeds that budget with 4096 raw-byte paths and four commit
ordinals each, verifies every parent mask and Bloom-filter membership, and
checks temporary-file cleanup. Existing merge, timestamp, publication failure,
cancellation and incremental-update tests exercise the new staging path.

`BenchmarkHistoryPathStaging` includes insertion and ordered traversal. On
Apple Silicon, 1024 records fell from 217–220 to 164–165 microseconds; 16,384
records fell from 54.0–54.3 to 3.3–3.5 milliseconds. The latter case allocated
3.17 MB versus 6.13 MB, with more small allocations during external merging.
The fresh GCS run returned first/ten results at 3.48/4.25 s versus Git 1.59 s
(2.67x), completed at 6.46 s, and retained 93,486,582 bytes. Full acquisition
became available around 2.01 s, versus 1.65 s in the preceding run; these runs
do not establish an end-to-end speedup. Cold query-only ratios were
1.63x/2.53x/1.31x, plus 154–277 ms bootstrap. The 2x target remains unmet.
The sorted-staging build is running in the retained nested-KVM mount, with
the CLI on PATH and ten results checked for each of the three smoke paths.
Its temporary benchmark prefix and remote binary were removed and verified
absent; the user mount's durable repository data was preserved.

## Publication and resumption

The first complete graph frame publishes when its prerequisites are ready.
While pack import is still pending, additional ready frames join that first
publication, up to the same 8192-commit limit. Subsequent frames share a
publication, capped at 8192 commits or approximately one second of processing.
The final frame splits at the remaining commit budget even when earlier frames
were shortened by large messages; the cap does not round up to a frame boundary.
Read-frame size and publication size are independent: small reads must not force
hundreds of remote CAS operations. GCS documents a one-write-per-second limit
for rapidly replacing one object; overly frequent HEAD updates can be throttled.
[Cloud Storage object documentation](https://docs.cloud.google.com/storage/docs/objects).

All referenced immutable data finishes uploading before HEAD changes. Coverage
and the remaining work frontier are published in that same CAS. Failed CAS does
not expose the new records. A restarted writer resumes the stored frontier;
completion markers let ordinary branch updates stop at already-covered ancestry.

One publication may run while ingestion prepares the next batch. The worker
owns a frozen copy of the index changes, spilling after 1 MiB; it never borrows
the producer's database transaction. Payload uploads finish before taking the
shared writer lock. The next publication waits for its predecessor, so CAS,
coverage, and completion stay ordered. Cancellation joins the worker and removes
temporary staging. A failed CAS prevents later batches from being published.
Tests hold the first HEAD write open while the next graph upload proceeds,
then verify complete Git parity, cancellation, and rejected-CAS behavior.

Full ancestry acquisition also overlaps pack import with history preparation.
`ImportHistoryPacks` owns both operations until they finish. It can read the
stable local acquisition before its packs are published, while ordinary
`IngestHistory` still accepts only already-published local packs. Prepared
history uploads may run early. Pack recipes use a separate temporary sorter
with a 1 MiB run budget. Recipes stream directly from pack indexes into this
sorter instead of first building and copying a temporary B-tree. After pack
uploads finish, the first history publication
includes those recipes and pack markers in its own CAS. No intermediate HEAD
exposes just the new pack recipes. Both sorters own their bytes and are removed
after the operation. Failed uploads, failed CAS, and cancellation join the
workers without exposing new recipes or coverage. A shallow
input with no coverable commits still finishes its pack import before returning
pending coverage; an already-covered tip also publishes any new packs on their
own. No durable format or additional cache tier is introduced.
Upload failures retain their original storage error when cancellation stops
the queue; caller cancellation still reports cancellation. Single-CPU and race
tests exercise both paths.

Once full acquisition finishes, the background controller cancels redundant
shallow acquisition/indexing and waits for the combined import. That prevents
a shallow pass from keeping an outdated local pack view while full objects
become available remotely. Until then, shallow windows can publish progress
independently of the slower full transfer.

If acquisition provides no new covered commits and history remains incomplete,
the writer retains the previous frontier without publishing another generation.
With no frontier yet, the selected tip is the continuation. Completion still
publishes even when its commits were already indexed for another tip. Setting
an unchanged acquisition error also avoids a write, but checks the durable HEAD
version so a stale writer cannot silently clear another writer's failure.

Tree differences also use bounded look-ahead: up to seven workers prepare parent
commits, keeping at most sixteen pending results, while the caller consumes the
original disk-backed frontier. Only the caller adds records to frames or
advances coverage. A prepared older commit therefore cannot bypass a missing
newer one. The caller computes synchronously when speculative capacity is full;
speculative queue admission never blocks. Shutdown cancels and joins every
worker before acquisition mappings are released.
The worker count scales down with available CPUs, reserving one for the caller;
at least one worker remains to overlap I/O on a single-CPU process.

Tree comparison skips byte-identical runs at complete entry boundaries. It
still validates one copy of every skipped entry; a common byte prefix ending
inside a name or binary object ID cannot hide a change. Differing entries use
the same ordered merge. On Apple Silicon, the 10,000-entry sparse-change
benchmark fell from 328–329 microseconds to 199–201 microseconds, with unchanged
allocation counts. Probing for another run only after an unchanged entry avoids
repeating failed probes throughout a dense rewrite. Changing every entry measured
1.13 ms versus the preceding 1.09–1.12 ms. The real-repository startup runs above
do not establish an end-to-end speedup
from this tradeoff.

Each ingestion pass pins its initial coverage-index root. Snapshot and pack
publications may advance HEAD while it works, but cannot change that pass's
previously covered history. Its disk-backed seen set handles commits processed
during the pass. Object acquisition and final CAS publication still use the
current root, so concurrent snapshot state is preserved. This avoids repeatedly
reading a new global index only to check the same coverage records.

Each commit accumulates changed-path parent masks in a charged 1 MiB working
map. Larger changes spill through the existing external sorter, whose run
budget is another 1 MiB. Spill keys contain both path and parent ordinal, so
merging runs preserves every parent's bit. Small commits avoid creating a
temporary file. The acquisition object cache remains 32 MiB. Object decoding
uses two to four slots per repository, scaled with available CPUs. Foreground
reads have an additional two-slot limit; acquisition may use all global slots.
This adds no durable format or cache tier.

Object loaders use their decoder budget rather than also holding page-fetch
slots. Otherwise simultaneous object loads could occupy every page-cache slot
while all wait for nested object-index reads. Regression tests cover memory and
disk caches, mixed acquisition/foreground loads, and a single page-fetch slot.

After ingestion completes, graph compaction combines fragmented graph containers.
Small updates wait until at least four containers have accumulated. A larger
completed batch (at least eight frames across multiple containers) also packs
its newest container densely, even with fewer publications. It packs the existing
frames into 256 KiB objects, then publishes replacement locations and a
`graph_compacted` closure marker together by CAS. Path postings, messages, and
file bodies are unchanged. Future updates stop at compacted ancestor tips;
small updates accumulate before another packing pass. Old objects remain valid
for readers pinned to earlier publications. Compaction uses disk-backed staging,
and a rejected CAS leaves both the durable HEAD and local reader root unchanged.

Commit and tree acquisition is blobless. For a previously unavailable selected
revision, the full ancestry transfer starts alongside the mount's depth-one
setup. This early transfer does not publish objects or coverage: the background
importer still uploads its packs and publishes them before indexing may use them.
There is at most one early transfer per repository, it reuses the existing
ancestry acquisition lane, and filesystem shutdown cancels and joins it. A
failed mount may retry using a completed transfer; no reader depends on those
local files after publication.

The full fetch also runs alongside bounded shallow windows in separate
acquisition directories. The first window
deepens by 64 levels immediately; later windows grow to a maximum of 16,384
levels, giving the full fetch one second to finish before each additional
window. A stalled full fetch therefore cannot prevent intermediate coverage.
Both directories retain refs for incremental negotiation. Once full acquisition
finishes, redundant shallow fetch/index work is canceled and the combined import
takes over. Published shallow coverage remains readable. Index construction
still publishes bounded newest-first batches.
Canceling a fetch terminates its isolated process group so Git's HTTP helper
cannot outlive the operation; cancellation/retry is tested on macOS and Linux.
It does not wait
for unrelated branches or tags. Current snapshot contents download independently
in a separate acquisition directory. Snapshot metadata construction holds the
writer lock only for its final publication, so neither a slow content download
nor directory-page upload prevents history batches from becoming visible. Tree comparisons skip identical subtrees and
stream changed tree entries; they do not fetch historical file bodies. Work
queues, per-commit differences, and postings spill to a temporary bbolt database.

## Reader behavior

The traversal preserves commit-date ordering, parent insertion order, merge
TREESAME simplification, and first-parent behavior. Parent timestamps travel
with covered records, so a valid current result does not wait for its parents'
indexes. Results are never reordered after emission.

At a coverage gap, the same request refreshes the manifest and waits. A gap is
not EOF. Local publication wakes readers; independent readers poll only at gaps.
An acquisition error is reported at the gap without hiding already-covered
results. A count limit, callback error, or canceled reader stops that query; the
repository-wide background writer continues. The existing protobuf command
transport provides streaming, disconnect cancellation, and socket backpressure.

The terminal CLI sends results directly to the pager through an OS pipe. It
does not spool the complete log before opening `less`. Omitted `-n` means stream
through the real end of history; explicit `-n 0` produces no output. The protobuf
request carries an explicit `unlimited` bit, distinct from older callers' count
defaults. There is no implicit log deadline while reading a page or waiting for
coverage. `--timeout` remains available, and quitting the pager cancels that
request. The existing connection, frame, decoded-cache, and traversal budgets
still apply. Native traversal starts with a 1 MiB memory window and at most 4096
queued candidates. When either fills, both duplicate suppression and the
timestamp-priority queue move to a temporary mapped B-tree. Keys preserve signed
timestamp order and stable insertion ties; candidate values use protobuf.
Transactions checkpoint every 256 mutations. EOF, errors, and cancellation
close and remove this query-local scratch file; it is neither repository data
nor a persisted query-result cache. The browser demo has no native scratch
filesystem and reports an explicit limit if it exceeds its memory window.

A terminal regression test holds the producer open until a real pager consumes
its first line. CLI parity tests include more than 1000 results, and transport
tests check that a waiting log has no implicit deadline and disconnect cancels
its handler. The buffered pager path is retained for `show`.

Branch/tag advertisements are published at mount or explicit update. Revision/path
disambiguation reads those stored refs; an ordinary file-log query does not run
`ls-remote` against GitHub. SHA-pinned mounts can reuse already-imported data offline.

Durable index data is separate from the single bounded decoded cache. A query
also holds a bounded eight-frame decoded working set and the existing bounded
traversal frontier. Cache eviction never removes coverage or repository data.

## Measurements and remaining validation

The earlier read-ahead baseline measured: mount ready **1.17 s**, first README result **6.73 s**,
first ten **9.27 s** versus Git **1.63 s (5.70x)**, complete snapshot/history
**10.38 s**, and **94.05 MB** retained durable data, including raw packs and
superseded immutable metadata. This uses bounded read-ahead, parallel full/shallow acquisition and
direct public-repository validation. A direct lookup uses GitHub's
[`GET /repos/{owner}/{repo}`](https://docs.github.com/en/rest/repos/repos#get-a-repository),
reuses a fresh owner listing when available, and otherwise caches at most 256
validation results. Reading an owner directory still lists all public repos.
The previous parallel-acquisition run with owner pagination took 3.69 s to
mount and 12.35 s for ten results (7.88x Git), retaining 94.68 MB.

Before read-ahead, completed-store cold queries measured README **485 ms**
vs Git 164 ms (**2.95x**), AGENTS.md **218 ms** vs 64 ms (**3.42x**), and
package.json **441 ms** vs 179 ms (**2.46x**). Snapshot opening added
115–181 ms. Each query uses a new 32 MiB decoded cache and is denied Store writes.

With bounded graph-guided read-ahead, three cold queries per path against the
retained GCS store gave these medians: README **264 ms** vs Git **162 ms**,
AGENTS.md **125 ms** vs **63 ms**, and package.json **295 ms** vs **179 ms**.
Individual query ratios were **1.51–1.64x**, **1.95–2.09x**, and
**1.58–1.69x**, respectively. These exclude snapshot opening; three runs are not
a percentile guarantee, and the shortest query still had one miss. Against the
newly built store from the fresh-start run, cold queries were **281/149/299 ms**
(**1.73/2.35/1.69x**), with another **111–191 ms** for snapshot opening.
Fresh import latency and the shortest cold query remain unresolved, so the 2x
goal is not complete.

Overlapping publication was compared against a synchronous build with the same
reader and acquisition code. The traced pair returned ten entries in **9.47 s
versus 10.14 s**; an untraced pair measured **10.37 s versus 11.37 s**. Git's
clone-plus-query baseline was **1.58–1.71 s**. These are individual paired runs,
not a latency guarantee: an earlier asynchronous run took **11.34 s**. The
untraced asynchronous run returned its first entry at **6.94 s**, completed
snapshot/history at **11.38 s**, and measured cold query-only times of
**334/109/318 ms** for README/AGENTS.md/package.json (**2.06/1.70/1.80x** Git),
plus **105–150 ms** opening. Its retained Store size was **95.39 MB**, versus
**93.14 MB** for the synchronous pair; publication boundaries depend on elapsed
time, so superseded immutable metadata sizes vary. Fresh-start performance
remains well above 2x.

The asynchronous trace attributed **4.64 CPU seconds** to history ingestion
(including compaction), with **2.28 seconds** in tree differences. Across all
publications, HEAD updates occupied **3.21 seconds**, including a single
**1.22-second** update; these overlapping durations must not be added to the
end-to-end latency. Publication overlap reduces idle time but does not remove
the all-path index's CPU cost or variable remote HEAD latency.

Starting full ancestry acquisition alongside depth-one mount setup reduced
first-ten latency to **7.26 s and 7.24 s** in two fresh GCS runs, versus Git
**1.64 s and 1.60 s** (**4.42x and 4.53x**). Disabling just that overlap in
the same build took **10.71 s**. Mount readiness remained **1.18/1.14 s**;
first entries appeared at **5.02/5.40 s** and complete snapshot/history at
**12.89/10.56 s**. The first run retained **95.43 MB**, compared with **95.36 MB**
for the disabled-overlap run; the repeat retained **95.56 MB**. Completion
latency remains variable.

The repeat run's completed-store cold queries were **325/123/292 ms** for
README/AGENTS.md/package.json (**1.97/1.91/1.63x** Git), plus **129–169 ms**
opening. The earlier run still missed on AGENTS.md (**2.04x**), so these runs
do not establish a consistent cold-query bound. Fresh first-ten latency
still exceeds the 2x target. Regression tests hold depth-one setup
open while full acquisition completes, verify those local objects and coverage
remain unpublished, and compare the eventual file history with Git. Shutdown
tests verify cancellation, process cleanup, and acquisition-lane release on
macOS and Linux.

Bounded parallel commit preparation returned ten entries in **6.22 s and
6.26 s** in fresh GCS runs, versus Git **1.63/1.60 s** (**3.82x/3.91x**).
First entries appeared at **4.21/4.24 s**; complete snapshot/history, including
final state publication, took **12.16/10.09 s**. One serial-preparation GCS
comparison took **12.07 s** for ten entries; remote publication latency makes
that single comparison noisy. The first parallel run retained **94.44 MB**,
versus **93.29 MB** for the serial run; the repeat retained **93.82 MB**.

An isolated local-Store comparison on GCE, using the same complete blobless
acquisition, measured first-ten/complete-index times of **3.09/3.51 s** with
three workers and **3.98/4.41 s** with serial preparation. An instrumented run
observed **11,511 preparations for 11,511 distinct commits**, ruling out
duplicate commit preparation in that workload. Parallel execution still used
more process CPU in the full GCS benchmark (**9.76 s versus 7.91 s**). The
potential decoder/cache contention and publication latency need investigation.

The repeat parallel run's cold queries were **346/171/281 ms**, respectively
**2.17/2.73/1.61x** Git, plus **121–143 ms** opening. Neither the fresh-start
target nor a consistent 2x cold-query bound has been established.

Skipping unchanged partial coverage and error publications did not materially
change fresh-start latency: **4.20 s** to the first result and **6.33 s** to ten,
versus Git **1.65 s** (**3.84x**). Complete snapshot/history took **9.96 s**,
with **94.50 MB** retained. Cold README/AGENTS.md/package.json queries measured
**382/153/289 ms** versus Git **163/63/177 ms** (**2.35/2.43/1.63x**), plus
**127–173 ms** opening. These are individual runs, not an established bound.

A following trace run measured **6.34 s** to ten results versus Git **1.61 s**.
It showed **0.48 s** total history-writer-lock waiting and **3.97 s** total HEAD
write time, including **2.93 s** in two late writes after the first ten results.
Those late writes explain delayed completion but not startup. A separate local
acquisition diagnostic observed **883 overlapping loads out of 113,934**, too
few to justify treating duplicate concurrent loads as the primary bottleneck.

Pinning coverage for each ingestion pass measured **3.97 s** to the first result
and **5.90 s** to ten versus Git **1.57 s** (**3.76x**). Full snapshot/history
took **8.82 s**, with **93.87 MB** retained versus **94.46 MB** in the trace run.
Cold query times were **320/115/286 ms** versus Git **163/63/178 ms**, plus
**112–165 ms** opening. This individual run meets the query-only ratios but
does not establish a consistent cold bound, and startup still misses 2x.

Overlapping pack import and history preparation measured **3.10 s** to the first
result and **5.16 s** to ten versus Git **1.57 s** (**3.30x** for ten). Complete
snapshot/history took **6.57 s**, with **93.92 MB** retained. Cold query times
were **338/124/288 ms** versus Git **162/63/176 ms**, plus **111–156 ms** opening.
The first result is approximately 2x in this run, but first-ten startup and a
consistent cold-query bound remain unmet. Preparation consumed **4.39 CPU s**,
including **3.18 CPU s** comparing trees; overlap reduced elapsed time rather
than eliminating that work.

Path-page construction now tracks protobuf size incrementally rather than
rescanning every prior entry on each append. `BenchmarkHistoryPathPages` fell
from **4.52 ms to 1.21 ms** for 4,096 paths (Apple Silicon); output remained
**208,022 encoded bytes in 27 pages**. On the same GCE acquisition with this
change, first-ten/complete-index times were **2.79/3.13–3.17 s** with three
workers, **2.58–2.60/2.90–2.91 s** with seven workers and two decoders, and
**2.29–2.30/2.62–2.69 s** with seven workers and four decoders (two runs each).

The combined fresh GCS run returned the first result at **3.13 s** and ten at
**4.58 s**, versus Git **1.59 s** (**2.89x** for ten). Snapshot/history completed
at **6.29 s**, with **92.44 MB** retained. Cold README/AGENTS.md/package.json
queries took **302/180/324 ms** versus Git **164/63/180 ms**, plus **123–154 ms**
opening. Startup and a consistent cold-query bound remain above target.

Read-ahead inspects neighboring headers in an already-cached graph container,
using path Bloom matches to warm path/display containers. It adds no durable
data and uses the same globally bounded decoded cache. At most six speculative
loads run per shared cache, leaving two foreground cache slots available; each
query queues at most eight more containers. Pager backpressure cannot grow that
queue. A busy speculative budget drops hints instead of holding up a shared
fetch needed by foreground readers. The budget stays held until the actual
loader exits, even if its waiter cancels sooner. Limits, cancellation, and
pager exit cancel outstanding speculative work.
Foreground reads still validate authoritative frame references and determine
traversal order. A live reader retries when another reader owning their shared
fetch cancels; it does not inherit the other reader's cancellation.

The acquisition tests hold the full-fetch lane unavailable and require a file
older than the first 64 levels to appear before that lane is released. A real
stalled HTTP fetch must disconnect its helper, leave no Git locks, and allow a
successful retry after cancellation. Both tests run on macOS and Linux.

Repeatable test: `TestFileHistoryMedium`, with `GYIT_HISTORY_SOURCE` pointing to
an existing packed Git directory. Optional `GYIT_HISTORY_SHA` selects its tip;
`GYIT_HISTORY_STORE` selects a **new isolated** local or GCS prefix. The default
uses a temporary local store. The test compares formatted output to Git and
reports both cold bootstrap and the first query on the opened snapshot.

Local source, 10,934 commits: indexing 3.05 s, 12.69 MB of additional writes;
first matching README entry 1.22 s, first ten 3.05 s. Six fully indexed cold
reader comparisons were 0.48–0.84 times Git. These are single runs, not a latency
percentile claim, and acquisition was already complete.

GCE/native GCS, 11,510 commits, fresh isolated store: importing existing acquired
packs took 25.70 s; the new history index took another 12.34 s and wrote 19.63 MB.
During indexing, README's first entry arrived at 4.28 s and its first ten at
11.86 s. These times start after pack import, not at a fresh GitHub clone.

With an empty reader cache, queries after opening the snapshot still miss 2x:
README n10 586 ms vs Git 167 ms (3.51x); AGENTS.md n10 404 ms vs 63 ms (6.38x);
package.json n10 581 ms vs 180 ms (3.22x). Cold bootstrap adds 150–213 ms.
The AGENTS.md n100 cold query exceeded one second (1.09 s). Queries perform
11–31 history-object GETs in these runs; remote round trips remain the bottleneck.

In the live nested-KVM guest, cached ordinary `gyit log -n 10 FILE` returned
byte-identical formatted output to Git through the normal virtio-fs baseline.
Five-run medians: README 55 ms vs 187 ms; AGENTS.md 34 ms vs 83 ms;
package.json 67 ms vs 207 ms (0.29–0.40x). These cached figures do not erase the
cold GCS misses above. The CLI is on PATH and the interactive pager was tested.

`TestFreshFileHistoryStartup` compares an empty mount/store against Git's bare,
single-branch blobless clone plus `log -n 10 -- README.md`, pinned to the same
commit. Set `GYIT_STARTUP_STORE` to a disposable prefix and
`GYIT_STARTUP_REPOSITORY` to a public owner/repository. It reports mount-ready,
first-result and first-ten latency, and compares exact commit IDs. The caller
must remove its cloud prefix; temporary local clones/state are test-owned.

The initial fresh-start measurement was 78.54 s vs Git 1.60 s (49x), with the
first result at 57.72 s. Separating snapshot acquisition reduced that to
47.04 s vs Git 1.83 s (25.65x), first result 25.81 s; it exposed the remaining
snapshot publication-lock coupling, now covered by a blocked-upload regression.
After narrowing that lock: first result 25.24 s, first ten 45.17 s versus Git
1.65 s (27.36x); the lock fix prevents starvation but did not explain the bulk
of this fresh-start latency. Fresh startup and
cold remote reads remain performance work; cached timings are not the acceptance
criterion for those cases. A repeat with phase tracing took 48.02 s (31.42x):
64 commits were covered at 5.65 s, 192 at 8.25 s, 960 at 20.05 s, and 1984 at
23.35 s. README's first matching change arrived only in the next window, at
27.45 s. Thus a valid newest-first prefix appears early, but sparse paths can
still wait through many acquisition/publication windows. The 2x expectation is
not met for this fresh-start case. The old complete-
parent-history builder and query path have been removed. Synthetic parity tests
cover merges with skewed clocks, deletions, type changes, empty roots, paged path
dictionaries, partial coverage, restart, cancellation, and publication failures.

### Follow-up measurements

Retaining acquisition refs and matching Git's promisor fetch behavior
(`fetch.negotiationAlgorithm=noop` for explicit missing-blob wants) avoids
repeated history transfer without breaking lazy file reads. Progress text no
longer publishes a durable HEAD for every phase. Large pack payload uploads now
drain before the shared publication lock; the new regression blocks those
uploads while requiring independent history results to remain available.

With these changes, a fresh native-GCS mount reached ready at 3.41 s, its first
README result at 19.01 s, and ten at 30.50 s versus Git's 1.59 s (19.18x).
The first 64 commits were indexed at 4.61 s. The fresh-start target remains unmet.

`TestFileHistoryReadPerformance` is a read-only probe of an existing store:
set `GYIT_HISTORY_READ_STORE`, `GYIT_HISTORY_SOURCE`, and `GYIT_HISTORY_SHA`.
It denies Store writes, uses a new bounded cache per path, compares exact
formatted output to Git, and records each object request. Against the live GCS
publication, one cold README query took **1.52 s** versus Git 165 ms; its history
reads were ten graph objects and eight path/display objects. AGENTS.md took
592 ms versus 63 ms, and package.json 932 ms versus 182 ms. These timings vary
with GCS latency; they confirm fragmentation of the graph across frequent
publications as a source of serial round trips.

After graph compaction, the same live GCS store and revision, with a new empty
cache for each query, measured README 519 ms vs Git 166 ms (3.13x), AGENTS.md
228 ms vs 63 ms (3.61x), and package.json 428 ms vs 181 ms (2.37x). README's
graph GETs fell from ten to three; AGENTS.md's from six to one. Output matched
Git exactly. These measurements exclude snapshot opening on both sides of the
gyit query timer and remain single-run observations. None of these three query
times exceeded one second; the earlier slow runs remain recorded above.

The local 10,934-commit ingestion, including compaction, took 3.69 s and wrote
15.64 MB of additional immutable data; first result 1.23 s, first ten 3.28 s.
This includes retained pre-compaction objects, not just the final live graph.
Cold GCS reads and fresh-start first-ten latency still miss the 2x expectation.
Bounded read-ahead and reducing the sequential acquisition windows remain
performance work; cached timings do not establish success for those cases.

`TestFileLogBeyondVisitedMemoryWindow` can be expanded with
`GYIT_TRAVERSAL_COMMITS=100010`. The 100,000-commit mostly unchanged synthetic
history exceeded a 40-second ingestion deadline with 8 MiB lookup-index
containers. With the 512 KiB target, pack import took 299 ms, history ingestion
16.99 s, and the complete file-history read 929 ms (ingestion cache retained).
The smaller normal fixture and a merge tree with 8192 pending leaf candidates
compare the entire streamed commit sequence to Git. Separate traversal tests
exercise 120,010 candidates, repeated timestamps, signed timestamp extremes,
distinct rename paths, cancellation cleanup, and scratch-storage failure.

The 2 MiB compromise indexed the same synthetic history in 16.43 s. Reading
its full file history took 907 ms versus Git's 266 ms (3.41x), with ingestion
cache retained. Smaller containers did not fix fresh GCS startup: the 512 KiB
experiment took 38.12 s to return ten README entries versus Git's 1.59 s,
and 2 MiB took 41.25 s versus 1.54 s. These individual runs include variable
remote latency. The latter completed snapshot/history ingestion at 46.09 s
and retained 136.69 MB of durable objects, including current snapshot contents,
raw packs, and older publications. Cold query-only times with 2 MiB containers
were README 482 ms vs 165 ms (2.93x), AGENTS.md 293 ms vs 63 ms (4.64x), and
package.json 470 ms vs 178 ms (2.64x); bootstrap added 134–178 ms. All three
still miss the target. The next investigation isolates the long publication
stall from sequential acquisition overhead.

The runtime trace found little publication-lock contention (under 100 ms total)
in its run, but 6.72 s waiting on HEAD writes, including 3.75 s in retry backoff.
Reducing acquisition rounds cut first-ten latency to 24.79 s versus Git 1.62 s.
The CPU profile then exposed 3.99 CPU seconds spent in SHA-256, mostly repeatedly
verifying cached index pages. The shared bounded cache now admits verified
pages independently of their enclosing containers. A page is verified on load;
eviction requires verification again. Cached pages own their bytes, including
when their source container is evicted or a waiter cancels.

With both changes: first README result 13.73 s, first ten 19.71 s versus Git
1.65 s (**11.97x**), snapshot/history complete 35.92 s, retained durable bytes
88.06 MB. SHA-256 CPU fell to 0.47 s, but tree comparison and allocation remain
substantial. Cold README/AGENTS.md/package.json queries were respectively
574/300/408 ms versus Git 162/65/179 ms (**3.54x/4.64x/2.29x**), plus 154–161 ms
bootstrap. These are individual comparative runs; fresh and cold targets remain
unmet. The temporary GCS prefixes are removed after measurements; the live
nested-KVM test mount retains its repository data.

Tree comparison now borrows immutable tree bytes, compares binary names/OIDs,
and allocates paths only for differing entries. Files do not allocate empty
tree readers. `BenchmarkHistoryTreeDelta` compares 10,000-entry trees: a one-file
change fell from 1.58 ms / 3.51 MB / 80,024 allocations to 0.319 ms / 672 bytes /
15 allocations; changing every file fell from 9.56 ms / 88.30 MB to 1.10 ms /
2.08 MB (Apple Silicon, two one-second runs). Git parity tests cover insertion,
deletion, executable bits, symlinks, file-to-directory transitions and directory
ordering; parser tests also cover raw names, historical modes and truncation.
The corresponding fresh GCS first-ten run was 17.70 s versus Git 1.64 s (10.79x).

Snapshot preparation can now read already-published acquisition packs locally
instead of re-downloading their trees from GCS. Pack publication markers are
checked before using this path; deleting acquisition files afterwards leaves
readers fully functional from durable Store data. Its acquisition lane stays
locked through preparation, independent of the history lane. In the next fresh
GCS run, first result was 14.62 s, first ten 18.29 s versus Git 1.71 s (10.68x),
and complete snapshot/history preparation fell to 19.50 s from 36.82 s. Retained
durable data was 87.78 MB. Cold query-only ratios were 3.15x/4.73x/3.27x for
README/AGENTS.md/package.json. Startup and cold-query targets remain unmet;
tree decoding/allocation, acquisition rounds and serial remote query reads
remain the measured areas to improve.

Replacing the later shallow expansions with one unshallow fetch measured 16.01 s
to ten README results. Fetching into a separate non-shallow source reduced the
full acquisition itself from about 3 s to 1.4 s, and first-ten latency to 15.24 s
versus Git 1.67 s (9.13x). Retained durable bytes were 95.92 MB; this includes
the small initial shallow pack and the full ancestry pack, not just index data.

Local acquisition is now filtered by durable pack publication markers before
history indexing uses it. A concurrent fetch can create local files before its
Store import completes; those files must not allow coverage to claim completion.
A regression demonstrates pending coverage before publication and complete
coverage after import. A separate integration test reaches a sparse path's
creation beyond the initial 64 levels and reads it after removing every local
history acquisition directory.

Acquisition views initially retained at most two zlib decoder workspaces for
their lifetime (the wider worker pool now retains at most four).
`BenchmarkHistoryInflater` measured allocation dropping from 40,680 to 52 bytes
per decompression (10.8 to 7.6 microseconds); partial delta-header reads and
malformed-input recovery are tested. The fresh native-GCS run with this change
returned the first README result at 10.66 s and ten at 13.66 s versus Git 1.55 s
(8.80x). Snapshot/history completed at 14.47 s, with 93.18 MB retained durable
data. Cold query-only ratios remained 3.16x/4.56x/2.88x, plus 111–179 ms bootstrap.
History-index CPU fell to 4.50 s, including compaction; startup still spends
substantial time before indexing begins. These results remain above target.

## Trailing-slash directory history regression (September 28)

The fresh interactive Linux session exposed a separate bug from cold-object
layout latency. `log lib` selected the indexed literal-path walker, while
`log Documentation/` was excluded by `Matcher.LiteralPaths` and recursively
walked filesystem directory metadata. That route could demand blob contents
for sizes even though history comparison needs only tree identities. The
initial explanation that the first result necessarily needed deeper ancestry
was incorrect.

Regression: adding `nested/` to `TestIngestedHistoryFirstQuery` failed with
“first file-history query read a historical Git pack”. Literal directory paths
now retain their slash and select the history walker. Version-3 history frames
include directory-only postings, excluding same-named ordinary-file changes.
Version-2 frames recheck changed values directly from root/ancestor trees;
they do not enumerate descendants. Path/revision disambiguation also resolves
literal identities directly. Directory/file/symlink replacements and merges
are compared against Git in `TestDirectoryHistoryTypeChanges`.

An isolated GCE replay of the original four acquisition packs (81,221,763 bytes
including indexes), with further acquisition forbidden, returned the first
`Documentation/` result in 23.7 ms and seven results before reaching genuinely
missing history. `lib -n 10` completed in 197 ms. Both use the same repository
window. The ten-result directory test correctly fails at the coverage gap;
it must not treat seven results as completion. Evidence is in
`.build/history-directory-v46-shallow.log`.

This does not establish the performance goal. Native Git in that replay took
2.8 ms for the first directory result and 13 ms for ten lib results. Existing
GCS-only version-2 data with a new 32 MiB cache took 1.08 s for the first
directory result (plus 534 ms bootstrap); ten results hit the 10-second deadline
after four outputs. Evidence: `.build/history-directory-v46-cold.log`.

A separate extra-64-level shallow fetch was stopped at 45 seconds, so merely
replacing the 16x depth growth with a smaller constant is not justified by the
measurement. The diagnostic used temporary acquisition state and no new store.
The cold dependency-chain layout and shallow-acquisition continuation remain
separate unresolved work.

### Independent acquisition at a true coverage gap

`TestFileHistoryGapAcquiresSharedWindow` originally failed after three seconds
with both bulk acquisition lanes held. The reader now asks the writer for a
64-level blobless window rooted at the missing commit, rather than waiting for
a global shallow deepen. That fetch includes trees for every path. It runs in
the independent history acquisition lane, coalesces concurrent requests for
the same commit, and publishes through the existing object store before readers
use it. It never constructs a per-path index. The full-history worker continues.
The acquisition belongs to the filesystem lifetime (30-second request ceiling);
closing a pager cancels its wait, while closing the mount cancels the fetch
process group. Regression tests cover both lifetimes and reuse by another path
with upstream deliberately unavailable.

The optional local read view now prefers a published full ancestry source.
Before that exists it combines shallow and demand windows. Selecting only one
shallow window was tested and rejected: it pushed the other window's reads back
to GCS and made repeat queries slower. Tests also prove that a preferred source
missing a later update still falls back to the authoritative store.

GCE replay with real GitHub acquisition, real GCS publication, a 32 MiB cache,
and bulk ingestion disabled (`TestSharedHistoryGapLatency`, v48):

- Initial 81 MB window publication: 1.50 s (input already acquired; not a fresh
  clone timing).
- `lib -n 10`: first 4.80 ms, ten 16.23 ms; native Git IDs-only 13.32 ms.
- `Documentation/ -n 10`: first 1.44 ms, ten 1.891 s, exactly one new shared
  window fetch/publication taking 1.877 s.
- Repeated directory query: first 42.58 ms, ten 383.82 ms, zero GitHub fetches;
  native Git IDs-only 8.04 ms. The remaining overhead is not within 2x.
- Full IDs match native Git in every completed query. Benchmark still computes
  gyit's merge-header parent abbreviations; IDs-only native timing omits that
  display work, so medium-format timings are also needed for like-for-like CLI
  comparison.

Both isolated diagnostic GCS prefixes were deleted and a subsequent listing
matched no objects. Existing interactive repository prefixes were preserved.

Final normal-setup check (`TestFreshDirectoryHistoryStartup`, real GitHub/GCS,
empty state/cache/store, background ingestion enabled) completed in 14.17 s:
mount ready at 1.716 s; first ten lib results at 12.432 s; following directory
query first result in 1.96 ms and ten in 1.119 s; repeat directory query 521 ms.
All IDs matched Git. No full acquisition was supplied to gyit by the test.
The separate native baseline source was used only by Git. This confirms the
original lib-then-directory stall is fixed under normal setup, but is not a
claim that already-published cold queries meet 2x.

The deployed v48 backend in the existing interactive nested-KVM guest returned
ten oneline directory results in 3.01 s on the first query after restart and
50 ms on repeat. Store/state were retained for that deployment measurement.
Binary SHA-256: `550edbfd06c6982335baaf4fe5d11b51c9358d003ea2142350e8434cfe2302ab`.
Full suite, focused race tests, Linux vhost build, and WebAssembly build passed.


## Ordinal graph layout experiment

The next experiment is test-only (`history_layout_probe_test.go` and its
explicitly experimental protobuf schema). It is not a replacement production
format yet. It tests the physical cause of cold query latency: serial lookups
for tiny commit frames, their path dictionaries, and unlinked parent SHAs.

The Go preparation scheduler computes changes for **all paths**, independent of
which paths the benchmark will query. A date-priority walk assigns ordinals,
with staging records and SHA-to-ordinal assignments on disk. A second pass emits
4,096-commit graph blocks containing parent ordinal deltas and inline parent
times. Commit IDs are a separate column. The path dictionary contains sorted
ordinal/parent-mask postings. Queries run the same date priority, insertion
order and first-TREESAME-parent rules; an unprepared parent is an explicit
coverage error, never EOF. Native Git only supplies the comparison output.

Initial local-layout measurements on the existing GCE machine:

| Prepared commits | Preparation and encoding | Compressed graph | Compressed IDs | Compressed paths and dictionary |
| --- | --- | --- | --- | --- |
| 8,192 | 2.10 s | 29,364 B | 163,886 B | 640,218 B |
| 65,536 | 16.55 s | 224,359 B | 1,311,088 B | 3,781,948 B |
| 262,144 | 66.23 s | 884,944 B | 5,244,352 B | 17,546,465 B |

The 8,192-commit prefix returns ten correct lib and directory results. The
65,536 prefix still cannot return the first Kconfig result. The 262,144 prefix
returns its correct first result, but only two of ten before a coverage gap.
Those opt-in benchmark cases **fail**, rather than blessing incomplete output.
A global date prefix does not imply complete coverage for a path-specific walk:
TREESAME pruning can reach much older ancestry quickly.

The next revision concatenates graph frames into one range-readable object and
uses a flat sparse directory of path pages. It reads an independent protobuf
catalog on every query and counts that request. The graph read window is either
32 or 256 blocks, fetched in one contiguous range. It never reads the acquisition
repository during queries. The GCS experiment uses four objects in one isolated
diagnostic prefix and deletes them after the test, including on test failure.

Limits of this experiment:

- It compares full commit IDs with Git's `--format=%H`; display records, short-ID
  collision handling, and mount bootstrap are not included.
- Each query starts without a gyit reader cache; the GCS transport connection can
  remain established after publication. Native Git has the existing local
  source and its OS cache. Results are not fresh GitHub acquisition timings.
- The writer currently seals one experimental data set. Incremental immutable
  segments, bounded posting chunks for extremely common paths, byte-budgeted
  prefetch, and atomic production publication still need implementation.
- The flat path footer and whole-object upload buffers are measurement scaffolding,
  not a claim that the prototype meets the production writer's memory bounds.
- Tests cover skewed timestamps and merges, directory-only paths, missing paths,
  explicit gaps, and the transition from a full graph block to a partial block.

### Complete-ancestry GCS result

The pinned revision contains **1,484,088 reachable commits**, confirmed separately
with native Git. The experiment prepared all of them, recording **48,402,927
path/parent-mask postings**, in **393.51 s**, including encoding. Total compressed
layout: graph **5,163,827 B**, IDs **29,690,104 B**, paths/dictionary **137,365,431 B**,
plus a **32,011 B** catalog. Display metadata is not included.

All queries below read the real temporary GCS objects using the native GCS
adapter, with no decoded cache or local acquisition fallback. Catalog reads are
included. Native Git uses the same pinned revision and full-ID output. Every
result list matched exactly, including ordering. Representative observed runs:

| Path and limit | Prototype query | Native Git | Ratio | GCS GETs |
| --- | --- | --- | --- | --- |
| lib, 10 | 210.48 ms | 13.03 ms | 16.2x | 5 |
| Documentation/, 10 | 209.39 ms | 8.05 ms | 26.0x | 5 |
| Kconfig, 1 | 215.19 ms | 116.95 ms | 1.84x | 6 |
| Kconfig, 10 | 334.43 ms | 542.27 ms | 0.62x | 9 |
| COPYING, 1 | 251.31 ms | 462.71 ms | 0.54x | 5 |
| COPYING, 10 | 417.68 ms | 1,146.19 ms | 0.36x | 9 |
| README, 1 | 160.24 ms | 14.16 ms | 11.3x | 5 |
| README, 10 | 444.76 ms | 611.22 ms | 0.73x | 12 |

This table uses the 32-block window for the short lib/directory queries, Kconfig
1 and README 1, and the 256-block window for the others. Both settings were
measured, not chosen adaptively by the implementation. COPYING has only four
matching commits, so `-n 10` correctly returns four and proves EOF. For Kconfig
10 with the 256-block window, the first result arrived at **229.14 ms**, with all
ten at **334.43 ms**. The reader visited 21,670 graph records and transferred
5,751,638 B in total. The previous production empty-cache query returned only two
results by its 10 s cutoff; the prototype demonstrates the physical-layout fix,
not a deployed application result or a complete like-for-like display benchmark.

The remaining failures are explicit: short histories still pay about five
network round trips before their first result, and a flat full-history path
footer transfers roughly megabytes before a recent-only query. These misses
cannot be dismissed just because long sparse queries improved. Production work
must reduce those dependencies, integrate normal formatting and abbreviation,
retain incremental publication, enforce byte-based memory bounds, and measure
mount/bootstrap costs separately. Mount-time metadata prefetch can help a later
command, but its cost must be reported rather than hidden as a warm-cache result.

The wrapper deleted the four diagnostic objects with generation conditions and
verified the prefix was empty; an independent GCS listing matched no objects.
Existing interactive store prefixes were unchanged. Full trace is retained
locally in the ignored `.build/history-layout-probe-full-gcs.log`.
