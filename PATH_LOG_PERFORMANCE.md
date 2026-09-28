# Ingested file history

Literal single-path `gyit log` uses a durable history graph built during history
ingestion, for every path in every new reachable commit. It does not build or
extend an index in response to a query. The previous requested-prefix cache,
callbacks, schema and tests have been removed.

Each indexed revision has a history directory tree. Entries contain the current
object identity/mode and links to the last visible change for ordinary and
first-parent history. Change events link to shared commit metadata and immediate
parent cursors. Deleted paths retain history entries. Timestamp bounds allow
skipping unchanged ancestry while preserving Git's ordering at merges; ambiguous
cases step through the ingested metadata, without historical pack reads.

Updates stop at already-indexed ancestry. Unchanged subtrees and identical pages
are reused. All new records become visible through the existing atomic HEAD CAS.
Cancellation or a failed CAS leaves the previously published index usable.

The writer reads acquisition packs through the Go decoder. Native Git is used
for acquisition and as the test oracle, never as the query engine. A temporary
32 MiB decoder cache and checkpointed on-disk work queues support ingestion;
directory comparisons and staging batches require additional working memory.

Literal queries return a history-not-ready error until their revision is indexed.
Initial background ingestion builds the index after commit/tree acquisition.
Small updates extend it before switching when the necessary ancestry is present.
Other pathspecs and `--follow` retain the general walker. Blame is unchanged.

## Physical storage

All keys below are relative to one repository's object-store prefix:

```text
HEAD                                      mutable protobuf publication root
generations/<manifest-sha256>             immutable copy of a publication root
index/<random-id>/<8-digit-hex-number>     uncompressed index pages, up to 8 MiB
index/progressive-<id>/<number>            zstd filesystem pages, up to 256 KiB
index/progressive-history-<id>/<number>    zstd history pages, up to 256 KiB
packs/progressive/<git-pack-id>/<number>   original Git pack segments, up to 8 MiB
```

A PageReference contains the object key, byte offset, length and SHA-256 of the
stored page bytes. Global index pages are protobuf, with up to 128 items or
children. Compressed metadata containers concatenate independent zstd frames;
each frame encodes one protobuf record/page. History directories target 8 KiB
before compression; history records have a 64 KiB encoded limit.

The global index maps `history/root/<commit>` to an inline FileHistoryRoot.
That root points into history containers containing paged directories, path
change events and shared commit records. These are internal index keys, not
individual object-store objects. `g/<oid>` maps Git object IDs to pack offsets;
`tree/<oid>` maps prepared filesystem trees to directory pages.

History readers fetch immutable containers and retain one uncompressed protobuf
container per cache entry. This uses the existing bounded local disk cache;
evicting it cannot remove durable history. Global index reads currently fetch
entire index containers. Old immutable publications and failed unpublished
uploads are not reclaimed by this change.

## Measurements, 2026-09-27

The medium local fixture has 10,934 commits reachable from its selected tip.
Complete all-path history ingestion took **37.27 seconds** and wrote
**342,585,197 bytes** of additional durable data. This excludes Git acquisition
and the existing pack/object index. A synthetic one-file update across a
100-directory repository wrote about **12.7 KiB** in four writes; unchanged
directory references were preserved. This is not a Linux-scale ingestion result.

Fresh-reader local tests used a new empty decoded cache for every path and count.
All formatted output matched Git. Ten-entry queries across README.md, AGENTS.md
and package.json took **52–61 ms**, versus **38–88 ms** for Git. Some 100-entry
cases remained above 2x (up to 2.8x in the last run).

On the existing GCE host, with native GCS and revision
`1cc7e2361237ce7244430ee1d581c77f95c57ac8`, complete history ingestion took
**103.11 seconds**. A previously unqueried README.md, `-n 10`, returned identical
formatted output on all five runs:

| Read | gyit | Native Git | Ratio |
|---|---:|---:|---:|
| First reader | 623 ms | 169 ms | 3.69x |
| Subsequent readers | 20–27 ms | 164 ms | 0.12–0.16x |

The first reader reused the importer cache, but no prior file-log query had run.
These are host reader timings, excluding guest control transport and paging.
The user accepted the cold-read result; it does not meet the earlier 2x target.
Benchmark writes used an isolated temporary GCS overlay, leaving the live
repository and mount unchanged.

## Reproduce

Build `./cmd/gyit-bench-log`. Against an already indexed store:

```sh
gyit-bench-log -store gs://BUCKET/REPOSITORY -sha SHA -path README.md \
 -cache /tmp/gyit-log-fresh -git-dir /path/to/acquisition.git -n 10 -runs 5
```

Use a fresh cache directory for cold reads. Add
`-ingest-from /path/to/acquisition.git` to measure full index ingestion separately.
It writes durable data and must be the repository's sole writer. To isolate a
benchmark from a live store, use `-base-store` for its read-only prefix and
`-store` for a new empty scratch prefix. Remove the scratch prefix afterward.
Default deadlines are two minutes for ingestion and 20 seconds per query.

The opt-in medium test reuses an existing packed bare fixture; it does not clone:

```sh
GYIT_HISTORY_SOURCE=/path/to/fixture.git \
 go test ./internal/repo -run '^TestFileHistoryMedium$' -count=1 -v
go test ./internal/repo -run '^$' -bench '^BenchmarkFileHistoryFirstQuery$'
```

Normal-suite regression tests cover untouched first queries without historical
pack reads or writes, divergent merges and skewed timestamps, first-parent
history, disconnected branches, deletion/type changes, unchanged-subtree reuse,
failed publication, stale-writer CAS and cancellation.
