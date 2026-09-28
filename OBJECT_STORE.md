# Repository storage

gyit has one repository format: a progressively populated, immutable Git object
pool. The macOS app, Linux FUSE mount, virtio-fs servers, and `gyit import` all
use it. There is no alternate importer or compatibility reader.

The `Store` interface supports local directories, S3-compatible storage, and
native GCS (`gs://bucket/prefix`). GCS uses Application Default Credentials and
object-generation preconditions; S3 uses conditional writes. Local storage uses
an atomic replacement protected by a lock. Failed compare-and-swap publication
never replaces another writer's HEAD.

## Durable layout

Within one repository's store prefix:

```text
HEAD                         protobuf manifest: current immutable index root
generations/<hash>           immutable copies of published manifests
packs/progressive/<pack-id>/<segment>    original Git pack bytes, split into 8 MiB objects
index/<build-id>/...         immutable object-index pages in containers
index/progressive-...       compressed directory pages in 256 KiB containers
```

Git pack bytes retain Git's compression and deltas. The importer reads pack
indexes and headers to locate objects and determine lengths; it does not expand
all historical file contents. Readers reconstruct requested objects, verify
their identities, and retain decoded bytes in the bounded cache.

The index maps these keys to protobuf records:

| Key | Value |
| --- | --- |
| `g/<object-id>` | Git pack identity, offset, and object size |
| `pack/<pack-id>` | Already imported pack marker |
| `tree/<tree-id>` | Prepared directory metadata root |
| `state/<commit-id>` | Snapshot/history availability and progress |
| `refs-root` | Reference-index root for standalone local imports |

Directory entries contain names, modes, exact sizes, object identities, and
child-directory references. Listing a prepared directory reads its metadata
without decoding file bodies. Unprepared historical trees can be read directly
from their Git objects without writing derived data back to the store.

Structured records are defined in `proto/gyit/storage/v1/` and generated with
Buf. The source schemas are authoritative.

## Acquisition and publication

GitHub setup fetches shallow metadata first and prepares the selected snapshot.
If setup takes longer than ten seconds, the mount displays a live `NOTICE` until
the snapshot can replace it atomically. File bodies are requested on demand;
background work acquires remaining snapshot contents and history. A bounded
foreground history request uses a separate acquisition lane so it does not wait
behind a full background fetch.

Updates upload new immutable packs and rebuild affected metadata, reusing
unchanged subtrees. All immutable writes must complete before HEAD is replaced
using compare-and-swap. Existing snapshots retain their selected commit.
`gyit update` explicitly advances a branch/tag mount after its new snapshot is
ready. Background history publication alone does not change mounted files.

A local `gyit import --repo PATH --store LOCATION` makes a temporary packed
mirror of the source without modifying it, publishes the same pack format, and
prepares HEAD (or the snapshot selected with `--rev`). It also publishes local
references. This utility is a full local import; GitHub mounts use progressive
network acquisition. Only SHA-1 repositories are supported currently.

## Cache versus repository data

Durable packs, indexes, manifests, and acquisition state are **not cache** and
are not evicted to meet the cache budget. The macOS app happens to keep both
durable storage and cache on local disk; remote mounts keep durable objects in
the configured object store.

The disposable cache stores uncompressed decoded objects and metadata. A shared
4 GiB disk budget covers all repositories and preserves 20 GiB of free space.
Files are mmap-backed; the OS manages resident pages. There is no separate
compressed-object cache or retained RAM payload tier in mounted operation.
Clearing the cache does not delete repository data. Durable-store garbage
collection has not been implemented.
