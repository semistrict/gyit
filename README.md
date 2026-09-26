# gyit

GitHub repositories as a read-only filesystem, prepared automatically for local
browsing and IDE use. The only supported mount mode is the GitHub namespace.

```text
/Volumes/gyit/github.com/torvalds/linux/
/Volumes/gyit/github.com/torvalds/linux@v6.12/
/Volumes/gyit/github.com/owner/repo@feature%2Flogin/src/main.go
/Volumes/gyit/github.com/owner/repo@0123456789abcdef0123456789abcdef01234567/
```

Opening a repository waits up to three seconds for setup. If it completes within
that time, the real files appear directly. Otherwise the directory contains only
`NOTICE`, which reports progress or the failure while setup continues. Once setup
succeeds, the complete repository replaces that placeholder in one publication.
If the repository itself has a `NOTICE`, its real file appears at that point.

No revision suffix selects the remote's default branch. `@branch`, `@tag`, or a
full commit SHA selects a revision. Encode slashes inside revision names as
`%2F`. Branches and tags resolve once per mount; the selected commit stays pinned.
Repository path inode numbers remain stable when setup completes.

## How setup works

Setup uses Git's transport to fetch complete history, all public branches and
tags, then imports them into gyit's durable immutable repository storage. The
selected revision remains pinned. Setup finishes only after full history is
imported; there is no separate history store or later history fetch triggered by
a command. Directory and file reads load data on demand from this store, so IDE
indexing does not consume an API request per file. At most two setups run
concurrently. Existing shallow stores are not reused; repositories are prepared
again under `repositories-v1` on their next setup.

The volume root contains `github.com`. Owner directories list public GitHub
repositories. Listings are
paginated and cached for five minutes; listing does not import repositories. The
GitHub root shows visited owners. Enter an owner/repository path directly. A
nonexistent or inaccessible repository retains `NOTICE` with its setup error.
Touch the synthetic `NOTICE` to retry a failed setup. Remount to refresh branch revisions.

Prepared snapshots persist on disk and are reused by commit. This storage is
separate from the shared uncompressed mmap cache, whose default maximum is
4 GiB total across all repositories and revisions, shrinking to preserve 20 GiB
of free disk space. Only the disposable cache has this size limit and eviction.
The repository store is durable data (the future S3 backend), with no configured
size limit or automatic eviction. Deleting the cache does not delete imported
repositories. Staging clones are removed after setup or cancellation; mounted
files remain read-only.

## macOS

Use the native [app and FSKit build instructions](macos/README.md). The app
mounts `/Volumes/gyit`, with repositories under `github.com`; enter an owner/repository path in the app to open
it in Finder. The app currently prepares public repositories. It does not
require Terminal Full Disk Access or permission to read a local source repo.

## Linux

Requires Go 1.26.6+, Git, FUSE 3, a C compiler, and zlib development headers.
A pure-Go build is also available using `CGO_ENABLED=0`.

```sh
go build -o gyit ./cmd/gyit
mkdir -p "$HOME/gyit"
./gyit mount "$HOME/gyit"
```

From another terminal:

```sh
cd "$HOME/gyit/github.com/torvalds/linux"
cat NOTICE
# When setup completes, ls shows the real repository files.
```

Use `--data-dir` to choose prepared-snapshot storage and `--disk-cache-dir` to
choose the cache. `--disk-cache-mib` defaults to 4096. Public repositories are
the supported scope for now.
One mount owns each data/cache directory. Stop with Ctrl-C or `fusermount3 -u`.
See [Lima testing](LIMA.md).

## Current scope

This is a prototype. Native macOS mounting and background publication need
platform-specific integration checks. Git LFS files remain pointer files and
submodules appear as empty directories. Git transport throttling and ordinary
network failures still apply.

Run `gyit log` inside a repository or subdirectory. The CLI discovers the
mount through a protobuf root attribute and reads the mounted, pinned repository
through its private local command socket. History stays in the same durable
gyit object store; commands do not fetch a second copy. GitHub paths remain pinned;
open another `@revision` path instead of using checkout. [Object-store tools](OBJECT_STORE_COMMANDS.md),
[storage layout](OBJECT_STORE.md), and [historical benchmarks](BENCHMARKS.md)
describe that engine, not measured GitHub setup performance.

## Development and license

See [CONTRIBUTING.md](CONTRIBUTING.md) for checks. Structured persisted metadata
uses protobuf, generated with Buf. The ordinary test suite uses small local
fixtures; no large repository download is required.

Apple Silicon signing and notarization are described in [the macOS guide](macos/README.md).

gyit is licensed under [LGPL-2.1-or-later](LICENSE). Third-party code retains
its existing notices and licenses; see [THIRD_PARTY.md](THIRD_PARTY.md).
