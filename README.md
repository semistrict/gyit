# 🍑gyit

🍑gyit mounts GitHub as a read-only filesystem. The path *is* the URL:

```text
/Volumes/gyit/github.com/torvalds/linux/kernel/sched/core.c
```

`cd` into a repository you have never opened before and the complete working
tree is sitting there. No `git clone` you typed, no `.git` directory, no
checkout materialized on disk. Open it in an editor, `rg` through it, `wc -l`
it — as far as the kernel is concerned it is ordinary files. It is a prototype,
it is read-only, and public repositories are the supported scope.

## Quick start — macOS on Apple Silicon

Download the signed, notarized `.dmg`. Requires macOS 27 or later:

**<https://github.com/semistrict/gyit/releases/latest>**

1. Drag `gyit.app` to Applications and launch 🍑gyit.
2. Enable `gyitfs`: System Settings → General → Login Items &
   Extensions → File System Extensions. (The app's **File System Extension
   Settings** button opens that pane for you.)
3. Click **Mount 🍑gyit**. `/Volumes/gyit` appears.
4. Type `torvalds/linux` (or `owner/repo@v1.2.3`) and click **Open repository**.
   Finder opens it and setup starts.

The image also carries a standalone signed `gyit` command; copy it onto your
`PATH` and `gyit mount` does step 3 from a shell. Native mounts keep repository
data and cache inside the extension's sandbox container, so `--disk-cache-dir`
is rejected there. Build, signing, notarization:
[macos/README.md](macos/README.md).

## Quick start — Linux

Needs Go 1.26.6+, Git, FUSE 3 (`/dev/fuse` and `fusermount3`, usually the
`fuse3` package). The Linux server supports a pure-Go `CGO_ENABLED=0` build.

```sh
go build -o gyit ./cmd/gyit
mkdir -p "$HOME/gyit"
./gyit mount "$HOME/gyit"
```

From another terminal:

```sh
cd "$HOME/gyit/github.com/torvalds/linux"
cat NOTICE          # live setup progress, until the real tree replaces it
ls                  # then: the actual kernel source
```

`--data-dir` picks durable repository storage (typically
`~/.cache/gyit/repositories`), `--disk-cache-dir` the disposable cache (typically
`~/.cache/gyit/github`), and `--disk-cache-mib` defaults to `4096`. One process
owns each data directory, enforced with `flock`. Stop with Ctrl-C or
`fusermount3 -u`. `GH_TOKEN` or `GITHUB_TOKEN`, if set, is passed to the GitHub
API and Git transport, mainly for rate limits. Running under Lima in a VM:
[LIMA.md](LIMA.md).

## Revisions are part of the path

```text
/Volumes/gyit/github.com/torvalds/linux/              # remote default branch
/Volumes/gyit/github.com/torvalds/linux@v6.12/        # tag
/Volumes/gyit/github.com/owner/repo@feature%2Flogin/  # branch; slashes are %2F
/Volumes/gyit/github.com/owner/repo@0123456789abcdef0123456789abcdef01234567/
```

Each path selects an immutable commit. Different revisions of one repository
can be open at once. `gyit update` refreshes a branch/tag path explicitly;
background history acquisition does not change mounted files.

## Files first, history in the background

The first access resolves the revision through Git, acquires shallow metadata,
and prepares its directory listing. File contents load on demand while complete
history is acquired in the background. Revisions share one durable object pool.

Your read waits up to ten seconds for setup. If it finishes in that window, you
see real files immediately. Otherwise a live `NOTICE` shows progress until the
snapshot replaces it atomically. `touch NOTICE` retries failed setup.

## History commands

Run these inside a mounted repository:

```sh
gyit status
gyit log --oneline -n 10
gyit log --follow -- README.md
gyit update
```

The CLI discovers the mount's protobuf control endpoint by walking up from the
current directory. If requested commits are not available yet, `gyit log`
acquires the needed history ahead of the background job. The mount currently
exposes status, log, and update; other history/view implementations remain
available internally but are not exposed by the mount. Native Git commands are
not supported: no virtual `.git` directory is created.

## What persists, and what gets thrown away

Imported snapshots are durable: keyed by `sha256(owner/repo/commit)`, no size
limit, never evicted, and deleting one costs you a re-import. The decoded cache
— uncompressed chunks, decoded directories, mmapped metadata — is disposable:
shared across all repositories rather than budgeted per mount, capped at 4 GiB
by default (`--disk-cache-mib`), LRU-evicted, and shrunk to keep 20 GiB of the
filesystem free. Clearing it does not remove imported repositories. On-disk
layout and indexes: [OBJECT_STORE.md](OBJECT_STORE.md).

## Limits

Plainly, because most of these will matter to you before the novelty wears off:

- **Read-only, and only that.** No staging, commits, pushes, or branch
  management; writes, creates, renames, removals, and metadata changes return
  `EROFS`. The one exception is `touch` on a synthetic `NOTICE` — "retry setup".
- **Public repositories are the supported scope.** A token is read from the
  environment and passed through, but private access is not a tested path.
- **Only SHA-1 repositories are supported.**
- **Durable storage has no garbage collection.** Removing objects that a reader
  may still use can corrupt that reader. The cache budget does not cap durable
  storage.
- **Revisions are pinned until `gyit update`.** A full commit ID never advances.
- **Git LFS files are pointer files. Submodules are empty directories.**
  Ownership and timestamps are synthetic, names over 255 bytes are rejected, and
  inode numbers are path hashes, so collisions are theoretically possible.
- **macOS needs 27 or later on Apple Silicon**, with the FSKit extension enabled.
- Owner directory listings come from the GitHub API, are paginated, and are
  cached for five minutes. Listing an owner imports nothing; only reading inside
  a repository does.
- It is a prototype. Git transport throttling and ordinary network failures
  apply. The app's durable store is local; a native GCS adapter and experimental
  nested-KVM harness exercise remote storage. Cold remote scans remain slow;
  see [scan measurements](SCAN_PERFORMANCE.md). S3 performance is unvalidated.

## Internals and development

[SEARCH_RESEARCH.md](SEARCH_RESEARCH.md) explains why there is no full-text
index yet, with source-level findings on SeaSearch and Quickwit.
[CONTRIBUTING.md](CONTRIBUTING.md) covers build, tests, and the protobuf/Buf
workflow; the ordinary suite runs on small local fixtures.

gyit is licensed under [LGPL-2.1-or-later](LICENSE). Third-party code retains
its own notices; see [THIRD_PARTY.md](THIRD_PARTY.md).
