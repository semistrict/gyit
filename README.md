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
`fuse3` package), a C compiler, and zlib headers. `CGO_ENABLED=0` gives a
pure-Go build that uses Go's zlib instead of the system one.

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

Every path is pinned to one immutable commit for the life of the mount. Two
revisions of the same repository are two directories you can have open at once.

## The tradeoff: first access buys complete history

gyit is not a lazy clone, and that is the deliberate part. The first time you
touch `owner/repo@revision`, `git ls-remote` resolves it to a commit. If a
snapshot for that `(owner, repo, commit)` already exists, gyit reuses it without
another fetch or import.
Otherwise a background job fetches **complete history for every branch and tag**
over Git's ordinary transport, refuses the result if it came back shallow, and
imports it into gyit's own immutable on-disk format.

Your read blocks for up to ten seconds. If setup finishes inside that window
you just see the real files. If it doesn't, the directory contains exactly one
file, `NOTICE` — a live progress report, so a slow setup is legible rather than
a hang — and the whole tree appears later in a single atomic swap. A partial
tree is never visible. If setup fails, `NOTICE` says why and stays put; `touch`
it to retry. Two setups run at a time and the rest queue.

So the full fetch and import happen once per prepared commit; resolving a new
path may still contact the remote. The filesystem is lazy: afterwards nothing
is checked out, reading a file is a lookup into an immutable index plus a
byte-range read from local disk, and an IDE indexing pass makes no GitHub
requests at all. The bill arrives up front: for a repository the size of Linux,
setup can mean minutes of fetching and tens of GB on disk, with no partial,
blobless, or shallow mode on offer. Import timings and method — single runs on
one machine, which do not establish general performance — are in
[BENCHMARKS.md](BENCHMARKS.md) and [CORRECTNESS.md](CORRECTNESS.md), along with
what is explicitly *not* measured.

## Git history, without a checkout

The mount synthesizes a `user.gyit.control` xattr at each repository root; the
CLI walks up to it and speaks protobuf over the private endpoint it names, so
there is no daemon to configure. History commands work from anywhere in the tree,
against the pinned commit, with no second copy and no Git binary on the reader:

```sh
cd /Volumes/gyit/github.com/torvalds/linux/kernel/sched
gyit log --oneline -n 20 -- core.c
gyit log --follow -- core.c
gyit blame -L 100,140 core.c
gyit diff v6.11 v6.12 --name-status
gyit grep -n 'sched_class' -- core.c
gyit show HEAD:core.c
```

Also present: `status`, `annotate`, `ls-tree`, `ls-files`, `cat-file`, `branch`,
`tag`, `show-ref`, `rev-parse`, `rev-list`, `merge-base`, `shortlog`. Run `gyit`
with no arguments for the list. Output follows Git's formats closely enough that
a byte-for-byte parity suite compares them, and every traversal is explicitly
bounded, so a limit produces an error rather than an approximate answer.
`gyit switch` is refused on GitHub mounts — a path is pinned, so open a different
`@revision` instead. Semantics, flags, and known divergences from Git:
[OBJECT_STORE_COMMANDS.md](OBJECT_STORE_COMMANDS.md).

## What persists, and what gets thrown away

Imported snapshots are durable: keyed by `sha256(owner/repo/commit)`, no size
limit, never evicted, and deleting one costs you a re-import. The decoded cache
— uncompressed chunks, decoded directories, mmapped metadata — is disposable:
shared across all repositories rather than budgeted per mount, capped at 4 GiB
by default (`--disk-cache-mib`), LRU-evicted, and shrunk to keep 20 GiB of the
filesystem free. Clearing it does not remove imported repositories. On-disk
layout, indexes, and archive read recipes: [OBJECT_STORE.md](OBJECT_STORE.md).

## Limits

Plainly, because most of these will matter to you before the novelty wears off:

- **Read-only, and only that.** No staging, commits, pushes, or branch
  management; writes, creates, renames, removals, and metadata changes return
  `EROFS`. The one exception is `touch` on a synthetic `NOTICE` — "retry setup".
- **Public repositories are the supported scope.** A token is read from the
  environment and passed through, but private access is not a tested path.
- **First access is a full fetch** of all branches and tags, as above.
- **The store can exceed the source pack.** Archive imports may preserve the
  source pack *and* write converted fallbacks for objects that exceed native
  read limits. There is also no garbage collection: nothing in the durable store
  is ever reclaimed, and deleting old generations while a reader may use them
  corrupts it.
- **Revisions are resolved once, at setup.** A branch path is pinned to the tip
  it had then; remount to pick up movement.
- **Linux mounts use direct I/O with zero kernel attribute caching.** That keeps
  content correct at stable inode numbers, but rules out file-backed `mmap`.
  Tools that require it cannot run directly from the mount.
- **Git LFS files are pointer files. Submodules are empty directories.**
  Ownership and timestamps are synthetic, names over 255 bytes are rejected, and
  inode numbers are path hashes, so collisions are theoretically possible.
- **macOS needs 27 or later on Apple Silicon**, with the FSKit extension enabled.
- Owner directory listings come from the GitHub API, are paginated, and are
  cached for five minutes. Listing an owner imports nothing; only reading inside
  a repository does.
- It is a prototype. Git transport throttling and ordinary network failures
  apply, and the S3-backed future of the durable store is unvalidated here.

## Internals and development

[SEARCH_RESEARCH.md](SEARCH_RESEARCH.md) explains why there is no full-text
index yet, with source-level findings on SeaSearch and Quickwit.
[CONTRIBUTING.md](CONTRIBUTING.md) covers build, tests, and the protobuf/Buf
workflow; the ordinary suite runs on small local fixtures.

gyit is licensed under [LGPL-2.1-or-later](LICENSE). Third-party code retains
its own notices; see [THIRD_PARTY.md](THIRD_PARTY.md).
