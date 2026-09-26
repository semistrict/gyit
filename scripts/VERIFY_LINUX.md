# Repeatable Linux correctness checks

These checks use the ordinary source tree and `go build ./cmd/gyit`. They never
clone, fetch, repack, or change the existing Git source. An import requires a new
empty object store; every other phase can reuse a retained store.

The fixture is a full-history SHA-1 Linux clone with one existing pack and the
historical tags `v4.4` and `v2.6.24`. Git, Python 3.9+, Go, and GNU `sort` (named
`sort` or `gsort`) are required. The independent inventory includes unreachable
source-local objects. Read verification retains a 32 MiB cache.

From the repository root:

```sh
python3 scripts/verify_linux.py prepare --source .testdata/linux-repo.git
python3 scripts/verify_linux.py build
python3 scripts/verify_linux.py import --store .testdata/linux-store
python3 scripts/verify_linux.py catalog --store .testdata/linux-store
python3 scripts/verify_linux.py reader --store .testdata/linux-store
python3 scripts/verify_linux.py commands --store .testdata/linux-store
```

`prepare` reuses its existing fixture after checking source identity and the
independent inventory hash. To adopt an already saved independent Git inventory,
pass `--facts /absolute/path/objects.tsv --expected-facts-sha256 HASH`. `--source`,
`--store`, `--output`, and `--fixture` accept paths outside this repository.

`import` times the actual `gyit import` executable, then launches an independent
persisted-store verifier. To repeat only that verifier, run the `verify` phase.
The `catalog` phase compares every non-tag object identity, type, and size and
walks every main-index page. `reader` checks sampled exact bytes, directories,
bounded cache state, and failures in a child with Git access trapped. `commands`
performs 98 exact stdout/exit-code comparisons with Git at three revisions.
These checks do not exhaustively reconstruct every historical object body.

Each phase preserves reports, output, and failed stores under
`.build/linux-correctness/`. Any operation taking over one second is flagged.
Processes have explicit watchdogs: CLI import 180 seconds, subsequent core
verification 200 seconds, catalog/reader 200 seconds, and commands 260 seconds
(60 seconds per command). A timeout kills the entire owned process group.
Source edits invalidate the binary manifest; rerun `build` before further checks.

For real FUSE checks, prepare a small independent oracle on the source host:

```sh
python3 scripts/verify_linux_fuse_prepare.py \
  --source .testdata/linux-repo.git --out .build/linux-fuse-oracle
```

Build `gyit` normally on Linux, or cross-compile it using the project's supported
toolchain, then run this inside Linux with `/dev/fuse` and `fusermount3` available:

```sh
python3 scripts/verify_linux_fuse.py \
  --binary /absolute/path/gyit --store /absolute/path/linux-store \
  --oracle /absolute/path/linux-fuse-oracle --report /tmp/linux-fuse-report.json
```

The FUSE verifier creates two private mounts, compares files and directories,
switches versions, checks stable path inodes and pinned open handles, and verifies
cleanup of its mounts, processes, sockets, and temporary directory. It has a
180-second watchdog plus 30 seconds for cleanup. It leaves existing mounts alone.
The source repository is not an input to this mounted phase.
