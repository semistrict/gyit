# Contributing

Build from the repository root with Go 1.26.6+, Git, a C compiler, and zlib
headers installed:

```sh
go build -o gyit ./cmd/gyit
go test ./... -count=1 -timeout=180s
go vet ./...
```

For a Linux build without native libraries:

```sh
GOOS=linux CGO_ENABLED=0 go build -o .build/gyit-linux ./cmd/gyit
```

Structured formats are protobuf. Edit `proto/` sources, then run `buf lint`
and `buf generate`; never edit generated Go files directly.

The normal suite uses small local fixtures. Large repository imports, mounted
FUSE checks, and remote object-store tests are opt-in; see [benchmarks](BENCHMARKS.md),
[correctness checks](CORRECTNESS.md), and [Linux VM setup](LIMA.md).
Do not run a full Linux import as part of ordinary tests.

Native macOS instructions and bridge tests are in [macos/README.md](macos/README.md).
A local signed build is not a notarized public app distribution.

Regression tests must assert the intended behavior and fail when the bug is
present. Include relevant tests with behavior changes and document commands
that intentionally differ from Git. Keep credentials, profiles, caches, and
repository fixtures out of commits.

Original contributions use LGPL-2.1-or-later. Changes to separately licensed files
must retain their existing notices and applicable licensing.
