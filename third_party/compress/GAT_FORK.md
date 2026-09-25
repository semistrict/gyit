# Bounded encoding extension

This directory contains the runtime sources needed by the Zstandard dependency
from `github.com/klauspost/compress v1.20.0`, including its license. Upstream source
files are copied verbatim. Unused compression packages and upstream test fixtures
are omitted. The module replacement is relative and works offline after the other
dependencies are downloaded.

Only `zstd/gat_cutoff.go`, `zstd/gat_fast_cutoff.go`, and
`zstd/gat_cutoff_test.go` are local additions. The two implementation files derive
from upstream `encoder.go` and `enc_fast.go`. Normal encoding and decoding paths
are unchanged. `EncodeAllBelow` can discard a frame once its minimum encoded size
cannot beat an already-compressed delta. A stopped result must never be stored.

The lower bound counts entropy in literals already committed by the matcher.
For any binary prefix code, the cost of those literals is at least their entropy.
Integer logarithms round down. Raw literals cannot be smaller than this bound;
a single repeated literal has zero entropy. Completed block bytes provide an
additional exact bound. Encoder resets discard partial block state normally.

Run local extension tests with:

```
cd third_party/compress
go test -race ./zstd -run '^TestBoundedEncoder' -count=1
```

When updating the dependency, recopy its runtime sources and rebase the two
derived methods against upstream. Run these tests, the application's byte-exact
delta-selection tests, and complete import/read benchmarks before adoption.
