# Third-party code

The root LGPL-2.1-or-later license applies to project code. The following
third-party sources retain their existing licenses and notices. Keep their notices with redistributed
source. Binary packaging must also account for these components and linked
module dependencies; a binary distribution is not prepared by this change.

| Component | Location | License and notices |
| --- | --- | --- |
| LibXDiff adaptations | `internal/repo/patchmyers.go`, `internal/repo/patchindent.go` | LGPL-2.1-or-later; [notice](third_party/xdiff/NOTICE), [license](third_party/xdiff/LGPL-2.1) |
| Compression library, including local bounded-encoder changes | `third_party/compress` | [BSD license](third_party/compress/LICENSE); source files retain additional applicable notices |
| xxhash | `third_party/compress/zstd/internal/xxhash` | [license](third_party/compress/zstd/internal/xxhash/LICENSE.txt) |
| LZ4 reference implementation | `third_party/compress/internal/lz4ref` | [license](third_party/compress/internal/lz4ref/LICENSE) |
| Snappy reference implementation | `third_party/compress/internal/snapref` | [license](third_party/compress/internal/snapref/LICENSE) |

External Go dependencies are pinned in `go.mod` and `go.sum`; their upstream
licenses remain applicable. Run `go list -m all` for the dependency inventory.
