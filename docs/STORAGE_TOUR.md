# 🍑gyit storage tour

Build the interactive presentation and the shared Go reader for the browser:

```sh
python3 scripts/build_storage_tour.py
```

Open `.build/storage-tour/standalone.html` in a browser. It embeds the matching
Go runtime, Wasm binary and repository fixture, and works offline. Alternatively,
serve `docs/` with any static server and open `storage-tour.html`.

Chapter 10 is a command adapter backed by the production repository reader:

```text
ls -al
cat README.md
gyit log -n 10 README.md
gyit checkout v1
gyit checkout main
cache clear
```

The trace records real `Store.Get` calls: exact object keys, requested ranges,
returned byte counts, and a stored-byte preview. Repeat a command to see cache
reuse. `cache clear` reopens the selected snapshot and shows its bootstrap reads.
The fixture contains twelve commits, a tag and prepared directory metadata.
The native test compares file-log output with Git's output captured by the fixture
generator, and checks contents, revision switching and cold/warm reads.

The browser replaces remote storage with immutable in-memory object bytes and
the mmap disk cache with the existing bounded memory LRU. Animation pacing adds
an explicit delay per request. This is not a network or filesystem benchmark.
Import/publication, cloud adapters, bbolt and mmap remain native-only; index,
protobuf, compression, pack and history reading are shared code.

## Rebuild inputs

- Edit `docs/storage-tour.html` for the authored presentation.
- Edit `internal/tour` for the command adapter and traced fixture store.
- Edit `cmd/gyit-tour-wasm` for the browser bridge.
- Edit `cmd/gyit-tour-fixture` for fixture source data; then run
  `python3 scripts/build_storage_tour.py --refresh-fixture`.
- Never edit `internal/tour/testdata/repository.zip`, generated Wasm/runtime
  assets, or the standalone HTML directly.

The fixture generator uses native Git to create local synthetic history and
capture expected output, then imports it through the real writer. No Git process
runs in the browser. Fixture object keys may change when regenerated.

```sh
go test ./internal/tour
go test ./...
GOOS=linux GOARCH=arm64 go build ./cmd/gyit
```
