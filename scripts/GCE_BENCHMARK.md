# Nested KVM with native GCS

This development harness measures a complete directory walk, with or without
`stat` on every entry. Git is not used in the guest. It compares a GCS-backed gyit
repository with a matching ordinary checkout served by Linux `virtiofsd`, in the
same QEMU/KVM guest. It does not route through a host FUSE mount or FSKit.

Use a disposable GCE Intel N2 host with nested virtualization enabled, at least
16 GiB RAM and enough disk to retain the cache's 20 GiB free-space reserve.
The measured configuration used `n2-standard-8`, Cascade Lake, Ubuntu 24.04 and a
100 GiB SSD boot disk. Both the host and bucket were in `us-east4`. The guest
gets four vCPUs and 8 GiB RAM. Verify `/dev/kvm` exists; QEMU uses `-enable-kvm`
and cannot silently fall back to software emulation.

The VM's attached service account needs read access only to the private bucket.
GCS authentication uses Application Default Credentials. No service-account key
file, interoperability key, or S3 endpoint is needed. Keep public-access
prevention and uniform bucket-level access enabled.

On the disposable Ubuntu host:

```sh
sudo apt-get update
sudo apt-get install -y qemu-system-x86 virtiofsd linux-image-generic \
  busybox-static cpio python3 initramfs-tools kmod
sudo modprobe kvm_intel
```

Build on the development machine:

```sh
python3 scripts/build_vhost.py --arch amd64
```

Copy `.build/vhost/gyit-vhost`, `.build/vhost/fuse.test`,
`scripts/benchmark_nested_kvm.py`, and `scripts/benchmark_scan.py` to the same
directory on the host. Run `./fuse.test -test.run TestScatterReadAcrossGuestPages`
there before benchmarking. This catches fragmented guest read buffers that
upstream Go-FUSE's experimental vhost-user entry point currently rejects.
The build is Linux amd64 with `CGO_ENABLED=0`; macOS remains arm64-only.

Upload a prepared repository's durable object store to a dedicated GCS prefix.
It includes HEAD, generations, indexes, and packs. Do not upload acquisition Git
directories or the decoded cache. `gyit import` produces this same format.

```sh
gcloud storage rsync "$PREPARED_STORE" "gs://$BUCKET/repository" \
  --recursive --exclude='.*\.lock$'
```

Copy the matching normal checkout separately, excluding its root `.git`. On
macOS, disable archive metadata so AppleDouble files do not change the baseline:

```sh
COPYFILE_DISABLE=1 tar --no-mac-metadata --exclude='./.git' \
  -C "$CHECKOUT" -cf checkout.tar .
```

The gyit server gets no local copy of the durable store. Its data lives in GCS;
its only repository cache is the bounded 4 GiB decoded disk cache. The ordinary
checkout is solely the comparison filesystem.

On the Linux host, set paths to an installed **generic** kernel and matching
initrd, then run each mode in a fresh guest:

```sh
sudo python3 benchmark_nested_kvm.py \
  --store gs://BUCKET/repository --sha FULL_COMMIT_ID --server ./gyit-vhost \
  --checkout /path/to/checkout --cache /var/tmp/gyit-cache \
  --kernel /boot/vmlinuz-VERSION-generic \
  --initrd /boot/initrd.img-VERSION-generic \
  --output /var/tmp/gyit-stat-empty-1 --mode stat --empty-cache --timeout 180
```

Repeat with `--mode names` and a different output directory. `--empty-cache`
creates and removes an isolated decoded cache for that run. Omit it to reuse
`--cache`; run once to populate that directory before measuring an existing
cache. `cache_initially_empty` records whether the directory was empty before
server startup. Host OS caches are retained. The baseline uses
`virtiofsd --cache=always`; gyit uses a 300-second metadata TTL. Both guest mounts
are read-only.

Every run compares five alternating-order pairs, checks identical path/type/size
digests, and checks file contents after timing. The output JSON records first
scan and warm-median ratios, startup time, GCS Get calls, returned bytes and
aggregate request time. Request counters include startup and the final content
check; they count adapter calls rather than SDK-internal HTTP retries. Import,
upload and guest boot are outside scan timing. Exit status 1 with a complete
result means the 2× threshold was missed; an exception means no valid result.

The runner terminates its guest and both daemons even on failure. The disposable
guest has no writable disk; it is terminated after its success marker because
the experimental vhost-user backend can stall QEMU during device teardown.

After copying out results, delete the test VM (including its boot disk), bucket
and dedicated service account. Verify each is gone; a VM expiration timer is
only a fallback, not cleanup. Disable bucket soft delete when creating a truly
disposable test bucket so cleanup does not retain the uploaded data.

See [measured results](../SCAN_PERFORMANCE.md) for limits and comparisons.

## Progressive format

The harness also supports the progressive object store. Build
`./cmd/gyit-bench-prepare` for Linux along with the vhost server. Given an existing
packed snapshot fixture on the host, publish directly through the native adapter:

```sh
./gyit-bench-prepare --source /path/to/snapshot.git \
  --store gs://BUCKET/snapshot --sha FULL_COMMIT_ID \
  --state /var/tmp/gyit-import-state --cache /var/tmp/gyit-import-cache \
  --timeout 180s
```

This writes durable packs, protobuf metadata and CAS HEAD directly to GCS.
Its JSON timing output distinguishes pack and metadata publication. The source
fixture must contain every blob needed for that snapshot; this tool does not
fetch missing Git objects. Keep the import cache separate from the reader cache.

Pass `--sha FULL_COMMIT_ID` to `benchmark_nested_kvm.py` to serve this
format. The reader gets only the GCS prefix, selected commit and its own decoded
cache; it needs no acquisition Git repository. The benchmark compares file
contents and traversal digests; no virtual `.git` directory is exposed.
Run readers with bucket objectViewer after publication. The first run using a
new persistent cache is still cold: inspect `cache_initially_empty` in results.

For the separate writer/control integration test, cross-compile
`go test -c ./internal/githubfs` to Linux and run on the VM:

```sh
GYIT_TEST_GCS_PREFIX=gs://BUCKET/isolated-update-test ./githubfs.test \
  -test.run '^TestProgressiveGCSUpdateAndLog$' -test.v -test.timeout=120s
```

That test needs bucket objectAdmin while running. It creates its Git fixture
locally and tests moved branches/tags, pinned SHA mounts, failed updates, and log
parity with Git against the actual GCS backend. It exercises host-side commands;
it does not claim guest-to-host control-socket forwarding. Its prefix is durable
test data and must be removed with the disposable bucket during cleanup.

### Interactive GitHub namespace

The vhost server defaults to public GitHub automount. Point `--store` at a
repository namespace root, with separate local state and decoded cache paths:

```sh
./gyit-vhost --store gs://BUCKET/github --socket /run/gyit.sock \
  --state /var/lib/gyit --cache /var/cache/gyit
```

Attach that socket to the guest's `gyit` virtio-fs device and mount it at
`/mnt/gyit`. Browsing `/mnt/gyit/github.com/torvalds/linux` starts the ordinary
progressive import; long setup displays the updating `NOTICE`. Repository
objects are durable GCS data; only the separate decoded cache is size-limited.

Fixed-fixture benchmark servers require `--prepared`. The nested-KVM benchmark
runner supplies this flag automatically. Do not use a prepared fixture server
for interactive GitHub browsing.

The guest also needs the Linux `gyit` binary installed in `/usr/bin` (on PATH).
Repository command discovery uses a mount-relative `.gyit.control` endpoint:
protobuf requests travel over virtio-fs and are forwarded to the host command
service. Host Unix socket paths are not usable directly inside the guest.
Mount gyit's device without the kernel `ro` option to permit this control-file
exchange; repository mutation operations still return `EROFS`. The control file
is not included in directory listings. The ordinary-checkout baseline can remain
kernel-read-only. An interactive initrd must keep a PID 1 supervisor running
when the user exits the shell, and should include `less` for the command pager.

## Responsiveness regressions

`verify_nested_kvm_regressions.py` checks, from inside a fresh guest, that
background work and paused pagers do not starve foreground requests:

- `deepen-reads`: file reads complete while a history `--deepen` batch runs.
  A host thread watches the server's Git children and publishes the deepen
  state to the guest through a separate `--cache=never` virtio-fs share.
- `status-with-paused-logs`: `gyit status` succeeds beside 20 log streams
  whose output nobody reads (the pipes fill, like paused pagers).
- `file-log-with-paused-file-logs`: a file log completes beside two paused
  file-history streams, compared with the same query alone.

Build the Linux server as above and the CLI with
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build ./cmd/gyit`, copy both scripts
next to each other, and run as root with a fresh store prefix per run:

```sh
sudo python3 verify_nested_kvm_regressions.py \
  --server ./gyit-vhost --cli ./gyit \
  --store gs://BUCKET/PREFIX/github \
  --kernel /boot/vmlinuz-VERSION-generic \
  --initrd /boot/initrd.img-VERSION-generic --output /var/tmp/gyit-verify-1
```

The server imports `torvalds/linux` from GitHub (override with
`--repository`); its history stays incomplete for the whole run, which the
checks rely on. Results are in `result.json`. The import is durable data in
the prefix; remove it during cleanup.
