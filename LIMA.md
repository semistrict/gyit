# Try the running Linux mount

The `default` Lima VM has a user service named `gat-demo` running the read-only
medium fixture at `~/gat-demo/repo`, with a 32 MiB userspace cache. Its object
store is the ignored host directory `.testdata/lima-store`, shared into Lima.
The full import contains 477,069 objects and all 40,985 reachable commits.
No checkout is materialized in the VM. The binaries and environment file live
under `~/gat-demo` inside Lima.

From your Mac:

```sh
limactl shell default
```

Then inside Lima:

```sh
. ~/gat-demo/env
cd ~/gat-demo/repo

gat status
gat log --oneline -n 10
gat log --follow -- README.md
gat log -- ':(glob)docs/**/*.md'
gat diff HEAD~1 --name-status
gat blame -L 1,20 README.md
gat annotate -L 1,20 README.md
ls ~/gat-demo/repo
cat ~/gat-demo/repo/README.md

# Switch this running mount to a historical version, 100 first-parent steps back.
gat switch HEAD~100
ls ~/gat-demo/repo

# Return to the original version.
gat switch -
```

`gat status` shows Git-style clean status, for example:

```text
On branch main
nothing to commit, working tree clean
```

After switching to an ancestor or SHA, it reports detached HEAD instead.
`gat status -sb` shows a compact branch line, while `gat status --porcelain`
prints nothing for this read-only, clean mount.

`gat log` shows 20 entries by default. Use `gat log --oneline -n 50` or
`gat log --first-parent main` to choose the amount and traversal.

You can also use `gat switch main`, a tag, or a short commit ID. `HEAD~100`
is relative to the currently mounted version; `-` switches back. Branch and tag
names come from the latest import, and do not follow the source automatically.

Commands also work from any subdirectory of the mount. `--socket "$GAT_SOCKET"`
is an optional override for commands issued from elsewhere.

The environment defines these imported commits:

| Variable | Full commit ID |
| --- | --- |
| `GAT_SHA_HEAD` | `595cc91e8cbb1c2ca822d0311dcf12709410c582` |
| `GAT_SHA_OLD` | `ea218f5cd8a536b6e89dbd7f2338c93e87bc5bbf` |

For an observable content change, compare `.github/workflows/rust-release.yml`
between these versions. `stat` reports the same inode across switches. Existing
open file handles continue reading the version they opened; reopen a file to see
the new contents. The source refs are not updated automatically.

The live smoke check switched versions, compared file bytes with Git object IDs,
verified an old open handle and stable inode, rejected a missing commit, and
restored the initial version. On this VM the switch took 4.71 ms including client
startup and the warm root listing took 4.33 ms. These are local measurements with
a shared filesystem store, not S3 latency measurements.

## Manage the demo

Inside Lima:

```sh
cd ~
systemctl --user status gat-demo --no-pager
journalctl --user -u gat-demo --no-pager -n 30
systemctl --user stop gat-demo
```

Stopping the service unmounts the filesystem and removes its socket. The fixture
and object store remain available for another run. The service is transient; it
is not configured to start automatically when the VM reboots. To start it again:

```sh
. ~/gat-demo/env
systemd-run --user --unit gat-demo --property=Type=exec \
  "$HOME/gat-demo/bin/gat" mount \
  --store /Users/ramon/src/gat/.testdata/lima-store \
  --sha "$GAT_SHA_HEAD" --cache-mib 32 \
  --socket "$GAT_SOCKET" "$HOME/gat-demo/repo"
```

## Rebuild and rerun checks

From `/Users/ramon/src/gat` on the Mac:

```sh
go test -race ./...
buf lint
go vet ./...
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c \
  -o .build/mount-control.test ./internal/mount
limactl shell default env GAT_FUSE_TEST=1 \
  /Users/ramon/src/gat/.build/mount-control.test \
  -test.run '^TestMountedSwitchAndConcurrentPublisher$' -test.v

GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o .build/gat-linux ./cmd/gat
```

Stop the service before replacing its binaries. Inside Lima:

```sh
systemctl --user stop gat-demo
install -m 755 /Users/ramon/src/gat/.build/gat-linux ~/gat-demo/bin/gat
```

Then use the start command above. Regenerate protobuf code with `buf generate`
after editing the schema. The server and client must both use the new protobuf
protocol; the earlier newline-based client is incompatible.
