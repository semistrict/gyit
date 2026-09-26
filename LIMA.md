# Linux testing in Lima

From the repository root on macOS, with an existing running Lima instance:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o .build/gyit-linux ./cmd/gyit
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c \
  -o .build/github-mount.test ./internal/githubmount
limactl shell default env GYIT_GITHUB_FUSE_TEST=1 \
  "$PWD/.build/github-mount.test" -test.run TestMountedBackgroundPublication -test.v
```

The VM needs Git, `/dev/fuse`, and `fusermount3` (`fuse3` on Ubuntu).
The mounted integration test creates a small local remote, verifies NOTICE
progress and complete-tree publication, then unmounts and removes its fixture.

For interactive use, inside Lima, run the built binary from the shared checkout:

```sh
mkdir -p "$HOME/gyit"
/path/to/gyit/.build/gyit-linux mount "$HOME/gyit"
```

In another shell, `cd ~/gyit/github.com/torvalds/linux` starts setup. Read `NOTICE`
until it is replaced with the repository files. Stop the mount with Ctrl-C.
Prepared snapshots remain available for reuse.
