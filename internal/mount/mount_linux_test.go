//go:build linux

package mount

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"gat/internal/control"
	"gat/internal/controlcli"
	pb "gat/internal/gen/gat/control/v1"
	"gat/internal/repo"
	"gat/internal/store"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Exercise the real go-fuse bridge without /dev/fuse. A path GETATTR with no
// file handle can still reach Node.Getattr with an arbitrary open handle.
func TestPathGetattrAfterSwitchWithOpenHandle(t *testing.T) {
	source := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(t.Context(), "git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	oldBody, newBody := "old bytes\n", "new contents with a different size\n"
	commit := func(body string) string {
		t.Helper()
		if err := os.WriteFile(filepath.Join(source, "file"), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		git("add", "file")
		git("commit", "-qm", "snapshot")
		return git("rev-parse", "HEAD")
	}
	first, second := commit(oldBody), commit(newBody)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Import(t.Context(), backend, repo.ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, err := repo.New(backend, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := repository.OpenRevision(t.Context(), first, "")
	if err != nil {
		t.Fatal(err)
	}
	state := control.New(repository, initial)
	raw := fs.NewNodeFS(&node{state: state}, &fs.Options{RootStableAttr: &fs.StableAttr{Ino: 1}})
	var before fuse.EntryOut
	if status := raw.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &before); status != fuse.OK {
		t.Fatal(status)
	}
	header := fuse.InHeader{NodeId: before.NodeId}
	var oldOpen fuse.OpenOut
	if status := raw.Open(nil, &fuse.OpenIn{InHeader: header, Flags: syscall.O_RDONLY}, &oldOpen); status != fuse.OK {
		t.Fatal(status)
	}
	defer raw.Release(nil, &fuse.ReleaseIn{InHeader: header, Fh: oldOpen.Fh})
	read := func(fh uint64, want string) {
		t.Helper()
		buf := make([]byte, 256)
		result, status := raw.Read(nil, &fuse.ReadIn{InHeader: header, Fh: fh, Size: uint32(len(buf))}, buf)
		if status != fuse.OK {
			t.Fatal(status)
		}
		defer result.Done()
		data, status := result.Bytes(buf)
		if status != fuse.OK || string(data) != want {
			t.Fatalf("handle bytes %q, want %q: %v", data, want, status)
		}
	}
	read(oldOpen.Fh, oldBody)
	response := state.Handle(t.Context(), &pb.Request{Version: control.Version, Operation: &pb.Request_Switch{Switch: &pb.SwitchRequest{Revision: second}}})
	if response.GetError() != nil || response.GetSnapshot().GetSha() != second {
		t.Fatalf("switch: %v", response)
	}
	var after fuse.AttrOut
	// Flags/Fh are deliberately absent: this is path stat, not an fd request.
	if status := raw.GetAttr(nil, &fuse.GetAttrIn{InHeader: header}, &after); status != fuse.OK {
		t.Fatal(status)
	}
	if after.Size != uint64(len(newBody)) || after.Ino != before.Ino {
		t.Fatalf("path attributes after switch: size=%d inode=%d; want size=%d inode=%d", after.Size, after.Ino, len(newBody), before.Ino)
	}
	read(oldOpen.Fh, oldBody)
	var newOpen fuse.OpenOut
	if status := raw.Open(nil, &fuse.OpenIn{InHeader: header, Flags: syscall.O_RDONLY}, &newOpen); status != fuse.OK {
		t.Fatal(status)
	}
	defer raw.Release(nil, &fuse.ReleaseIn{InHeader: header, Fh: newOpen.Fh})
	read(newOpen.Fh, newBody)
	var lookedUp fuse.EntryOut
	if status := raw.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &lookedUp); status != fuse.OK {
		t.Fatal(status)
	}
	if lookedUp.Ino != before.Ino || lookedUp.Size != uint64(len(newBody)) {
		t.Fatal("lookup did not keep path inode and refresh attributes", lookedUp.Attr)
	}
	for range 3 {
		if status := raw.GetAttr(nil, &fuse.GetAttrIn{InHeader: header}, &after); status != fuse.OK || after.Size != uint64(len(newBody)) {
			t.Fatal("path attributes depend on which handle bridge chooses", after.Attr, status)
		}
	}
}

// Hold both writers immediately before CAS so they race with the same HEAD token.
type racingPublisher struct {
	store.Store
	ready   chan<- struct{}
	release <-chan struct{}
}

func (s *racingPublisher) Put(ctx context.Context, key string, data []byte, token string) error {
	if key == "HEAD" {
		select {
		case s.ready <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, data, token)
}

func TestMountedSwitchAndConcurrentPublisher(t *testing.T) {
	if os.Getenv("GAT_FUSE_TEST") != "1" {
		t.Skip("set GAT_FUSE_TEST=1 on Linux with /dev/fuse and fusermount3")
	}
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git: %v: %s", err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	commit := func() string { git("add", "."); git("commit", "-qm", "fixture"); return git("rev-parse", "HEAD") }
	git("init", "-q")
	raw := make([]byte, 128<<10)
	rand.New(rand.NewSource(14)).Read(raw)
	before := "before" + string(raw)
	after := "after-longer" + string(raw)
	write("a", before)
	write("sub/b", "nested-before")
	write("removed", "gone")
	write("typechange/child", "old child")
	for i := 0; i < 300; i++ {
		write(fmt.Sprintf("wide/%04d", i), "entry")
	}
	first := commit()
	s, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Import(context.Background(), s, repo.ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	start := func() (string, string) {
		t.Helper()
		mountpoint := t.TempDir()
		socketDir := t.TempDir()
		if err := os.Chmod(socketDir, 0700); err != nil {
			t.Fatal(err)
		}
		socket := filepath.Join(socketDir, "control.sock")
		r, _ := repo.New(s, 1<<20)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- Run(ctx, r, first, mountpoint, socket) }()
		t.Cleanup(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("mount did not stop")
			}
		})
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(filepath.Join(mountpoint, "a")); err == nil {
				break
			}
			select {
			case err := <-done:
				t.Fatalf("mount failed: %v", err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("mount did not become ready")
			}
			time.Sleep(10 * time.Millisecond)
		}
		return mountpoint, socket
	}
	m1, socket := start()
	m2, socket2 := start()
	for _, pair := range [][2]string{{m1, socket}, {m2, socket2}} {
		for _, path := range []string{pair[0], filepath.Join(pair[0], "sub")} {
			got, err := control.Discover(path)
			if err != nil || got != pair[1] {
				t.Fatal("discover correct mount", path, got, err)
			}
		}
	}
	read := func(root, name, want string) {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil || string(b) != want {
			t.Fatalf("read %s: %q want %q: %v", name, b, want, err)
		}
	}
	ino := func(name string) uint64 {
		t.Helper()
		s, err := os.Stat(filepath.Join(m1, name))
		if err != nil {
			t.Fatal(err)
		}
		return s.Sys().(*syscall.Stat_t).Ino
	}
	aIno, bIno, dirIno := ino("a"), ino("sub/b"), ino("sub")
	f, err := os.Open(filepath.Join(m1, "a"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := os.Stat(filepath.Join(m1, "added")); !os.IsNotExist(err) {
		t.Fatal("expected absent file", err)
	}
	wide, err := os.ReadDir(filepath.Join(m1, "wide"))
	if err != nil || len(wide) != 300 {
		t.Fatal("paged readdir", len(wide), err)
	}
	write("a", after)
	write("sub/b", "nested-after")
	write("added", "new")
	os.Remove(filepath.Join(dir, "removed"))
	os.RemoveAll(filepath.Join(dir, "typechange"))
	write("typechange", "now a file")
	second := commit()
	publishCtx, cancelPublish := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelPublish()
	ready, release := make(chan struct{}, 2), make(chan struct{})
	type publication struct {
		stats repo.Stats
		err   error
	}
	results := make(chan publication, 2)
	for range 2 {
		go func() {
			stats, err := repo.Import(publishCtx, &racingPublisher{Store: s, ready: ready, release: release}, repo.ImportOptions{Repo: dir})
			results <- publication{stats, err}
		}()
	}
	for range 2 {
		select {
		case <-ready:
		case result := <-results:
			t.Fatalf("writer failed before publication: %v", result.err)
		case <-publishCtx.Done():
			t.Fatal(publishCtx.Err())
		}
	}
	read(m1, "a", before)
	read(m2, "a", before)
	close(release)
	var stats repo.Stats
	successes, conflicts := 0, 0
	for range 2 {
		select {
		case result := <-results:
			if result.err == nil {
				successes++
				stats = result.stats
			} else if errors.Is(result.err, store.ErrConflict) {
				conflicts++
			} else {
				t.Fatal(result.err)
			}
		case <-publishCtx.Done():
			t.Fatal(publishCtx.Err())
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("publication results: successes=%d conflicts=%d", successes, conflicts)
	}
	if stats.DeltaChunks == 0 {
		t.Fatal("mount fixture must exercise a delta-backed version")
	}
	read(m1, "a", before)
	read(m2, "a", before)
	client := control.Client{Socket: socket}
	if status, err := client.Status(context.Background()); err != nil || status.Sha != first {
		t.Fatal("initial status", status, err)
	}
	if _, err := client.Switch(context.Background(), strings.Repeat("0", 40)); err == nil {
		t.Fatal("switched to missing commit")
	}
	read(m1, "a", before)
	t.Chdir(filepath.Join(m1, "sub"))
	var stdout, stderr bytes.Buffer
	if err := controlcli.Run(context.Background(), []string{"switch", second[:12]}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), second) {
		t.Fatal("switch discovered from nested working directory", stdout.String(), stderr.String(), err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"log", "--oneline", "b"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	wantLog := git("log", "--oneline", "--no-decorate", "--abbrev=7", "--", "sub/b") + "\n"
	if stdout.String() != wantLog {
		t.Fatalf("nested file log differs: got %q want %q", stdout.String(), wantLog)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"log", "--oneline", "--", "../removed"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	wantLog = git("log", "--oneline", "--no-decorate", "--abbrev=7", "--", "removed") + "\n"
	if stdout.String() != wantLog {
		t.Fatalf("deleted file log differs: got %q want %q", stdout.String(), wantLog)
	}

	for _, pattern := range []string{":(glob)*", ":(top,literal)sub/b", ":/sub/b", ":(exclude)b", "../a", ":(glob)../wide/00??"} {
		stdout.Reset()
		if err := controlcli.Run(context.Background(), []string{"log", "--oneline", "--", pattern}, &stdout, &stderr); err != nil {
			t.Fatal(pattern, err)
		}
		cmd := exec.Command("git", "-C", filepath.Join(dir, "sub"), "-c", "log.decorate=false", "log", "--oneline", "--abbrev=7", "--", pattern)
		expected, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		if stdout.String() != string(expected) {
			t.Fatalf("nested pathspec %q: got %q want %q", pattern, stdout.String(), expected)
		}
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"status"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), "HEAD detached at "+second[:7]) {
		t.Fatal("status discovered after switch", stdout.String(), err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"status", "--socket", socket2}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), "HEAD detached at "+first[:7]) {
		t.Fatal("explicit socket must override working directory", stdout.String(), err)
	}
	if status, err := client.Status(context.Background()); err != nil || status.Sha != second {
		t.Fatal("switched status", status, err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"log", "--oneline", "-n", "1"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), second[:7]+" fixture") {
		t.Fatal("log discovered from mounted subdirectory", stdout.String(), err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"blame", "--line-porcelain", "b"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), second+" 1 1 1\n") || !strings.Contains(stdout.String(), "filename sub/b\n\tnested-after") {
		t.Fatal("blame from nested cwd", stdout.String(), err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"diff", first, "--", "b"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), "diff --git a/sub/b b/sub/b") || !strings.Contains(stdout.String(), "+nested-after") {
		t.Fatal("diff from nested cwd", stdout.String(), err)
	}
	stdout.Reset()
	if err := controlcli.Run(context.Background(), []string{"annotate", "b"}, &stdout, &stderr); err != nil || !strings.Contains(stdout.String(), "nested-after") {
		t.Fatal("annotate from nested cwd", stdout.String(), err)
	}
	// New read-only commands discover the same mount from a nested cwd.
	for _, args := range [][]string{{"ls-tree", "HEAD"}, {"ls-files", "-s"}, {"cat-file", "-p", "HEAD:./b"}, {"grep", "-n", "nested", "--", "b"}, {"grep", "-n", "nested", "HEAD:./b"}, {"show", "HEAD:./b"}, {"show", "--name-only", "HEAD", "--", "b"}, {"rev-parse", "--show-prefix"}, {"rev-parse", "--show-cdup"}, {"rev-list", "-n1", "HEAD"}, {"merge-base", "HEAD", "HEAD"}} {
		stdout.Reset()
		stderr.Reset()
		if err := controlcli.Run(t.Context(), args, &stdout, &stderr); err != nil {
			t.Fatalf("nested %v: %v", args, err)
		}
		native := exec.Command("git", append([]string{"-C", filepath.Join(dir, "sub"), "-c", "color.ui=false", "-c", "log.decorate=false"}, args...)...)
		want, err := native.Output()
		if err != nil {
			t.Fatalf("native %v: %v", args, err)
		}
		if !bytes.Equal(stdout.Bytes(), want) {
			t.Fatalf("nested %v: got %q want %q", args, stdout.Bytes(), want)
		}
	}
	stdout.Reset()
	if err := controlcli.Run(t.Context(), []string{"rev-parse", "--show-toplevel"}, &stdout, &stderr); err != nil || strings.TrimSpace(stdout.String()) != m1 {
		t.Fatalf("mount root: %q %v", stdout.String(), err)
	}
	read(m1, "a", after)
	read(m1, "sub/b", "nested-after")
	read(m1, "added", "new")
	read(m1, "typechange", "now a file")
	read(m2, "a", before)
	if ino("a") != aIno || ino("sub/b") != bIno || ino("sub") != dirIno {
		t.Fatal("path inode changed across switch")
	}
	for _, check := range []struct{ root, body string }{{m1, after}, {m2, before}} {
		info, err := os.Lstat(filepath.Join(check.root, "a"))
		if err != nil || info.Size() != int64(len(check.body)) {
			t.Fatalf("path size with old handle still open: %v %v; want %d", info, err, len(check.body))
		}
	}
	if _, err := os.Stat(filepath.Join(m1, "removed")); !os.IsNotExist(err) {
		t.Fatal("removed file remained", err)
	}
	b := make([]byte, 6)
	if _, err := f.ReadAt(b, 0); err != nil || string(b) != "before" {
		t.Fatal("open handle did not stay pinned", string(b), err)
	}
	if err := os.WriteFile(filepath.Join(m1, "a"), []byte("write"), 0644); err == nil {
		t.Fatal("read-only mount accepted write")
	}
	entries, err := os.ReadDir(m1)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range entries {
		names[e.Name()] = true
	}
	if !names["added"] || names["removed"] {
		t.Fatal("root directory stale", names)
	}
}

func TestControlAttributeBufferSemantics(t *testing.T) {
	n := &node{controlEndpoint: []byte("endpoint")}
	if size, err := n.Getxattr(context.Background(), control.EndpointAttribute, nil); size != 8 || err != 0 {
		t.Fatal(size, err)
	}
	if _, err := n.Getxattr(context.Background(), control.EndpointAttribute, make([]byte, 2)); err != syscall.ERANGE {
		t.Fatal(err)
	}
	data := make([]byte, 8)
	if size, err := n.Getxattr(context.Background(), control.EndpointAttribute, data); size != 8 || err != 0 || string(data) != "endpoint" {
		t.Fatal(size, err, data)
	}
	if _, err := n.Getxattr(context.Background(), "user.other", nil); err != syscall.ENODATA {
		t.Fatal(err)
	}
	n.path = "child"
	if _, err := n.Getxattr(context.Background(), control.EndpointAttribute, nil); err != syscall.ENODATA {
		t.Fatal(err)
	}
}
