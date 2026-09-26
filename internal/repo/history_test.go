package repo

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestHistoryLazyReadsAndLimits(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("old\n"))
	write(t, source, "stable/sub/file", []byte("unchanged\n"))
	write(t, source, "unrelated/large", bytes.Repeat([]byte("x"), maxHistoryFile+1))
	first := commit(t, source)
	write(t, source, "file", []byte("new\n"))
	write(t, source, "long-line", bytes.Repeat([]byte("a"), maxBlameLine+1))
	if err := os.Chmod(filepath.Join(source, "unrelated/large"), 0700); err != nil {
		t.Fatal(err)
	}
	second := commit(t, source)
	local, _ := store.NewLocal(t.TempDir())
	counted := &countedStore{Store: local}
	if _, err := Import(ctx, counted, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(counted, 1<<20)
	ends, err := r.OpenRevisions(ctx, []string{first, second}, "")
	if err != nil {
		t.Fatal(err)
	}
	counted.reset()
	var out bytes.Buffer
	if err := ends[0].Diff(ctx, ends[1], DiffOptions{NameOnly: true}, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "file\nlong-line\nunrelated/large\n" || counted.packGets != 0 {
		t.Fatal("name-only fetched contents", out.String(), counted.packGets)
	}
	counted.reset()
	out.Reset()
	if err := ends[0].Diff(ctx, ends[1], DiffOptions{Paths: []string{"unrelated"}}, &out); err != nil || !strings.Contains(out.String(), "old mode 100644\nnew mode 100755") || counted.packGets != 0 {
		t.Fatal("mode-only diff fetched contents", err, counted.packGets)
	}
	counted.reset()
	out.Reset()
	if err := ends[0].Diff(ctx, ends[1], DiffOptions{Paths: []string{"stable"}}, &out); err != nil || out.Len() != 0 || counted.packGets != 0 {
		t.Fatal("unchanged subtree read contents", err, counted.packGets)
	}
	counted.reset()
	out.Reset()
	if err := ends[0].Diff(ctx, ends[1], DiffOptions{Paths: []string{"file"}, Context: 3}, &out); err != nil || !strings.Contains(out.String(), "-old\n+new\n") {
		t.Fatal("filtered patch", out.String(), err)
	}
	if counted.bytes > 1<<20 {
		t.Fatal("downloaded unrelated large file", counted.bytes)
	}
	if err := ends[1].Blame(ctx, BlameOptions{Path: "long-line"}, func(BlameLine) error { t.Fatal("oversized line emitted"); return nil }); err == nil {
		t.Fatal("accepted oversized line")
	}
	if err := ends[1].Blame(ctx, BlameOptions{Path: "unrelated/large"}, func(BlameLine) error { return nil }); err == nil {
		t.Fatal("accepted oversized file")
	}
	stop := errors.New("consumer stopped")
	if err := ends[1].Blame(ctx, BlameOptions{Path: "file"}, func(BlameLine) error { return stop }); !errors.Is(err, stop) {
		t.Fatal("lost callback error", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := ends[0].Diff(canceled, ends[1], DiffOptions{}, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatal("diff ignored cancellation", err)
	}
	if err := ends[1].Blame(canceled, BlameOptions{Path: "file"}, func(BlameLine) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal("blame ignored cancellation", err)
	}
	a, b := make([]string, 1600), make([]string, 1600)
	for i := range a {
		a[i] = "a"
		b[i] = "b"
	}
	if _, err := lineDiff(ctx, a, b); err == nil {
		t.Fatal("unbounded edit-work accepted")
	}
}
