package repo

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

func TestLogLiteralPathNeedsNoHistoricalBlobs(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	command(t, source, "config", "uploadpack.allowFilter", "true")
	write(t, source, "dir/target", []byte("first\n"))
	write(t, source, "dir/unrelated", []byte(strings.Repeat("unrelated contents\n", 10000)))
	commit(t, source)
	write(t, source, "dir/target", []byte("second\n"))
	tip := commit(t, source)
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, source, "clone", "--bare", "--filter=blob:none", "--quiet", "file://"+source, thin)
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ImportPacks(t.Context(), thin); err != nil {
		t.Fatal(err)
	}
	p.Demand = func(_ context.Context, ids []string) error {
		return fmt.Errorf("file history requested unavailable blobs: %v", ids)
	}
	if err = p.IngestHistory(t.Context(), tip, thin); err != nil {
		t.Fatal(err)
	}
	s, err := p.Open(t.Context(), tip)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = s.LogPaths(t.Context(), 10, false, []string{"dir/target"}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Fields(command(t, source, "log", "--format=%H", "-n", "10", "--", "dir/target"))
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v want %v", got, want)
	}
}
