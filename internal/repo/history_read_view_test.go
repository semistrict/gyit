//go:build !js

package repo

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

func TestHistoryReadViewUsesPublishedLocalObjects(t *testing.T) {
	dir := historySkewFixture(t)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, _ := store.NewLocal(t.TempDir())
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	p.UsePublishedHistorySources(filepath.Join(dir, ".git"))
	snapshot, err := p.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	p.store = &historyReadOnlyStore{Store: backend, denyPacks: true}
	var got []string
	var display strings.Builder
	if err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"hot"}}, func(e LogEntry) error {
		if len(got) != 0 {
			display.WriteByte('\n')
		}
		got = append(got, e.SHA)
		return WriteLogEntry(&display, e, false)
	}); err != nil {
		t.Fatal(err)
	}
	want := command(t, dir, "log", "--format=%H", "-n", "10", sha, "--", "hot")
	if strings.Join(got, "\n") != want {
		t.Fatalf("got %v want %s", got, want)
	}
	wantDisplay := command(t, dir, "log", "--no-decorate", "--no-color", "--format=medium", "-n", "10", sha, "--", "hot")
	if strings.TrimSpace(display.String()) != wantDisplay {
		t.Fatalf("deferred author/date/message formatting differs:\n%s\nwant:\n%s", display.String(), wantDisplay)
	}
	if len(historyReadViews) != 0 {
		t.Fatal("query leaked its acquisition view")
	}
	// Losing the optional acquisition files cannot lose durable query data.
	if err := os.RemoveAll(filepath.Join(dir, ".git", "objects", "pack")); err != nil {
		t.Fatal(err)
	}
	p.store = &historyReadOnlyStore{Store: backend}
	got = nil
	if err := snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"hot"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, "\n") != want {
		t.Fatalf("durable query differs: %v", got)
	}
}

func TestHistoryReadViewWaitsForPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	command(t, dir, "config", "uploadpack.allowFilter", "true")
	for i := range 3 {
		write(t, dir, "file", []byte{byte('a' + i)})
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, dir, "clone", "--bare", "--depth=1", "--filter=blob:none", "file://"+dir, thin)
	backend, _ := store.NewLocal(t.TempDir())
	writer, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.ImportPacks(t.Context(), thin); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: backend}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reader.UsePublishedHistorySources(filepath.Join(dir, ".git"))
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	results, done := make(chan string, 3), make(chan error, 1)
	go func() {
		done <- snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { results <- e.SHA; return nil })
	}()
	select {
	case id := <-results:
		t.Fatalf("unpublished local parent exposed result %s", id)
	case err := <-done:
		t.Fatalf("unfinished publication became EOF: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := writer.ImportPacks(ctx, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(results)
	var got []string
	for id := range results {
		got = append(got, id)
	}
	want := command(t, dir, "log", "--format=%H", "-n", "10", sha, "--", "file")
	if strings.Join(got, "\n") != want {
		t.Fatalf("got %v want %s", got, want)
	}
	if len(historyReadViews) != 0 {
		t.Fatal("query leaked its acquisition view")
	}
	stopped, stop := context.WithCancel(t.Context())
	stop()
	if err := snapshot.LogWithOptions(stopped, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(LogEntry) error { t.Error("emitted after cancellation"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}

func TestHistoryReadViewChoosesOneSourceAndFallsBackForUpdates(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("old"))
	commit(t, dir)
	command(t, dir, "repack", "-ad")
	old := t.TempDir()
	if err := os.MkdirAll(filepath.Join(old, "objects", "pack"), 0700); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, ".git", "objects", "pack", "*"))
	for _, name := range files {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(old, "objects", "pack", filepath.Base(name)), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(t, dir, "file", []byte("new"))
	commit(t, dir)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, _ := store.NewLocal(t.TempDir())
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{old, filepath.Join(dir, ".git")} {
		if err := p.ImportPacks(t.Context(), source); err != nil {
			t.Fatal(err)
		}
	}
	p.UsePublishedHistorySources(old, filepath.Join(dir, ".git"))
	ctx, close, err := p.historyReadView(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	source := ctx.Value(historySourceKey{}).(*historySource)
	if len(source.packs) != 1 {
		close()
		t.Fatalf("mapped %d packs, want only the preferred source", len(source.packs))
	}
	// The preferred source lacks the new tip. Its published store recipe must
	// still make it visible; using only one accelerator cannot hide updates.
	if found, _, err := source.locate(sha); err != nil || found != nil {
		close()
		t.Fatal("fixture tip unexpectedly in preferred source")
	}
	close()
	s, err := p.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := s.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if want := command(t, dir, "log", "--format=%H", "--", "file"); strings.Join(got, "\n") != want {
		t.Fatalf("%v want %s", got, want)
	}
	// Without a full ancestry source, retain both shallow windows. Denying
	// remote pack reads proves a later window cannot displace the newer one.
	p.UsePublishedHistorySources(t.TempDir(), old, filepath.Join(dir, ".git"))
	p.store = &historyReadOnlyStore{Store: backend, denyPacks: true}
	got = nil
	if err := s.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if want := command(t, dir, "log", "--format=%H", "--", "file"); strings.Join(got, "\n") != want {
		t.Fatalf("combined windows: %v want %s", got, want)
	}

}

// Read views are bounded process-wide. A consumer that stops reading (a paused
// pager) must not keep one: other file-history queries proceed, and the paused
// query resumes with complete results once its consumer reads again.
func TestPausedFileLogsDoNotHoldReadViews(t *testing.T) {
	dir := historySkewFixture(t)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, _ := store.NewLocal(t.TempDir())
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	p.UsePublishedHistorySources(filepath.Join(dir, ".git"))
	snapshot, err := p.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	want := command(t, dir, "log", "--format=%H", "-n", "10", sha, "--", "hot")
	query := func(ctx context.Context, emitted func()) (string, error) {
		var got []string
		err := snapshot.LogWithOptions(ctx, LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"hot"}}, func(e LogEntry) error {
			got = append(got, e.SHA)
			emitted()
			return nil
		})
		return strings.Join(got, "\n"), err
	}
	type result struct {
		got string
		err error
	}
	resume := make(chan struct{})
	paused := make(chan struct{}, cap(historyReadViews))
	results := make(chan result, cap(historyReadViews))
	for range cap(historyReadViews) {
		var once sync.Once
		go func() {
			got, err := query(t.Context(), func() {
				once.Do(func() {
					paused <- struct{}{}
					<-resume
				})
			})
			results <- result{got, err}
		}()
	}
	for range cap(historyReadViews) {
		<-paused
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	got, err := query(ctx, func() {})
	if err != nil || got != want {
		t.Fatalf("query while other pagers are paused: %v\n%s", err, got)
	}
	close(resume)
	for range cap(historyReadViews) {
		if r := <-results; r.err != nil || r.got != want {
			t.Fatalf("resumed query: %v\n%s", r.err, r.got)
		}
	}
	if len(historyReadViews) != 0 {
		t.Fatal("query leaked its acquisition view")
	}
}
