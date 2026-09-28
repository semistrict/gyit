package repo

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
)

type historyReadOnlyStore struct {
	store.Store
	denyPacks bool
}

func (s *historyReadOnlyStore) Put(context.Context, string, []byte, string) error {
	return fmt.Errorf("history query must never write")
}
func (s *historyReadOnlyStore) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	if s.denyPacks && strings.HasPrefix(k, "packs/") {
		return nil, "", fmt.Errorf("first file-history query read a historical Git pack")
	}
	return s.Store.Get(ctx, k, o, n)
}

func TestIngestedHistoryFirstQuery(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := 0; i < 4; i++ {
		write(t, dir, "never-queried", []byte(fmt.Sprint(i)))
		write(t, dir, "nested/also-new", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	backend, _ := store.NewLocal(t.TempDir())
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"never-queried", "nested/also-new"} {
		for _, count := range []int{1, 100} {
			ro := &historyReadOnlyStore{Store: backend}
			p, err := NewProgressive(t.Context(), ro, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := p.Open(t.Context(), sha)
			if err != nil {
				t.Fatal(err)
			}
			ro.denyPacks = true
			var got []string
			err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: count, FullCommitIDs: true, Paths: []string{path}}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
			if err != nil {
				t.Fatal(err)
			}
			want := command(t, dir, "log", "--format=%H", fmt.Sprintf("-n%d", count), "--", path)
			if strings.Join(got, "\n") != want {
				t.Fatalf("%s count %d: %v != %s", path, count, got, want)
			}
		}
	}
}

func TestIngestedHistoryClockSkewAndMerges(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var stream strings.Builder
	for i := 1; i <= 4; i++ {
		fmt.Fprintf(&stream, "blob\nmark :%d\ndata 1\n%d\n", i, i)
	}
	for i := 0; i < 160; i++ {
		fmt.Fprintf(&stream, "commit refs/heads/c%02d\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", i, 100+i, 1000000000+(i*17)%13)
		if i > 0 {
			fmt.Fprintf(&stream, "from :%d\n", 100+max(0, i-2))
		}
		if i > 3 && i%3 == 0 {
			fmt.Fprintf(&stream, "merge :%d\n", 100+i-3)
		}
		fmt.Fprintf(&stream, "deleteall\nM 100644 :%d hot\nM 100644 :%d nested/rare\n\n", 1+i%4, 1+(i/7)%4)
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(stream.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	command(t, dir, "symbolic-ref", "HEAD", "refs/heads/c159")
	backend, _ := store.NewLocal(t.TempDir())
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []string{"c159", "c136", "c131"} {
		sha := command(t, dir, "rev-parse", revision)
		for _, path := range []string{"hot", "nested/rare", "nested", "."} {
			for _, fp := range []bool{false, true} {
				ro := &historyReadOnlyStore{Store: backend}
				p, err := NewProgressive(t.Context(), ro, nil, t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				snapshot, err := p.Open(t.Context(), sha)
				if err != nil {
					t.Fatal(err)
				}
				ro.denyPacks = true
				var got []string
				err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: 100, Paths: []string{path}, FirstParent: fp, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
				if err != nil {
					t.Fatal(err)
				}
				args := []string{"log", "-n100", "--format=%H"}
				if fp {
					args = append(args, "--first-parent")
				}
				args = append(args, sha, "--", path)
				want := command(t, dir, args...)
				if strings.Join(got, "\n") != want {
					t.Fatalf("%s %s fp=%t got\n%s\nwant\n%s", revision, path, fp, strings.Join(got, "\n"), want)
				}
			}
		}
	}
}

func TestIngestedHistoryUpdatesAndFailedPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := 0; i < 100; i++ {
		write(t, dir, fmt.Sprintf("dir%03d/file", i), []byte("old"))
	}
	first := commit(t, dir)
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	fault := &countedStore{Store: local}
	counted := &progressiveCountStore{Store: fault}
	p, err := NewProgressive(t.Context(), counted, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ingest := func(sha string) error { return p.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")) }
	if err = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	if err = ingest(first); err != nil {
		t.Fatal(err)
	}
	_, before, err := p.fileHistoryState(t.Context(), first, "dir099")
	if err != nil {
		t.Fatal(err)
	}
	write(t, dir, "dir000/file", []byte("new"))
	second := commit(t, dir)
	command(t, dir, "repack", "-d")
	if err = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	head, _, err := local.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fault.failHead = true
	if err = ingest(second); err == nil {
		t.Fatal("publication failure must propagate")
	}
	afterHead, _, _ := local.Get(t.Context(), "HEAD", 0, -1)
	if string(head) != string(afterHead) {
		t.Fatal("failed publication changed HEAD")
	}
	if _, _, err = p.fileHistoryState(t.Context(), second, "dir000/file"); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("unpublished history visible: %v", err)
	}
	if _, _, err = p.fileHistoryState(t.Context(), first, "dir000/file"); err != nil {
		t.Fatal(err)
	}
	fault.failHead = false
	counted.bytes, counted.puts = 0, 0
	if err = ingest(second); err != nil {
		t.Fatal(err)
	}
	if err = stale.IngestHistory(t.Context(), second, filepath.Join(dir, ".git")); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale writer CAS: %v", err)
	}
	if counted.bytes > 256<<10 {
		t.Fatalf("one-file update wrote %d bytes", counted.bytes)
	}
	t.Logf("one-file update: %d bytes, %d writes", counted.bytes, counted.puts)
	_, after, err := p.fileHistoryState(t.Context(), second, "dir099")
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before.Directory, after.Directory) {
		t.Fatal("unchanged directory was rewritten")
	}
	for _, sha := range []string{first, second} {
		assertFileHistory(t, local, dir, sha, "dir000/file")
	}
	counted.bytes, counted.puts = 0, 0
	if err = ingest(second); err != nil {
		t.Fatal(err)
	}
	if counted.puts != 0 {
		t.Fatal("repeated ingestion wrote data")
	}
	write(t, dir, "dir000/file", []byte("third"))
	third := commit(t, dir)
	command(t, dir, "repack", "-d")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = p.IngestHistory(ctx, third, filepath.Join(dir, ".git")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if _, _, err = p.fileHistoryState(t.Context(), third, "dir000/file"); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("canceled history visible: %v", err)
	}
}

func assertFileHistory(t *testing.T, backend store.Store, source, sha, path string) {
	t.Helper()
	ro := &historyReadOnlyStore{Store: backend}
	p, err := NewProgressive(t.Context(), ro, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	ro.denyPacks = true
	var got []string
	err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: 100, Paths: []string{path}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := command(t, source, "log", "--format=%H", sha, "--", path)
	if strings.Join(got, "\n") != want {
		t.Fatalf("%s %s got %v want %s", sha, path, got, want)
	}
}

func TestIngestedHistoryDeletedPathsAndDisconnectedBranches(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "shape/child", []byte("initial"))
	first := commit(t, dir)
	if err := os.RemoveAll(filepath.Join(dir, "shape")); err != nil {
		t.Fatal(err)
	}
	write(t, dir, "shape", []byte("now a file"))
	commit(t, dir)
	if err := os.Remove(filepath.Join(dir, "shape")); err != nil {
		t.Fatal(err)
	}
	write(t, dir, "shape/child", []byte("returned"))
	last := commit(t, dir)
	command(t, dir, "checkout", "--orphan", "separate")
	command(t, dir, "rm", "-rf", ".")
	write(t, dir, "other", []byte("unrelated"))
	other := commit(t, dir)
	command(t, dir, "checkout", "main")
	backend, _ := store.NewLocal(t.TempDir())
	if _, err := Import(t.Context(), backend, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	for _, sha := range []string{first, last} {
		for _, path := range []string{"shape", "shape/child"} {
			assertFileHistory(t, backend, dir, sha, path)
		}
	}
	assertFileHistory(t, backend, dir, other, "other")
}
