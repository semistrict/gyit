package repo

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

// Hold the writer at its second publication. Readers must already be able to
// return the newest result; releasing the barrier lets them finish the history.
type historyPublicationGate struct {
	store.Store
	mu           sync.Mutex
	enabled      bool
	publications int
	first        chan struct{}
	release      chan struct{}
}

// These publication tests intentionally make raw packs unavailable to this
// reader, so they exercise waiting for derived records. With readable packs,
// the foreground fallback should finish without waiting for that publication.
type historyIndexOnlyStore struct{ store.Store }

func (s historyIndexOnlyStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "packs/") {
		return nil, "", store.ErrNotFound
	}
	return s.Store.Get(ctx, key, off, n)
}

func (s *historyPublicationGate) Put(ctx context.Context, k string, b []byte, c string) error {
	s.mu.Lock()
	block := false
	if k == "HEAD" && s.enabled {
		s.publications++
		block = s.publications == 2
	}
	first := k == "HEAD" && s.enabled && s.publications == 1
	s.mu.Unlock()
	if block {
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := s.Store.Put(ctx, k, b, c); err != nil {
		return err
	}
	if first {
		close(s.first)
	}
	return nil
}
func TestFileLogStreamsBeforeHistoryComplete(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := 0; i < 140; i++ {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	backend := &historyPublicationGate{Store: local, first: make(chan struct{}), release: make(chan struct{})}
	writer, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	reader.store = historyIndexOnlyStore{backend}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result := make(chan string, 140)
	queryDone := make(chan error, 1)
	go func() {
		queryDone <- snapshot.LogWithOptions(ctx, LogOptions{Count: 140, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { result <- e.SHA; return nil })
	}()
	select {
	case err := <-queryDone:
		t.Fatalf("query returned before publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	backend.mu.Lock()
	backend.enabled = true
	backend.mu.Unlock()
	writerDone := make(chan error, 1)
	go func() { writerDone <- writer.IngestHistory(ctx, sha, filepath.Join(dir, ".git")) }()
	defer close(backend.release)
	select {
	case <-backend.first:
	case <-ctx.Done():
		t.Fatal("no first history publication")
	}
	select {
	case got := <-result:
		if got != sha {
			t.Fatalf("first result %s, want selected tip %s", got, sha)
		}
	case err := <-queryDone:
		t.Fatalf("query failed before first result: %v", err)
	case <-ctx.Done():
		t.Fatal("newest result waited for older history")
	}
	select {
	case err := <-writerDone:
		t.Fatalf("writer finished before older-history barrier: %v", err)
	default:
	}
	cancel()
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("writer did not cancel")
	}
	select {
	case err := <-queryDone:
		if err == nil {
			t.Fatal("incomplete history reported EOF")
		}
	case <-time.After(time.Second):
		t.Fatal("waiting query did not cancel")
	}
}

// A count-limited reader and a closed pager must not stop the repository-wide
// writer. Another reader must resume the same traversal without duplicates.
func TestFileLogResumesAcrossPublication(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for i := 0; i < 140; i++ {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	backend := &historyPublicationGate{Store: local, first: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	writer, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.ImportPacks(ctx, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	reader.store = historyIndexOnlyStore{backend}
	backend.enabled = true
	done := make(chan error, 1)
	go func() { done <- writer.IngestHistory(ctx, sha, filepath.Join(dir, ".git")) }()
	select {
	case <-backend.first:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	opt := LogOptions{Count: 1, FullCommitIDs: true, Paths: []string{"file"}}
	count := 0
	if err = snapshot.LogWithOptions(ctx, opt, func(e LogEntry) error {
		count++
		if e.SHA != sha {
			t.Errorf("tip %s != %s", e.SHA, sha)
		}
		return nil
	}); err != nil || count != 1 {
		t.Fatalf("limited query: %d, %v", count, err)
	}
	closed := errors.New("pager closed")
	if err = snapshot.LogWithOptions(ctx, opt, func(LogEntry) error { return closed }); !errors.Is(err, closed) {
		t.Fatalf("pager cancellation: %v", err)
	}
	opt.Count = 0
	opt.Unlimited = true
	results := make(chan string, 140)
	queryDone := make(chan error, 1)
	go func() {
		queryDone <- snapshot.LogWithOptions(ctx, opt, func(e LogEntry) error { results <- e.SHA; return nil })
	}()
	var got []string
	select {
	case id := <-results:
		got = append(got, id)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The blocked older publication must not be mistaken for end of history.
	select {
	case err := <-queryDone:
		t.Fatalf("premature EOF: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(backend.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-queryDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(results)
	for id := range results {
		got = append(got, id)
	}
	want := command(t, dir, "log", "--format=%H", sha, "--", "file")
	if strings.Join(got, "\n") != want {
		t.Fatalf("resumed output mismatch: got %d results", len(got))
	}
	// A never-requested path was indexed too, and an absent path has real EOF.
	assertFileHistory(t, local, dir, sha, "missing")
}

func TestFileLogWaitsAcrossShallowAcquisitionAndReportsFailure(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	command(t, dir, "config", "uploadpack.allowFilter", "true")
	for i := 0; i < 12; i++ {
		write(t, dir, "file", []byte(fmt.Sprint(i)))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	thin := filepath.Join(t.TempDir(), "thin.git")
	command(t, dir, "clone", "--bare", "--depth=4", "--filter=blob:none", "file://"+dir, thin)
	local, _ := store.NewLocal(t.TempDir())
	writer, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.ImportPacks(t.Context(), thin); err != nil {
		t.Fatal(err)
	}
	if err = writer.IngestHistory(t.Context(), sha, thin); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("shallow ingestion: %v", err)
	}
	state, err := writer.HistoryProgress(t.Context(), sha)
	if err != nil || state.Complete || state.CoveredCommits != 3 {
		t.Fatalf("partial coverage: %v, %v", state, err)
	}
	reader, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results := make(chan string, 12)
	done := make(chan error, 1)
	go func() {
		done <- snapshot.LogWithOptions(ctx, LogOptions{Count: 100, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { results <- e.SHA; return nil })
	}()
	var got []string
	for i := 0; i < 3; i++ {
		select {
		case id := <-results:
			got = append(got, id)
		case err := <-done:
			t.Fatalf("early EOF: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if want := command(t, dir, "log", "-3", "--format=%H", sha, "--", "file"); strings.Join(got, "\n") != want {
		t.Fatalf("covered prefix:\n%s\nwant:\n%s", strings.Join(got, "\n"), want)
	}
	// Failure is surfaced at the gap, without taking already covered data away.
	if err = writer.SetHistoryError(ctx, sha, "upstream unavailable"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "upstream unavailable") {
			t.Fatalf("failure: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	command(t, thin, "fetch", "--quiet", "--unshallow", "origin")
	if err = writer.ImportPacks(ctx, thin); err != nil {
		t.Fatal(err)
	}
	// A fresh writer resumes the durable frontier, not a process-local queue.
	writer, err = NewProgressive(ctx, local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.IngestHistory(ctx, sha, thin); err != nil {
		t.Fatal(err)
	}
	state, err = writer.HistoryProgress(ctx, sha)
	if err != nil || !state.Complete || state.Error != "" || state.CoveredCommits != 12 {
		t.Fatalf("complete coverage: %v, %v", state, err)
	}
	assertFileHistory(t, local, dir, sha, "file")
}

func TestFileLogEmptyRootAndPagedPaths(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	command(t, dir, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "--allow-empty", "-qm", "empty")
	empty := command(t, dir, "rev-parse", "HEAD")
	var input strings.Builder
	fmt.Fprintf(&input, "blob\nmark :1\ndata 1\nx\ncommit refs/heads/main\ncommitter Test <test@example.test> 1100000000 +0000\ndata 5\nfiles\nfrom %s\n", empty)
	names := make([]string, 3000)
	for i := range names {
		names[i] = fmt.Sprintf("%s/%04d-%s", strings.Repeat("directory-", 15), i, strings.Repeat("name-", 5))
		fmt.Fprintf(&input, "M 100644 :1 %s\n", names[i])
	}
	input.WriteString("\n")
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v %s", err, out)
	}
	command(t, dir, "repack", "-ad")
	sha := command(t, dir, "rev-parse", "HEAD")
	backend, _ := store.NewLocal(t.TempDir())
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	if err = p.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{".", names[0], names[1499], names[2999], "missing"} {
		assertFileHistory(t, backend, dir, sha, path)
	}
	assertFileHistory(t, backend, dir, empty, ".")
}

// A sparse file's history must not download every unrelated commit message.
// Real remote latency makes those serial requests dominate a cold query.
func TestSparseFileLogBoundsHistoryDownloads(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	input.WriteString("blob\nmark :1\ndata 1\nx\n")
	random := rand.New(rand.NewSource(1))
	for i := 0; i < 500; i++ {
		body := make([]byte, 12000)
		for j := range body {
			body[j] = byte(33 + random.Intn(90))
		}
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata %d\n%s\n", 100+i, 1100000000+i, len(body), body)
		if i > 0 {
			fmt.Fprintf(&input, "from :%d\n", 99+i)
		}
		if i == 0 {
			input.WriteString("M 100644 :1 file\n")
		}
		input.WriteByte('\n')
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v %s", err, out)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	backend, _ := store.NewLocal(t.TempDir())
	writer, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = writer.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	if err = writer.IngestHistory(t.Context(), sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	disk, err := store.NewDiskCache(nil, t.TempDir(), "history-reader", 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	measured := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}}
	reader, err := NewProgressive(t.Context(), measured, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, FullCommitIDs: true, Paths: []string{"file"}}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	if err != nil {
		t.Fatal(err)
	}
	want := command(t, dir, "log", "--format=%H", "-n10", sha, "--", "file")
	if strings.Join(got, "\n") != want {
		t.Fatal("sparse history differs from Git")
	}
	if n := measured.calls["GET history"]; n > 10 {
		t.Fatalf("sparse log fetched %d history containers; want at most 10 without unrelated messages", n)
	}
}

func TestFileLogPartialMergeOrder(t *testing.T) {
	dir := historySkewFixture(t)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	backend := &historyPublicationGate{Store: local, first: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	p, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = p.ImportPacks(ctx, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	reader, err := NewProgressive(ctx, backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	backend.enabled = true
	writerDone := make(chan error, 1)
	go func() { writerDone <- p.IngestHistory(ctx, sha, filepath.Join(dir, ".git")) }()
	select {
	case <-backend.first:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	result := make(chan string, 160)
	done := make(chan error, 1)
	go func() {
		done <- snapshot.LogWithOptions(ctx, LogOptions{Count: 1000, Paths: []string{"hot"}, FullCommitIDs: true}, func(e LogEntry) error { result <- e.SHA; return nil })
	}()
	var got []string
	select {
	case first := <-result:
		got = append(got, first)
	case err := <-done:
		t.Fatalf("no streamed merge result: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	want := command(t, dir, "log", "--format=%H", sha, "--", "hot")
	if got[0] != strings.Fields(want)[0] {
		t.Fatal("first result is out of order")
	}
	close(backend.release)
	select {
	case err := <-writerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(result)
	for id := range result {
		got = append(got, id)
	}
	if strings.Join(got, "\n") != want {
		t.Fatalf("streamed skewed merge traversal differs from Git:\n%s\nwant:\n%s", strings.Join(got, "\n"), want)
	}
}

type delayedDirectoryStore struct {
	store.Store
	started, release chan struct{}
}

func (s *delayedDirectoryStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "index/progressive-") && !strings.HasPrefix(key, "index/progressive-history-") {
		select {
		case <-s.started:
		default:
			close(s.started)
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestFileHistoryPublishesWhileSnapshotUploadIsBlocked(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("content"))
	commit(t, dir)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	backend := &delayedDirectoryStore{Store: local, started: make(chan struct{}), release: make(chan struct{})}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	prepared := make(chan error, 1)
	go func() { prepared <- p.PrepareSnapshot(ctx, sha) }()
	defer func() { close(backend.release); <-prepared }()
	select {
	case <-backend.started:
	case <-ctx.Done():
		t.Fatal("snapshot did not reach upload")
	}
	if err := p.IngestHistory(ctx, sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := snapshot.LogWithOptions(ctx, LogOptions{Count: 10, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != sha {
		t.Fatalf("got %v, want %s", got, sha)
	}
}

type delayedPackStore struct {
	store.Store
	started, release chan struct{}
	once             sync.Once
}

func (s *delayedPackStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "packs/") {
		s.once.Do(func() { close(s.started) })
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestFileHistoryPublishesWhilePackUploadIsBlocked(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "file", []byte("content"))
	commit(t, dir)
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	local, _ := store.NewLocal(t.TempDir())
	seed, err := NewProgressive(t.Context(), local, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = seed.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	write(t, dir, "another", []byte("new contents"))
	commit(t, dir)
	command(t, dir, "repack", "-ad")
	backend := &delayedPackStore{Store: local, started: make(chan struct{}), release: make(chan struct{})}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	imported := make(chan error, 1)
	go func() { imported <- p.ImportPacks(ctx, filepath.Join(dir, ".git")) }()
	defer func() { close(backend.release); <-imported }()
	select {
	case <-backend.started:
	case <-ctx.Done():
		t.Fatal("pack did not reach upload")
	}
	if err := p.IngestHistory(ctx, sha, filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := p.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := snapshot.LogWithOptions(ctx, LogOptions{Count: 10, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != sha {
		t.Fatalf("got %v, want %s", got, sha)
	}
}

// Incremental publications remain readable immediately, but accumulating a
// prefix must not leave one remote graph GET per small ingestion forever.
func TestIncrementalHistoryPacksGraphReads(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	var input strings.Builder
	input.WriteString("blob\nmark :1\ndata 1\nx\n")
	for i := 0; i < 128; i++ {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 1\nx\n", 100+i, 1100000000+i)
		if i > 0 {
			fmt.Fprintf(&input, "from :%d\n", 99+i)
		}
		if i == 0 {
			input.WriteString("M 100644 :1 file\n")
		}
		input.WriteByte('\n')
	}
	cmd := exec.Command("git", "-C", dir, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast-import: %v %s", err, out)
	}
	commits := strings.Fields(command(t, dir, "rev-list", "--reverse", "HEAD"))
	backend, _ := store.NewLocal(t.TempDir())
	writes := &progressiveCountStore{Store: backend}
	p, err := NewProgressive(t.Context(), writes, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.ImportPacks(t.Context(), filepath.Join(dir, ".git")); err != nil {
		t.Fatal(err)
	}
	var pinned *Snapshot
	for end := 16; end <= len(commits); end += 16 {
		if end == 64 {
			// Reject only the compaction publication, after ingestion is durable.
			if err := p.ingestHistoryBatches(t.Context(), []string{commits[end-1]}, filepath.Join(dir, ".git")); err != nil {
				t.Fatal(err)
			}
			before, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			writes.conflict = true
			err = p.CompactHistory(t.Context(), commits[end-1])
			writes.conflict = false
			if !errors.Is(err, store.ErrConflict) {
				t.Fatalf("compaction CAS: %v", err)
			}
			after, _, err := backend.Get(t.Context(), "HEAD", 0, -1)
			if err != nil || string(before) != string(after) {
				t.Fatalf("failed compaction changed durable publication: %v", err)
			}
			state, err := p.HistoryProgress(t.Context(), commits[end-1])
			if err != nil || state.GraphCompacted || !state.Complete {
				t.Fatalf("failed compaction changed local coverage: %v %v", state, err)
			}
		}
		if err := p.IngestHistory(t.Context(), commits[end-1], filepath.Join(dir, ".git")); err != nil {
			t.Fatal(err)
		}
		if end == 64 || end == 128 {
			state, err := p.HistoryProgress(t.Context(), commits[end-1])
			if err != nil || !state.GraphCompacted {
				t.Fatalf("compacted closure not recorded: %v %v", state, err)
			}
			before := writes.puts
			if err := p.CompactHistory(t.Context(), commits[end-1]); err != nil {
				t.Fatal(err)
			}
			if writes.puts != before {
				t.Fatal("already compacted history wrote new data")
			}
		}
		if end == 48 {
			old, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: backend}, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			pinned, err = old.Open(t.Context(), commits[end-1])
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	var oldRows []string
	if err := pinned.LogWithOptions(t.Context(), LogOptions{Count: 10, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { oldRows = append(oldRows, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(oldRows) != 1 || oldRows[0] != commits[0] {
		t.Fatalf("pinned publication changed: %v", oldRows)
	}
	disk, err := store.NewDiskCache(nil, t.TempDir(), "packed-graph-reader", 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer disk.Close()
	measured := &historyBenchmarkStore{Store: backend, calls: map[string]int{}, elapsed: map[string]time.Duration{}, trace: true}
	reader, err := NewProgressive(t.Context(), &historyReadOnlyStore{Store: measured}, disk, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := reader.Open(t.Context(), commits[len(commits)-1])
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = snapshot.LogWithOptions(t.Context(), LogOptions{Count: 10, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != commits[0] {
		t.Fatalf("history = %v, want oldest %s", got, commits[0])
	}
	graphs := 0
	for _, request := range measured.requests {
		if strings.Contains(request, "/progressive-history-v2-graph-") {
			graphs++
		}
	}
	if graphs > 2 {
		t.Fatalf("128 commits in small incremental publications require %d graph downloads, want <=2", graphs)
	}
}

// Local acquisition may finish before its Store publication. Coverage cannot
// depend on those unpublished bytes, including when another tip is importing.
func TestFileHistoryIgnoresUnpublishedAcquisition(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	for _, value := range []string{"first", "second"} {
		write(t, dir, "file", []byte(value))
		commit(t, dir)
	}
	sha := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "repack", "-ad")
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gitdir := filepath.Join(dir, ".git")
	if err := p.IngestHistory(t.Context(), sha, gitdir); !errors.Is(err, ErrHistoryIndexPending) {
		t.Fatalf("unpublished acquisition must remain pending: %v", err)
	}
	state, err := p.HistoryProgress(t.Context(), sha)
	if err != nil || state.Complete || state.CoveredCommits != 0 {
		t.Fatalf("unpublished objects contributed coverage: %v %v", state, err)
	}
	if err := p.ImportPacks(t.Context(), gitdir); err != nil {
		t.Fatal(err)
	}
	if err := p.IngestHistory(t.Context(), sha, gitdir); err != nil {
		t.Fatal(err)
	}
	state, err = p.HistoryProgress(t.Context(), sha)
	if err != nil || !state.Complete || state.CoveredCommits != 2 {
		t.Fatalf("publication did not resume coverage: %v %v", state, err)
	}
}
