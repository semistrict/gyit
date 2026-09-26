package repo

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gyit/internal/store"
)

// This tests public history operations and their object-store I/O contract.
// Unrelated commits must not cause one remote lookup per history step.
func TestBlameSkipsUnchangedHistoryIO(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	var input strings.Builder
	input.WriteString("blob\nmark :1\ndata 7\nstable\n\n")
	for i := 0; i < 2048; i++ {
		fmt.Fprintf(&input, "commit refs/heads/main\nmark :%d\ncommitter Test <test@example.test> %d +0000\ndata 7\nfixture\n", i+2, 1700000000+i)
		if i == 0 {
			input.WriteString("M 100644 :1 file\n")
		}
		other := fmt.Sprintf("other %d\n", i)
		fmt.Fprintf(&input, "M 100644 inline other\ndata %d\n%s\n", len(other), other)
		input.WriteByte('\n')
	}
	cmd := git(t.Context(), source, "fast-import", "--quiet")
	cmd.Stdin = strings.NewReader(input.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fixture: %v %s", err, out)
	}
	sha := command(t, source, "rev-parse", "main")
	root := command(t, source, "rev-list", "--max-parents=0", "main")
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	measured := &countedStore{Store: local}
	r, _ := New(measured, 32<<20)
	s, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	measured.reset()
	lines := 0
	if err := s.Blame(context.Background(), BlameOptions{Path: "file"}, func(l BlameLine) error {
		lines++
		if l.SHA != root || string(l.Content) != "stable\n" {
			t.Fatalf("wrong attribution: %+v", l)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if lines != 1 {
		t.Fatalf("got %d lines", lines)
	}
	if measured.gets > 64 || measured.bytes > 1<<20 {
		t.Fatalf("blame fetched unrelated history metadata: GETs=%d bytes=%d", measured.gets, measured.bytes)
	}
	// File log must likewise avoid one metadata read per unchanged ancestor.
	r, _ = New(measured, 32<<20)
	s, err = r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	measured.reset()
	entries := 0
	if err := s.LogPaths(t.Context(), 20, false, []string{"file"}, func(e LogEntry) error {
		entries++
		if e.SHA != root {
			t.Fatalf("wrong file history: %s", e.SHA)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if entries != 1 {
		t.Fatalf("got %d log entries", entries)
	}
	if measured.gets > 64 || measured.bytes > 1<<20 {
		t.Fatalf("log fetched unrelated history metadata: GETs=%d bytes=%d", measured.gets, measured.bytes)
	}

}

func TestHistoryIndexUpgradeAndPinnedBlame(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	name := "nested/space\nname"
	write(t, source, name, []byte("first\n"))
	first := commit(t, source)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, _ := New(local, 32<<20)
	old, err := repository.Open(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	check := func(s *Snapshot, sha, content string) {
		t.Helper()
		n := 0
		if err := s.Blame(t.Context(), BlameOptions{Path: name}, func(line BlameLine) error {
			n++
			if line.SHA != sha || string(line.Content) != content {
				t.Fatalf("wrong pinned attribution: %+v", line)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("got %d lines", n)
		}
	}
	// Reproduce a store written by the previous reader without changing its data.
	m, token, err := readHead(t.Context(), local)
	if err != nil {
		t.Fatal(err)
	}
	m.History = pageRef{}
	m.HistoryCount = 0
	raw, err := marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = local.Put(t.Context(), "HEAD", raw, token); err != nil {
		t.Fatal(err)
	}
	legacy, err := repository.Open(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	check(legacy, first, "first\n")
	write(t, source, name, []byte("second\n"))
	second := commit(t, source)
	if _, err = Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	current, err := repository.Open(t.Context(), second)
	if err != nil {
		t.Fatal(err)
	}
	check(current, second, "second\n")
	check(old, first, "first\n")
	check(legacy, first, "first\n")
	// Append through a merge whose first parent has unchanged file contents.
	command(t, source, "checkout", "-qb", "topic", first)
	write(t, source, "other", []byte("topic\n"))
	commit(t, source)
	command(t, source, "checkout", "-q", "main")
	command(t, source, "merge", "--no-ff", "-qm", "merge", "topic")
	merged := command(t, source, "rev-parse", "HEAD")
	if _, err = Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	s, err := repository.Open(t.Context(), merged)
	if err != nil {
		t.Fatal(err)
	}
	check(s, second, "second\n")
	check(current, second, "second\n")
	// Repeating an unchanged import must reuse the acceleration index.
	before, _, err := readHead(t.Context(), local)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	after, _, err := readHead(t.Context(), local)
	if err != nil {
		t.Fatal(err)
	}
	if before.History != after.History || before.HistoryCount != after.HistoryCount {
		t.Fatal("unchanged import rewrote history")
	}
}
