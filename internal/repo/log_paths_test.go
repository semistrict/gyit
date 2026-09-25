package repo

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"gat/internal/store"
)

func TestLogPathsMergeResolutionMatchesGit(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-q", "-b", "main")
	write(t, dir, "file", []byte("base\n"))
	commit(t, dir)
	command(t, dir, "checkout", "-qb", "topic")
	write(t, dir, "file", []byte("topic\n"))
	commit(t, dir)
	command(t, dir, "checkout", "-q", "main")
	write(t, dir, "file", []byte("main\n"))
	commit(t, dir)
	// Conflict resolution differs from both parents and must appear in file history.
	merge := exec.Command("git", "-C", dir, "-c", "user.name=Test", "-c", "user.email=test@example.test", "merge", "--no-ff", "--no-commit", "topic")
	if err := merge.Run(); err == nil {
		t.Fatal("fixture must conflict")
	}
	write(t, dir, "file", []byte("resolved\n"))
	sha := commit(t, dir)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	s, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	for _, firstParent := range []bool{false, true} {
		args := []string{"log", "--format=%H"}
		if firstParent {
			args = append(args, "--first-parent")
		}
		args = append(args, "--", "file")
		want := strings.Fields(command(t, dir, args...))
		var got []string
		if err := s.LogPaths(ctx, 20, firstParent, []string{"file"}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
			t.Fatal(err)
		}
		if strings.Join(got, " ") != strings.Join(want, " ") {
			t.Fatalf("first-parent=%t: got %v want %v", firstParent, got, want)
		}
	}
}

func TestFollowExactRenameReadsMetadataOnly(t *testing.T) {
	dir := t.TempDir()
	command(t, dir, "init", "-qb", "main")
	write(t, dir, "old", []byte(strings.Repeat("large file content\n", 100000)))
	root := commit(t, dir)
	command(t, dir, "mv", "old", "new")
	tip := commit(t, dir)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Import(t.Context(), local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	measured := &countedStore{Store: local}
	r, _ := New(measured, 32<<20)
	s, err := r.Open(t.Context(), tip)
	if err != nil {
		t.Fatal(err)
	}
	measured.reset()
	var commits []string
	if err := s.LogWithOptions(t.Context(), LogOptions{Count: 20, Follow: true, Paths: []string{"new"}}, func(e LogEntry) error { commits = append(commits, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(commits, " ") != tip+" "+root {
		t.Fatal("did not follow exact rename", commits)
	}
	if measured.packGets != 0 {
		t.Fatalf("exact rename fetched %d file-data ranges", measured.packGets)
	}
}
