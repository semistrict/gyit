package repo

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"

	"gyit/internal/pathspec"
	"gyit/internal/store"
)

func TestCombinedMergeNoPatchAndConflict(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	command(t, dir, "init", "-q", "-b", "main")
	write(t, dir, "shared", []byte("base\n"))
	base := commit(t, dir)
	command(t, dir, "checkout", "-qb", "topic")
	write(t, dir, "topic-file", []byte("topic\n"))
	topic := commit(t, dir)
	command(t, dir, "checkout", "-q", "main")
	write(t, dir, "main-file", []byte("main\n"))
	commit(t, dir)
	command(t, dir, "merge", "--no-ff", "-qm", "merge", topic)
	clean := command(t, dir, "rev-parse", "HEAD")
	command(t, dir, "checkout", "-q", "-B", "topic", base)
	write(t, dir, "shared", []byte("topic\n"))
	topic = commit(t, dir)
	command(t, dir, "checkout", "-q", "-B", "conflict", base)
	write(t, dir, "shared", []byte("main\n"))
	commit(t, dir)
	merge := exec.Command("git", "-C", dir, "merge", "--no-ff", "-m", "conflict", topic)
	merge.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
	if err := merge.Run(); err == nil {
		t.Fatal("expected source merge conflict")
	}
	write(t, dir, "shared", []byte("resolution\n"))
	resolved := commit(t, dir)
	local, _ := store.NewLocal(t.TempDir())
	if _, err := Import(ctx, local, ImportOptions{Repo: dir}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, 1<<20)
	matcher, _ := pathspec.Compile(nil, "")
	for _, tc := range []struct {
		sha         string
		unsupported bool
	}{{clean, false}, {resolved, true}} {
		s, err := r.Open(ctx, tc.sha)
		if err != nil {
			t.Fatal(err)
		}
		var parents parents
		if err := s.idx.get(ctx, "p/"+s.SHA, &parents); err != nil {
			t.Fatal(err)
		}
		var got bytes.Buffer
		err = s.showCombined(ctx, s, parents.Parents, matcher, DiffOptions{Context: 3}, &got)
		if tc.unsupported {
			if !errors.Is(err, ErrCombinedPatchUnsupported) {
				t.Fatalf("conflict error: %v", err)
			}
			if got.Len() != 0 {
				t.Fatal("unsupported combined patch emitted partial output")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		native := exec.Command("git", "-C", dir, "show", "--format=", tc.sha)
		want, err := native.Output()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), want) {
			t.Fatalf("clean combined patch differs: got %q want %q", got.Bytes(), want)
		}
	}
}
