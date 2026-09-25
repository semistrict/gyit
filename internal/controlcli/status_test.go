package controlcli

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"gat/internal/control"
	"gat/internal/repo"
	"gat/internal/store"
)

type statusStore struct {
	store.Store
	gets atomic.Int64
}

func (s *statusStore) Get(ctx context.Context, key string, offset, length int64) ([]byte, string, error) {
	s.gets.Add(1)
	return s.Store.Get(ctx, key, offset, length)
}

func TestStatusMatchesNativeGit(t *testing.T) {
	source := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", source}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test", "LC_ALL=C")
		data, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return string(data)
	}
	git("init", "-q", "-b", "main")
	for _, content := range []string{"first", "second"} {
		if err := os.WriteFile(filepath.Join(source, "file"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		git("add", ".")
		git("commit", "-qm", content)
		if content == "first" {
			git("tag", "v1")
		}
	}
	git("branch", "other")
	head := strings.TrimSpace(git("rev-parse", "HEAD"))
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := &statusStore{Store: local}
	ctx := context.Background()
	if _, err := repo.Import(ctx, storage, repo.ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, err := repo.New(storage, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := repository.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	controller := control.New(repository, initial)
	dir, err := os.MkdirTemp("", "gat-status-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "control.sock")
	server, err := control.Listen(ctx, socket, controller.Handle)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client := control.Client{Socket: socket}
	for _, revision := range []string{"", "HEAD", "other", head, "-", "v1", "HEAD~0", "main", "main~1", "main", "refs/heads/main", "main"} {
		if revision != "" {
			git("checkout", "-q", revision)
			if _, err := client.Switch(ctx, revision); err != nil {
				t.Fatal(revision, err)
			}
		}
		for _, flags := range [][]string{
			{}, {"--short"}, {"-sb"}, {"--porcelain"}, {"--porcelain=1", "--branch"},
			{"--porcelain=v2", "--branch"}, {"-z"}, {"-b", "-z"}, {"--porcelain=2", "-b", "-z"},
		} {
			want := git(append([]string{"status"}, flags...)...)
			var stdout, stderr bytes.Buffer
			args := append([]string{"status", "--socket", socket}, flags...)
			before := storage.gets.Load()
			if err := Run(ctx, args, &stdout, &stderr); err != nil {
				t.Fatal(args, err)
			}
			if stdout.String() != want {
				t.Fatalf("after checkout %q, status %v:\n got %q\nwant %q", revision, flags, stdout.String(), want)
			}
			if storage.gets.Load() != before {
				t.Fatal("status fetched object-store data")
			}
		}
	}
	before, _ := client.Status(ctx)
	if _, err := client.Switch(ctx, "missing-status-test-branch"); err == nil {
		t.Fatal("accepted missing branch")
	}
	after, _ := client.Status(ctx)
	if before.Sha != after.Sha || before.Branch != after.Branch || before.DetachedAt != after.DetachedAt {
		t.Fatal("failed switch changed status")
	}
}
