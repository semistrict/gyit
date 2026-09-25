package repo

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gat/internal/store"
)

func TestRevisionsMatchGitAndRefreshRefs(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			ctx := context.Background()
			source := t.TempDir()
			command(t, source, "init", "-q", "-b", "main", "--object-format="+format)
			write(t, source, "base", []byte("base"))
			first := commit(t, source)
			command(t, source, "tag", "v1")
			command(t, source, "checkout", "-qb", "topic")
			write(t, source, "topic", []byte("topic"))
			topic := commit(t, source)
			command(t, source, "checkout", "-q", "main")
			write(t, source, "main", []byte("main"))
			commit(t, source)
			command(t, source, "merge", "--no-ff", "-qm", "merge", "topic")
			head := command(t, source, "rev-parse", "HEAD")
			command(t, source, "tag", "-am", "release", "release")
			command(t, source, "tag", "-am", "nested", "nested", "release")
			tagID := command(t, source, "rev-parse", "release")
			command(t, source, "branch", "UpperCase", topic)
			command(t, source, "update-ref", "refs/remotes/origin/dev", topic)
			storage, _ := store.NewLocal(t.TempDir())
			counted := &countedStore{Store: storage}
			stats, err := Import(ctx, counted, ImportOptions{Repo: source})
			if err != nil {
				t.Fatal(err)
			}
			repository, _ := New(counted, 1<<20)
			for _, rev := range []string{"main", "refs/heads/main", "heads/main", "topic", "UpperCase", "v1", "release", "nested", tagID, tagID[:8], head[:8], "HEAD", "@", "HEAD~", "HEAD^2", "HEAD~1^", "HEAD^0", "HEAD~0", "release^{}", "nested^{commit}", "release^2~1", "origin/dev"} {
				want := command(t, source, "rev-parse", "--verify", rev+"^{commit}")
				counted.reset()
				got, err := repository.OpenRevision(ctx, rev, "")
				if err != nil || got.SHA != want {
					t.Fatalf("%s got %v: %v want %s", rev, got, err, want)
				}
				if counted.packGets != 0 {
					t.Fatal("revision resolution fetched blob data", rev)
				}
			}
			if got, err := repository.OpenRevision(ctx, "dev", ""); err != nil || got.SHA != topic {
				t.Fatal("remote branch guess", got, err)
			}
			if got, err := repository.OpenRevision(ctx, "HEAD", first); err != nil || got.SHA != first {
				t.Fatal("mount HEAD", got, err)
			}
			for _, rev := range []string{"uppercase", "missing", "HEAD^3", "v1~1", "main:base", "HEAD@{yesterday}", "main..topic", "HEAD~18446744073709551616"} {
				if _, err := repository.OpenRevision(ctx, rev, ""); err == nil {
					t.Fatal("accepted invalid revision", rev)
				}
			}
			repeated, err := Import(ctx, counted, ImportOptions{Repo: source})
			if err != nil || repeated.Generation != stats.Generation || repeated.Objects != 0 {
				t.Fatal("unchanged ref import", repeated, err)
			}
			command(t, source, "branch", "-D", "UpperCase")
			command(t, source, "update-ref", "refs/heads/main", first)
			command(t, source, "branch", "new-name", topic)
			command(t, source, "update-ref", "refs/remotes/upstream/dev", topic)
			if _, err := Import(ctx, counted, ImportOptions{Repo: source}); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.OpenRevision(ctx, "dev", ""); !errors.Is(err, ErrInvalidRevision) {
				t.Fatal("ambiguous remote guess", err)
			}
			if _, err := repository.OpenRevision(ctx, "UpperCase", ""); !errors.Is(err, store.ErrNotFound) {
				t.Fatal("deleted ref remained", err)
			}
			if got, err := repository.OpenRevision(ctx, "main", ""); err != nil || got.SHA != first {
				t.Fatal("moved ref", got, err)
			}
			if got, err := repository.OpenRevision(ctx, "new-name", ""); err != nil || got.SHA != topic {
				t.Fatal("new ref", got, err)
			}
			if got, err := repository.OpenRevision(ctx, head, ""); err != nil || got.SHA != head {
				t.Fatal("old commit lost", got, err)
			}
		})
	}
}

func TestRevisionMetadataUpgradeAndRestrictedImport(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	write(t, source, "file", []byte("first"))
	first := commit(t, source)
	command(t, source, "tag", "old")
	write(t, source, "file", []byte("second"))
	second := commit(t, source)
	storage, _ := store.NewLocal(t.TempDir())
	counted := &countedStore{Store: storage}
	_, err := Import(ctx, counted, ImportOptions{Repo: source, Revision: first})
	if err != nil {
		t.Fatal(err)
	}
	repository, _ := New(counted, 1<<20)
	if _, err := repository.OpenRevision(ctx, "main", ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("published ref to unimported commit", err)
	}
	if got, err := repository.OpenRevision(ctx, "old", ""); err != nil || got.SHA != first {
		t.Fatal(got, err)
	}
	m, token, err := readHead(ctx, counted)
	if err != nil {
		t.Fatal(err)
	}
	m.Refs = pageRef{}
	m.RefsHash = ""
	m.RevisionGraph = false
	data, _ := marshal(m)
	if err := counted.Put(ctx, "HEAD", data, token); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.OpenRevision(ctx, first+"~", ""); err == nil || !strings.Contains(err.Error(), "re-run import") {
		t.Fatal("legacy graph must request upgrade", err)
	}
	counted.reset()
	if _, err := Import(ctx, counted, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	if counted.packGets != 0 {
		t.Fatal("upgrade read old blob packs")
	}
	if got, err := repository.OpenRevision(ctx, "main~1", ""); err != nil || got.SHA != first {
		t.Fatal(got, err)
	}
	if got, err := repository.OpenRevision(ctx, "main", ""); err != nil || got.SHA != second {
		t.Fatal(got, err)
	}
}

func TestAbbreviatedObjectAmbiguity(t *testing.T) {
	seen := map[string]string{}
	var a, b, full string
	for i := 0; i < 10000; i++ {
		data := fmt.Sprintf("collision fixture %d", i)
		hash := fmt.Sprintf("%x", sha1.Sum([]byte(fmt.Sprintf("blob %d%c%s", len(data), 0, data))))
		if previous, ok := seen[hash[:4]]; ok {
			a, b, full = previous, data, hash
			break
		}
		seen[hash[:4]] = data
	}
	if full == "" {
		t.Fatal("could not build collision fixture")
	}
	source := t.TempDir()
	command(t, source, "init", "-q", "--object-format=sha1")
	write(t, source, "a", []byte(a))
	write(t, source, "b", []byte(b))
	commit(t, source)
	storage, _ := store.NewLocal(t.TempDir())
	if _, err := Import(context.Background(), storage, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	repository, _ := New(storage, 1<<20)
	if _, err := repository.OpenRevision(context.Background(), full[:4], ""); !errors.Is(err, ErrInvalidRevision) || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatal("ambiguous short ID must fail", err)
	}
	if _, err := repository.OpenRevision(context.Background(), full, ""); !errors.Is(err, ErrInvalidRevision) {
		t.Fatal("blob target must fail", err)
	}
}
