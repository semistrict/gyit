package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	pb "gyit/internal/gen/gyit/storage/v1"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"gyit/internal/store"
)

type indexReadStore struct {
	store.Store
	gets  atomic.Int64
	packs atomic.Int64
}

func (s *indexReadStore) Get(ctx context.Context, k string, o, n int64) ([]byte, string, error) {
	if strings.HasPrefix(k, "packs/") {
		s.packs.Add(1)
	}
	if strings.HasPrefix(k, "index/") {
		s.gets.Add(1)
	}
	return s.Store.Get(ctx, k, o, n)
}
func TestProgressiveIndexContainerRead(t *testing.T) {
	backend, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	measured := &indexReadStore{Store: backend}
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "container-test"}
	var refs []pageRef
	for _, value := range []string{"first", "second"} {
		b, err := marshal(page{Items: []item{{Key: value, Value: []byte(value)}}})
		if err != nil {
			t.Fatal(err)
		}
		ref, err := w.saveBytes(b)
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	p := directoryReader(t, measured, t.TempDir(), 16<<20)
	for i, ref := range refs {
		page, err := p.index().page(t.Context(), ref)
		if err != nil {
			t.Fatal(err)
		}
		if page.Items[0].Key != []string{"first", "second"}[i] {
			t.Fatal("wrong page")
		}
	}
	if got := measured.gets.Load(); got != 1 {
		t.Fatalf("index container fetched %d times; want 1", got)
	}
	bad := refs[0]
	bad.Hash = strings.Repeat("0", 64)
	if _, err := p.index().page(t.Context(), bad); err == nil {
		t.Fatal("accepted wrong page checksum")
	}
	bad = refs[1]
	bad.Offset += 1000
	if _, err := p.index().page(t.Context(), bad); err == nil {
		t.Fatal("accepted out of bounds page")
	}
}

// Foreground history packs are already local during import. Reading a newly
// published commit should use the same bounded decoded cache as any other read.
func TestProgressiveSmallCommitPackWarmsDecodedCache(t *testing.T) {
	root := t.TempDir()
	cmd := exec.Command("git", "init", "--bare", "--quiet", root)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init: %v %s", err, out)
	}
	body := []byte("tree 0000000000000000000000000000000000000000\nauthor Test <test@example.test> 1 +0000\ncommitter Test <test@example.test> 1 +0000\n\nsubject\n")
	cmd = exec.Command("git", "-C", root, "hash-object", "-t", "commit", "-w", "--stdin")
	cmd.Stdin = bytes.NewReader(body)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	oid := strings.TrimSpace(string(out))
	cmd = exec.Command("git", "-C", root, "pack-objects", filepath.Join(root, "objects", "pack", "pack"))
	cmd.Stdin = strings.NewReader(oid + "\n")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pack: %v %s", err, out)
	}
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	measured := &indexReadStore{Store: local}
	p := directoryReader(t, measured, t.TempDir(), 16<<20)
	if err := p.ImportPacks(t.Context(), root); err != nil {
		t.Fatal(err)
	}
	raw, kind, err := p.object(t.Context(), oid)
	if err != nil || kind != 1 || !bytes.Equal(raw, body) {
		t.Fatalf("object %d %q: %v", kind, raw, err)
	}
	if got := measured.packs.Load(); got != 0 {
		t.Fatalf("read freshly imported commit with %d pack GETs", got)
	}
}

type gatedPublicationStore struct {
	store.Store
	entered, release, generation chan struct{}
	fail                         bool
}

func (s *gatedPublicationStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "index/") {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		if s.fail {
			return errors.New("injected upload failure")
		}
	}
	if strings.HasPrefix(key, "generations/") {
		close(s.generation)
	}
	return s.Store.Put(ctx, key, data, condition)
}
func TestProgressivePublicationWaitsForImmutableUploads(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			backend := &gatedPublicationStore{Store: local, entered: make(chan struct{}), release: make(chan struct{}), generation: make(chan struct{}), fail: fail}
			p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				uploads := newPublicationUploads(t.Context(), backend)
				defer uploads.close()
				done <- p.stage(func(b *bolt.Bucket) error {
					if err := progressivePut(b, "state/test", &pb.ProgressiveState{}); err != nil {
						return err
					}
					return p.publishUploads(t.Context(), b, uploads)
				})
			}()
			select {
			case <-backend.entered:
			case <-time.After(time.Second):
				t.Fatal("index upload never started")
			}
			select {
			case <-backend.generation:
			case <-time.After(time.Second):
				close(backend.release)
				t.Fatal("immutable uploads were serialized")
			}
			if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
				close(backend.release)
				t.Fatalf("HEAD visible before index: %v", err)
			}
			close(backend.release)
			err = <-done
			if fail {
				if err == nil {
					t.Fatal("upload failure ignored")
				}
				if p.index().root != (pageRef{}) {
					t.Fatal("failed upload changed reader root")
				}
				if _, _, err := local.Get(t.Context(), "HEAD", 0, -1); !errors.Is(err, store.ErrNotFound) {
					t.Fatal("failed upload published HEAD")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}
