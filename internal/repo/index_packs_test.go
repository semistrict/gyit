package repo

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/store"
	bolt "go.etcd.io/bbolt"
)

type indexIOStore struct {
	store.Store
	writes, reads, wholeReads int
	bytes                     int64
}

func (s *indexIOStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if strings.HasPrefix(key, "index/") {
		s.writes++
		s.bytes += int64(len(data))
	}
	return s.Store.Put(ctx, key, data, condition)
}

func (s *indexIOStore) Get(ctx context.Context, key string, offset, length int64) ([]byte, string, error) {
	if strings.HasPrefix(key, "index/") {
		s.reads++
		if length < 0 {
			s.wholeReads++
		}
	}
	return s.Store.Get(ctx, key, offset, length)
}

// The bound measures durable object writes rather than machine-dependent time.
// Readers must fetch individual pages even when the writer batches pages.
func TestIndexBatchesWritesAndReadsOnlyPages(t *testing.T) {
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &indexIOStore{Store: local}
	db, err := bolt.Open(filepath.Join(t.TempDir(), "stage.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		for i := 0; i < 20000; i++ {
			value, err := marshal(object{Kind: "blob", Size: int64(i)})
			if err != nil {
				return err
			}
			if err := b.Put([]byte(fmt.Sprintf("o/%040d", i)), value); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	idx := &index{store: s, cache: newCache(64 << 10)}
	start := time.Now()
	err = db.View(func(tx *bolt.Tx) error {
		var err error
		idx.root, err = idx.update(t.Context(), tx.Bucket([]byte("records")))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("index build: %s; durable writes=%d bytes=%d", time.Since(start), s.writes, s.bytes)
	if s.writes > 2 {
		t.Fatalf("20,000 records need at most two packed writes, got %d", s.writes)
	}
	for _, i := range []int{0, 10000, 19999} {
		var got object
		if err := idx.get(t.Context(), fmt.Sprintf("o/%040d", i), &got); err != nil || got.Size != int64(i) {
			t.Fatalf("record %d: %+v %v", i, got, err)
		}
	}
	if s.reads == 0 || s.wholeReads != 0 {
		t.Fatalf("index must use page ranges: reads=%d whole=%d", s.reads, s.wholeReads)
	}
}

func TestIndexPackBoundariesAndChecksums(t *testing.T) {
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &indexIOStore{Store: local}
	w := &indexWriter{ctx: t.Context(), store: s, prefix: "boundaries", data: make([]byte, 0, indexPackSize)}
	var refs []pageRef
	for i := 0; i < 20; i++ {
		e, err := w.save(page{Items: []item{{Key: fmt.Sprint(i), Value: []byte(strings.Repeat(fmt.Sprint(i%10), 512<<10))}}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, e.ID)
	}
	if err := w.flush(); err != nil {
		t.Fatal(err)
	}
	if s.writes != 2 || refs[0].Pack == refs[len(refs)-1].Pack {
		t.Fatalf("did not cross a pack boundary: %d writes", s.writes)
	}
	idx := &index{store: s, cache: newCache(64 << 10)}
	for i, ref := range refs {
		p, err := idx.page(t.Context(), ref)
		if err != nil || len(p.Items) != 1 || p.Items[0].Key != fmt.Sprint(i) || string(p.Items[0].Value) != strings.Repeat(fmt.Sprint(i%10), 512<<10) {
			t.Fatalf("page %d differs after pack rollover: %v", i, err)
		}
	}
	if s.wholeReads != 0 || idx.cache.used > idx.cache.max {
		t.Fatal("page reads exceeded their bounds")
	}
	ref := refs[len(refs)-1]
	data, _, err := local.Get(t.Context(), ref.Pack, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	data[ref.Offset+ref.Length-1] ^= 1
	if err := local.Put(t.Context(), ref.Pack, data, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := idx.page(t.Context(), ref); err == nil {
		t.Fatal("corrupt packed page accepted")
	}
	ref.Length = indexPackSize + 1
	if _, err := idx.page(t.Context(), ref); err == nil {
		t.Fatal("oversized page range accepted")
	}
}

type failIndexStore struct {
	store.Store
	fail bool
}

func (s *failIndexStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	if s.fail && strings.HasPrefix(key, "index/") {
		return fmt.Errorf("injected index upload failure")
	}
	return s.Store.Put(ctx, key, data, condition)
}

func TestIndexFlushFailureCannotPublish(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("old"))
	first := commit(t, source)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &failIndexStore{Store: local}
	if _, err := Import(t.Context(), s, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	before, token, err := s.Get(t.Context(), "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	write(t, source, "file", []byte("new"))
	second := commit(t, source)
	s.fail = true
	if _, err := Import(t.Context(), s, ImportOptions{Repo: source}); err == nil {
		t.Fatal("index flush failure ignored")
	}
	after, afterToken, err := s.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || string(before) != string(after) || token != afterToken {
		t.Fatal("failed upload changed HEAD", err)
	}
	r, _ := New(s, 0)
	if _, err := r.Open(t.Context(), second); !IsNotFound(err) {
		t.Fatal("failed generation became visible", err)
	}
	old, err := r.Open(t.Context(), first)
	if err != nil {
		t.Fatal(err)
	}
	e, err := old.Resolve(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	if _, err := old.ReadAt(t.Context(), e.OID, buf, 0); err != nil || string(buf) != "old" {
		t.Fatal("old snapshot lost", err)
	}
	s.fail = false
	if _, err := Import(t.Context(), s, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(t.Context(), second); err != nil {
		t.Fatal("retry failed", err)
	}
}
