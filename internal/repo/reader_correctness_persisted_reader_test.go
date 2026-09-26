package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gyit/internal/archive"
	archivewire "gyit/internal/archive/wire"
	readerv1 "gyit/internal/gen/verification/reader/v1"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

func TestCorrectnessPersistedReader(t *testing.T) {
	if os.Getenv("GYIT_RUN_READER_CORRECTNESS") != "1" {
		t.Skip("set GYIT_RUN_READER_CORRECTNESS=1 for persisted-store reader verification")
	}
	source, backend := os.Getenv("GYIT_CORRECTNESS_SOURCE"), os.Getenv("GYIT_CORRECTNESS_STORE")
	if !filepath.IsAbs(source) || !filepath.IsAbs(backend) {
		t.Fatal("absolute GYIT_CORRECTNESS_SOURCE and GYIT_CORRECTNESS_STORE are required")
	}
	if _, err := os.Stat(filepath.Join(backend, "HEAD")); err != nil {
		t.Fatal("persisted store must exist", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	dir := t.TempDir()
	correctnessPrepareOracle(t, ctx, source, dir)
	trapDir := filepath.Join(dir, "bin")
	if err := os.Mkdir(trapDir, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "git-called")
	if err := os.WriteFile(filepath.Join(trapDir, "git"), []byte("#!/bin/sh\nprintf 'unexpected Git invocation\\n' >> \"$GYIT_CORRECTNESS_GIT_TRAP\"\nexit 97\n"), 0700); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestCorrectnessPersistedReaderSourceFree$", "-test.count=1", "-test.timeout=180s", "-test.v")
	child.Dir = dir
	for _, e := range os.Environ() {
		name, _, _ := strings.Cut(e, "=")
		if name != "PATH" && !strings.HasPrefix(name, "GYIT_") && !strings.HasPrefix(name, "GIT_") {
			child.Env = append(child.Env, e)
		}
	}
	child.Env = append(child.Env, "PATH="+trapDir, "GYIT_CORRECTNESS_READER_CHILD=1", "GYIT_CORRECTNESS_STORE="+backend, "GYIT_CORRECTNESS_ORACLE="+dir, "GYIT_CORRECTNESS_GIT_TRAP="+marker, "GYIT_CORRECTNESS_READER_REPORT="+os.Getenv("GYIT_CORRECTNESS_READER_REPORT"), "GYIT_CORRECTNESS_REQUIRE_EDGES="+os.Getenv("GYIT_CORRECTNESS_REQUIRE_EDGES"))
	child.Stdout, child.Stderr = os.Stdout, os.Stderr
	err = child.Run()
	if _, trapErr := os.Stat(marker); !os.IsNotExist(trapErr) {
		t.Error("source-free reader attempted Git", trapErr)
	}
	if err != nil {
		t.Fatal("source-free reader child", err)
	}
}

type correctnessReadCheck struct {
	Name    string
	Seconds float64
	Over1s  bool
	Error   string
}
type correctnessReadReport struct {
	Completed                           bool
	Tip                                 string
	SourceIndependent                   bool
	Checks                              []correctnessReadCheck
	Coverage                            map[string]int
	PeakChargedBytes, PeakCapacityBytes uint64
	CacheLimitBytes                     int
	Gets, Bytes, Writes                 uint64
	Limitations                         []string
}
type correctnessReadStore struct {
	store.Store
	gets, bytes, writes atomic.Uint64
	corruptKey          string
	corruptOffset       int64
	corruptions         atomic.Uint64
}

func (s *correctnessReadStore) Get(ctx context.Context, key string, off, n int64) ([]byte, string, error) {
	s.gets.Add(1)
	b, token, err := s.Store.Get(ctx, key, off, n)
	s.bytes.Add(uint64(len(b)))
	if err == nil && key == s.corruptKey && s.corruptOffset >= off && s.corruptOffset-off < int64(len(b)) {
		b = bytes.Clone(b)
		b[s.corruptOffset-off] ^= 0x80
		s.corruptions.Add(1)
	}
	return b, token, err
}
func (s *correctnessReadStore) Put(context.Context, string, []byte, string) error {
	s.writes.Add(1)
	return fmt.Errorf("persisted reader attempted a store write")
}

func correctnessCache(r *Repository, report *correctnessReadReport) error {
	r.cache.mu.Lock()
	var lengths, capacities uint64
	for _, item := range r.cache.items {
		b := item.Value.(cached).data
		lengths += uint64(len(b))
		capacities += uint64(cap(b))
	}
	used, max := r.cache.used, r.cache.max
	r.cache.mu.Unlock()
	r.globalMu.Lock()
	slot := r.globalSizes
	r.globalMu.Unlock()
	var table uint64
	if slot != nil {
		table = slot.charged.Load()
		if table > globalTableBudget {
			return fmt.Errorf("global size table exceeds 16 MiB")
		}
	}
	report.PeakChargedBytes = max64(report.PeakChargedBytes, uint64(used)+table)
	report.PeakCapacityBytes = max64(report.PeakCapacityBytes, capacities+table)
	if lengths != uint64(used) || used > max || max > DefaultCacheBytes || uint64(used)+table > DefaultCacheBytes || capacities+table > DefaultCacheBytes {
		return fmt.Errorf("cache bound: accounted=%d lengths=%d capacities=%d table=%d max=%d", used, lengths, capacities, table, max)
	}
	return nil
}
func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
func correctnessReadSlice(ctx context.Context, s *Snapshot, b *readerv1.Blob, f *os.File, off int64, n int) error {
	want, got := bytes.Repeat([]byte{0xa5}, n), bytes.Repeat([]byte{0xa5}, n)
	wn, we := f.ReadAt(want, off)
	gn, ge := s.ReadAt(ctx, b.Oid, got, off)
	if gn != wn || errors.Is(ge, io.EOF) != errors.Is(we, io.EOF) || (ge != nil && !errors.Is(ge, io.EOF)) || !bytes.Equal(got, want) {
		return fmt.Errorf("blob %s offset=%d length=%d: got n=%d err=%v, oracle n=%d err=%v, equal=%t", b.Oid, off, n, gn, ge, wn, we, bytes.Equal(got, want))
	}
	return nil
}
func correctnessBlob(ctx context.Context, s *Snapshot, b *readerv1.Blob, dir string) error {
	if b.Path != "" {
		e, err := s.Resolve(ctx, b.Path)
		if err != nil || e.OID != b.Oid || e.Size != b.Size {
			return fmt.Errorf("resolve %s: %+v %v", b.Path, e, err)
		}
	}
	f, err := os.Open(filepath.Join(dir, b.Oid))
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	var off int64
	for off < b.Size {
		n := int(min(int64(65539), b.Size-off))
		want, got := make([]byte, n), make([]byte, n)
		wn, we := f.ReadAt(want, off)
		gn, ge := s.ReadAt(ctx, b.Oid, got, off)
		if wn != n || we != nil || gn != n || ge != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("full blob %s at %d: Git n=%d err=%v, stored n=%d err=%v", b.Oid, off, wn, we, gn, ge)
		}
		h.Write(got)
		off += int64(n)
	}
	if !bytes.Equal(h.Sum(nil), b.Sha256) {
		return fmt.Errorf("full blob digest mismatch %s", b.Oid)
	}
	offsets := []int64{0, 1, b.Size / 2, max(int64(0), b.Size-17), b.Size, b.Size + 1}
	if b.Size > ChunkSize {
		offsets = append(offsets, ChunkSize-17, ChunkSize, 2*ChunkSize-3)
	}
	for _, off := range offsets {
		for _, n := range []int{1, 31, 257, 8193} {
			if err := correctnessReadSlice(ctx, s, b, f, off, n); err != nil {
				return err
			}
		}
	}
	if n, err := s.ReadAt(ctx, b.Oid, nil, b.Size); n != 0 || err != nil {
		return fmt.Errorf("empty read: %d %v", n, err)
	}
	if _, err := s.ReadAt(ctx, b.Oid, make([]byte, 1), -1); err == nil {
		return fmt.Errorf("negative offset accepted")
	}
	return nil
}
func correctnessEntry(got Entry, want *readerv1.Entry) bool {
	return got.Name == want.Name && got.OID == want.Oid && got.Mode == want.Mode && got.Size == want.Size
}
func correctnessDirectory(ctx context.Context, s *Snapshot, d *readerv1.Directory) error {
	if !d.ByOid {
		e, err := s.Resolve(ctx, d.Path)
		if err != nil || e.OID != d.Oid || e.Mode != 0040000 {
			return fmt.Errorf("resolve directory %s: %+v %v", d.Path, e, err)
		}
	}
	var got []Entry
	after := ""
	for {
		page, err := s.ReadDir(ctx, d.Oid, after, 128)
		if err != nil {
			return err
		}
		if len(page) > 128 {
			return fmt.Errorf("directory batch exceeded bound")
		}
		if len(page) == 0 {
			break
		}
		for _, entry := range page {
			if entry.Name <= after {
				return fmt.Errorf("directory is not strictly increasing")
			}
			after = entry.Name
			got = append(got, entry)
			if len(got) > len(d.Entries) {
				return fmt.Errorf("directory returned extra entries")
			}
		}
	}
	if len(got) != len(d.Entries) {
		return fmt.Errorf("directory %s count %d want %d", d.Path, len(got), len(d.Entries))
	}
	for i, want := range d.Entries {
		if !correctnessEntry(got[i], want) {
			return fmt.Errorf("directory %s row %d: %+v want %+v", d.Path, i, got[i], want)
		}
		lookup, err := s.Lookup(ctx, d.Oid, want.Name)
		if err != nil || !correctnessEntry(lookup, want) {
			return fmt.Errorf("lookup %s/%s: %+v %v", d.Path, want.Name, lookup, err)
		}
	}
	for _, after := range []string{"", "\x01", "zzzz-correctness-nonexistent"} {
		page, err := s.ReadDir(ctx, d.Oid, after, 17)
		if err != nil {
			return err
		}
		start := sort.Search(len(d.Entries), func(i int) bool { return d.Entries[i].Name > after })
		want := d.Entries[start:min(start+17, len(d.Entries))]
		if len(page) != len(want) {
			return fmt.Errorf("directory continuation length mismatch")
		}
		for i, e := range page {
			if !correctnessEntry(e, want[i]) {
				return fmt.Errorf("directory continuation mismatch")
			}
		}
	}
	after = ""
	for i := 0; i < min(5, len(d.Entries)); i++ {
		page, err := s.ReadDir(ctx, d.Oid, after, 1)
		if err != nil || len(page) != 1 || !correctnessEntry(page[0], d.Entries[i]) {
			return fmt.Errorf("single-entry continuation mismatch: %v", err)
		}
		after = page[0].Name
	}
	missing := ".reader-verification-missing"
	for _, e := range d.Entries {
		if e.Name == missing {
			missing += "-x"
		}
	}
	if _, err := s.Lookup(ctx, d.Oid, missing); !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("missing entry did not report not found: %v", err)
	}
	for _, limit := range []int{0, 129} {
		if _, err := s.ReadDir(ctx, d.Oid, "", limit); err == nil {
			return fmt.Errorf("invalid directory batch limit accepted")
		}
	}
	return nil
}

func TestCorrectnessPersistedReaderSourceFree(t *testing.T) {
	if os.Getenv("GYIT_CORRECTNESS_READER_CHILD") != "1" {
		t.Skip("isolated child of persisted reader verification")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 170*time.Second)
	defer cancel()
	dir := os.Getenv("GYIT_CORRECTNESS_ORACLE")
	raw, err := os.ReadFile(filepath.Join(dir, "oracle.pb"))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > 8<<20 {
		t.Fatal("oracle metadata exceeds bound")
	}
	o := &readerv1.Oracle{}
	if err = (proto.UnmarshalOptions{RecursionLimit: 16}).Unmarshal(raw, o); err != nil {
		t.Fatal(err)
	}
	if len(o.Revisions) == 0 || len(o.Revisions) > 32 || len(o.Blobs) > 128 || len(o.Directories) > 32 {
		t.Fatal("oracle case bounds")
	}
	for _, b := range o.Blobs {
		if len(b.Oid) != 40 || b.Size < 0 || b.Size > correctnessMaxBlobBytes {
			t.Fatal("oracle blob bounds")
		}
		if _, err := hex.DecodeString(b.Oid); err != nil {
			t.Fatal(err)
		}
	}
	local, err := store.NewLocal(os.Getenv("GYIT_CORRECTNESS_STORE"))
	if err != nil {
		t.Fatal(err)
	}
	meter := &correctnessReadStore{Store: local}
	report := correctnessReadReport{Tip: o.Tip, SourceIndependent: os.Getenv("GYIT_CORRECTNESS_SOURCE") == "", Coverage: map[string]int{}, CacheLimitBytes: DefaultCacheBytes, Limitations: []string{"Selected object/version coverage; not a byte-for-byte walk of every historical object.", "Repository API verification; mounted FUSE and CLI behavior are verified separately.", "32 MiB assertion covers retained payload capacity plus table metadata, not process RSS or bounded in-flight decode buffers.", "Source-free child receives no source path and traps Git; it is not an OS filesystem sandbox."}}
	for _, gap := range o.Gaps {
		report.Limitations = append(report.Limitations, "Oracle gap: "+gap)
	}
	defer func() {
		report.Completed = !t.Failed()
		report.Gets = meter.gets.Load()
		report.Bytes = meter.bytes.Load()
		report.Writes = meter.writes.Load()
		if report.Writes != 0 {
			report.Completed = false
			t.Error("reader attempted writes")
		}
		if path := os.Getenv("GYIT_CORRECTNESS_READER_REPORT"); path != "" {
			b, e := json.MarshalIndent(report, "", "  ")
			if e == nil {
				e = os.WriteFile(path, append(b, '\n'), 0600)
			}
			if e != nil {
				t.Error(e)
			}
		}
	}()
	if !report.SourceIndependent {
		t.Fatal("source environment leaked to reader child")
	}
	r, err := New(meter, DefaultCacheBytes)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := map[string]*Snapshot{}
	run := func(name string, fn func() error) {
		started := time.Now()
		err := fn()
		elapsed := time.Since(started)
		check := correctnessReadCheck{Name: name, Seconds: elapsed.Seconds(), Over1s: elapsed > time.Second}
		if err != nil {
			check.Error = err.Error()
			t.Errorf("%s: %v", name, err)
		}
		if e := correctnessCache(r, &report); e != nil {
			check.Error += " cache: " + e.Error()
			t.Error(e)
		}
		report.Checks = append(report.Checks, check)
		if check.Over1s {
			t.Logf("over 1 s: %s %.6f s", name, check.Seconds)
		}
	}
	for _, rev := range o.Revisions {
		rev := rev
		run("revision/"+rev.Expression, func() error {
			s, e := r.OpenRevision(ctx, rev.Expression, o.Tip)
			if e != nil {
				return e
			}
			if s.SHA != rev.Sha || s.Tree != rev.Tree {
				return fmt.Errorf("revision differs from Git: %+v", s)
			}
			snapshots[rev.Sha] = s
			return nil
		})
	}
	if len(snapshots) != len(o.Revisions) {
		t.Fatal("cannot continue without snapshots")
	}
	var nativeBlob *readerv1.Blob
	var nativeRecipe archivewire.Recipe
	for _, b := range o.Blobs {
		b := b
		s := snapshots[b.Revision]
		if s == nil {
			t.Fatal("blob revision missing")
		}
		run("blob/"+b.Label+"/"+b.Oid, func() error {
			if err := correctnessBlob(ctx, s, b, dir); err != nil {
				return err
			}
			_, c, err := s.readBlobPart(ctx, b.Oid, 0)
			if err != nil {
				return err
			}
			switch {
			case b.Size == 0:
				report.Coverage["empty_blob"]++
			case b.Size > ChunkSize:
				report.Coverage["large_chunked_blob"]++
			case c.ArchiveRecipe != "":
				recipe, err := checkedArchiveBlob(c, b.Oid, b.Size, 0)
				if err != nil {
					return err
				}
				report.Coverage["archive_blob"]++
				if len(recipe.Frames) > 1 {
					report.Coverage["native_chain_blob"]++
				}
				if nativeBlob == nil {
					nativeBlob, nativeRecipe = b, recipe
				}
			default:
				report.Coverage["converted_blob"]++
			}
			if b.Label == "related-history" {
				report.Coverage["related_history"]++
			}
			return nil
		})
	}
	for _, d := range o.Directories {
		d := d
		s := snapshots[d.Revision]
		run("directory/"+d.Revision+":"+d.Path, func() error {
			if err := correctnessDirectory(ctx, s, d); err != nil {
				return err
			}
			var stored object
			if err := s.idx.get(ctx, "o/"+d.Oid, &stored); err != nil {
				return err
			}
			if isArchiveTree(stored.Directory) {
				report.Coverage["archive_tree"]++
			} else {
				report.Coverage["compiled_tree"]++
			}
			if len(d.Entries) > 128 {
				report.Coverage["paged_directory"]++
			}
			return nil
		})
	}
	run("pinned snapshots after revision switches", func() error {
		for i := len(o.Revisions) - 1; i >= 0; i-- {
			rev := o.Revisions[i]
			s, e := r.OpenRevision(ctx, "HEAD", rev.Sha)
			if e != nil || s.SHA != rev.Sha || s.Tree != rev.Tree {
				return fmt.Errorf("mounted HEAD selection changed: %v", e)
			}
			if snapshots[rev.Sha].SHA != rev.Sha {
				return fmt.Errorf("old snapshot mutated")
			}
		}
		if len(o.Blobs) > 0 {
			return correctnessBlob(ctx, snapshots[o.Blobs[0].Revision], o.Blobs[0], dir)
		}
		return nil
	})
	run("concurrent readers and independent repository", func() error {
		other, e := New(meter, DefaultCacheBytes)
		if e != nil {
			return e
		}
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			worker := worker
			wg.Go(func() {
				for i := 0; i < 16 && len(o.Blobs) > 0; i++ {
					b := o.Blobs[(worker*17+i)%len(o.Blobs)]
					repo := r
					if worker%2 == 1 {
						repo = other
					}
					s, e := repo.Open(ctx, b.Revision)
					if e != nil {
						errs <- e
						return
					}
					f, e := os.Open(filepath.Join(dir, b.Oid))
					if e != nil {
						errs <- e
						return
					}
					e = correctnessReadSlice(ctx, s, b, f, b.Size/2, 257)
					f.Close()
					if e != nil {
						errs <- e
						return
					}
				}
			})
		}
		wg.Wait()
		close(errs)
		for e := range errs {
			if e != nil {
				return e
			}
		}
		return correctnessCache(other, &report)
	})
	if nativeBlob != nil {
		run("corrupt archive payload exposes no bytes", func() error {
			frame := nativeRecipe.Frames[0]
			bad := &correctnessReadStore{Store: local, corruptKey: archive.Key(nativeRecipe.ArchiveID, frame.Offset/archivewire.SegmentSize), corruptOffset: int64(frame.Offset % archivewire.SegmentSize)}
			badRepo, e := New(bad, DefaultCacheBytes)
			if e != nil {
				return e
			}
			s, e := badRepo.Open(ctx, nativeBlob.Revision)
			if e != nil {
				return e
			}
			for range 2 {
				dest := bytes.Repeat([]byte{0xa5}, 31)
				n, e := s.ReadAt(ctx, nativeBlob.Oid, dest, 0)
				if e == nil || n != 0 || !bytes.Equal(dest, bytes.Repeat([]byte{0xa5}, 31)) {
					return fmt.Errorf("corruption exposed data: n=%d error=%v", n, e)
				}
			}
			if bad.corruptions.Load() < 2 {
				return fmt.Errorf("corrupt body was cached or never requested")
			}
			return correctnessCache(badRepo, &report)
		})
	}
	run("corrupt catalog rejects snapshot", func() error {
		s := snapshots[o.Tip]
		ref := s.idx.root
		bad := &correctnessReadStore{Store: local, corruptKey: ref.Pack, corruptOffset: ref.Offset}
		badRepo, e := New(bad, DefaultCacheBytes)
		if e != nil {
			return e
		}
		if _, e = badRepo.Open(ctx, o.Tip); e == nil {
			return fmt.Errorf("corrupt catalog accepted")
		}
		if bad.corruptions.Load() == 0 {
			return fmt.Errorf("catalog corruption was not exercised")
		}
		return nil
	})
	if os.Getenv("GYIT_CORRECTNESS_REQUIRE_EDGES") == "1" {
		for _, kind := range []string{"empty_blob", "large_chunked_blob", "native_chain_blob", "archive_tree", "compiled_tree", "paged_directory"} {
			if report.Coverage[kind] == 0 {
				t.Errorf("required representation was not exercised: %s", kind)
			}
		}
		if report.Coverage["related_history"] != 16 {
			t.Errorf("required 16 related file revisions; exercised %d", report.Coverage["related_history"])
		}
	}
	if _, err := os.Stat(os.Getenv("GYIT_CORRECTNESS_GIT_TRAP")); !os.IsNotExist(err) {
		t.Error("Git was called in source-free child")
	}
}
