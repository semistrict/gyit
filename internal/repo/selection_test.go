package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
	bolt "go.etcd.io/bbolt"
)

func TestEncoderSelectsOlderBetterBase(t *testing.T) {
	a, b := make([]byte, 64<<10), make([]byte, 64<<10)
	random := rand.New(rand.NewSource(91))
	random.Read(a)
	random.Read(b)
	target := bytes.Clone(a)
	target[100] ^= 255
	var chosen string
	seen := 0
	encoder, err := newChunkEncoder(t.Context(), 2, 1, 4, nil, func(slot *encodeSlot) error {
		if slot.anchor != nil {
			slot.anchor.location = chunkBase{Pack: "packs/test", Length: 1, Hash: slot.hash}
		}
		if seen == 2 {
			if slot.base == nil {
				t.Error("similar older base was discarded")
			} else {
				chosen = slot.base.location.Hash
			}
		}
		seen++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.close()
	for i, raw := range [][]byte{a, b, target} {
		if err := encoder.addHint(fmt.Sprint(i), "same-file/0", raw); err != nil {
			t.Fatal(err)
		}
	}
	if err := encoder.finish(); err != nil {
		t.Fatal(err)
	}
	if chosen != fmt.Sprintf("%x", sha256.Sum256(a)) {
		t.Fatal("did not choose closest retained base")
	}
}

func TestConfiguredDepthRoundTripsWithBoundedReads(t *testing.T) {
	for _, depth := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			local, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var records []chunk
			var contents [][]byte
			var stats Stats
			writer := &packWriter{ctx: t.Context(), store: local, prefix: "depth-test", stats: &stats}
			maxDepth := 0
			encoder, err := newChunkEncoder(t.Context(), 2, depth, 4, nil, func(slot *encodeSlot) error {
				c, err := writer.add(slot.compressed, slot.hash)
				if err != nil {
					return err
				}
				if slot.base != nil {
					loc := slot.base.location
					c.Base = &loc
				}
				if slot.anchor != nil {
					slot.anchor.location = chunkLocation(c)
				}
				d, err := chunkLocation(c).depth()
				if err != nil {
					return err
				}
				maxDepth = max(maxDepth, d)
				// Exercise protobuf serialization of every embedded dependency.
				wire, err := marshal(c)
				if err != nil {
					return err
				}
				var decoded chunk
				if err := unmarshal(wire, &decoded); err != nil {
					return err
				}
				records = append(records, decoded)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer encoder.close()
			raw := make([]byte, 128<<10)
			random := rand.New(rand.NewSource(701))
			random.Read(raw)
			for i := 0; i < 10; i++ {
				random.Read(raw[i*8192 : (i+1)*8192])
				contents = append(contents, bytes.Clone(raw))
				if err := encoder.addHint(fmt.Sprint(i), "same-file/0", raw); err != nil {
					t.Fatal(err)
				}
			}
			if err := encoder.finish(); err != nil {
				t.Fatal(err)
			}
			if err := writer.flush(); err != nil {
				t.Fatal(err)
			}
			if maxDepth != depth {
				t.Fatalf("fixture must exercise requested depth: got %d want %d", maxDepth, depth)
			}
			measured := &countedStore{Store: local}
			snapshot := &Snapshot{idx: &index{store: measured, cache: newCache(0)}}
			for i, c := range records {
				measured.reset()
				got, err := snapshot.readChunk(t.Context(), c)
				if err != nil || !bytes.Equal(got, contents[i]) {
					t.Fatalf("version %d: %v", i, err)
				}
				d, _ := chunkLocation(c).depth()
				if measured.packGets != d+1 || d > depth {
					t.Fatalf("read exceeded bound: depth=%d GETs=%d", d, measured.packGets)
				}
			}

			measured.reset()
			expectedGets := 0
			var wg sync.WaitGroup
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for i, c := range records {
				d, _ := chunkLocation(c).depth()
				expectedGets += d + 1
				wg.Go(func() {
					got, err := snapshot.readChunk(ctx, c)
					if err != nil || !bytes.Equal(got, contents[i]) {
						t.Errorf("parallel version %d: %v", i, err)
					}
				})
			}
			wg.Wait()
			if measured.packGets != expectedGets {
				t.Fatalf("parallel chain read exceeded bound: got %d want %d", measured.packGets, expectedGets)
			}
		})
	}
}

func TestBaseCandidateMemoryAndCountBounds(t *testing.T) {
	cache := newBaseCache(4)
	for i := 0; i < 100; i++ {
		hint := fmt.Sprint(i % 7)
		cache.put(hint, newDeltaBase(make([]byte, ChunkSize)))
		if cache.used > baseCacheBytes || len(cache.entries) > 64 || len(cache.candidates(hint)) > 4 {
			t.Fatal("unbounded candidate cache")
		}
	}
}

func TestObjectOrderPrefersLargerVersions(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	small := command(t, source, "hash-object", "-w", "--stdin")
	write(t, source, "larger", []byte(strings.Repeat("content", 100)))
	large := command(t, source, "hash-object", "-w", "larger")
	ids, err := os.Create(filepath.Join(t.TempDir(), "ids"))
	if err != nil {
		t.Fatal(err)
	}
	defer ids.Close()
	fmt.Fprintf(ids, "%s same path\n%s same path\n", small, large)
	ids.Seek(0, 0)
	if err := orderObjectHints(t.Context(), ids, t.TempDir(), source); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(ids.Name())
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != large+" same path\n"+small+" same path\n" {
		t.Fatal("versions were not ordered by descending size")
	}
}

func TestIncrementalImportsRetainAlternativeBases(t *testing.T) {
	source := t.TempDir()
	command(t, source, "init", "-q")
	a, b := make([]byte, 64<<10), make([]byte, 64<<10)
	random := rand.New(rand.NewSource(715))
	random.Read(a)
	random.Read(b)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var firstHash string
	for i, raw := range [][]byte{a, b} {
		write(t, source, "file", raw)
		sha := commit(t, source)
		if _, err := Import(t.Context(), local, ImportOptions{Repo: source}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			r, _ := New(local, DefaultCacheBytes)
			snapshot, err := r.Open(t.Context(), sha)
			if err != nil {
				t.Fatal(err)
			}
			e, err := snapshot.Resolve(t.Context(), "file")
			if err != nil {
				t.Fatal(err)
			}
			var c chunk
			if err := snapshot.idx.get(t.Context(), chunkKey(e.OID, 0), &c); err != nil {
				t.Fatal(err)
			}
			firstHash = c.Hash
		}
	}
	target := bytes.Clone(a)
	target[900] ^= 255
	write(t, source, "file", target)
	sha := commit(t, source)
	stats, err := Import(t.Context(), local, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeltaChunks != 1 || stats.UploadedBytes > 1024 {
		t.Fatalf("forgot older base across imports: %+v", stats)
	}
	r, _ := New(local, DefaultCacheBytes)
	snapshot, err := r.Open(t.Context(), sha)
	if err != nil {
		t.Fatal(err)
	}
	e, err := snapshot.Resolve(t.Context(), "file")
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	if err := snapshot.idx.get(t.Context(), chunkKey(e.OID, 0), &c); err != nil {
		t.Fatal(err)
	}
	if c.Base == nil || c.Base.Hash != firstHash {
		t.Fatal("newest base displaced the better older base")
	}
	got := make([]byte, len(target))
	if _, err := snapshot.ReadAt(t.Context(), e.OID, got, 0); err != nil || !bytes.Equal(got, target) {
		t.Fatalf("incremental content: %v", err)
	}
}

func TestRejectOutOfRangeDeltaConfigurationAndChains(t *testing.T) {
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, opt := range []ImportOptions{{DeltaDepth: -1}, {DeltaDepth: MaxDeltaDepth + 1}, {DeltaCandidates: -1}, {DeltaCandidates: maxDeltaCandidates + 1}} {
		if _, err := Import(t.Context(), local, opt); err == nil {
			t.Fatal("accepted unsupported configuration")
		}
	}
	c := chunk{Pack: "packs/test", Length: 1, Hash: strings.Repeat("a", 64)}
	for i := 0; i < MaxDeltaDepth+1; i++ {
		loc := chunkLocation(c)
		c.Base = &loc
	}
	snapshot := &Snapshot{idx: &index{store: local, cache: newCache(0)}}
	if _, err := snapshot.readChunk(t.Context(), c); err == nil {
		t.Fatal("reader accepted unbounded chain")
	}
	wire, err := marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var decoded chunk
	if err := unmarshal(wire, &decoded); err == nil {
		t.Fatal("protobuf accepted unbounded chain")
	}
}

func TestVersionFourAnchorUpgrade(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	command(t, source, "init", "-q")
	raw := make([]byte, 32<<10)
	rand.New(rand.NewSource(701)).Read(raw)
	write(t, source, "file", raw)
	first := commit(t, source)
	local, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Full chunks and index pages have identical encodings in versions 4 and 5.
	if _, err := Import(ctx, local, ImportOptions{Repo: source, DisableDeltas: true}); err != nil {
		t.Fatal(err)
	}
	r, _ := New(local, DefaultCacheBytes)
	snapshot, err := r.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	e, err := snapshot.Resolve(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	var c chunk
	if err := snapshot.idx.get(ctx, chunkKey(e.OID, 0), &c); err != nil {
		t.Fatal(err)
	}
	db, err := bolt.Open(filepath.Join(t.TempDir(), "legacy.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m, token, err := readHead(ctx, local)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		wire, err := marshal(c)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(legacyAnchorKey("file/0000000000000000")), wire); err != nil {
			return err
		}
		m.Root, err = snapshot.idx.update(ctx, b)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	m.Version = 4
	wire, err := marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := local.Put(ctx, "HEAD", wire, token); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Open(ctx, first); err != nil {
		t.Fatalf("cannot open version 4: %v", err)
	}
	raw[100] ^= 255
	write(t, source, "file", raw)
	next := commit(t, source)
	stats, err := Import(ctx, local, ImportOptions{Repo: source, DeltaDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	if stats.DeltaChunks != 1 {
		t.Fatal("legacy full anchor was not reused")
	}
	m, _, err = readHead(ctx, local)
	if err != nil || m.Version != legacyFormatVersion {
		t.Fatal("manifest was not upgraded", err)
	}
	current, err := r.Open(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	updated, err := current.Resolve(ctx, "file")
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(raw))
	if _, err := current.ReadAt(ctx, updated.OID, got, 0); err != nil || !bytes.Equal(got, raw) {
		t.Fatal("upgraded snapshot has wrong bytes", err)
	}
}
