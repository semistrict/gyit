package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gyit/internal/store"
)

func deferredCapture(t *testing.T) func() map[string]int64 {
	t.Helper()
	var mu sync.Mutex
	counts, seen := map[string]int64{}, map[string]int{}
	deferredTraceHook = func(k string, n int64) { mu.Lock(); counts[k] = n; seen[k]++; mu.Unlock() }
	t.Cleanup(func() { deferredTraceHook = nil })
	return func() map[string]int64 {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]int64{}
		for k, v := range counts {
			if seen[k] != 1 {
				t.Fatalf("duplicate trace %s", k)
			}
			out[k] = v
		}
		return out
	}
}

func deferredFixture(t *testing.T) (string, string, string) {
	t.Helper()
	source := t.TempDir()
	command(t, source, "init", "-q")
	var body bytes.Buffer
	for i := 0; i < 2048; i++ {
		fmt.Fprintf(&body, "line %08d %x\n", i, sha256.Sum256([]byte(fmt.Sprint(i))))
	}
	var tip string
	for n := 0; n < 6; n++ {
		changed := bytes.Clone(body.Bytes())
		copy(changed[500+n*120:500+n*120+90], bytes.Repeat([]byte{byte('A' + n)}, 90))
		write(t, source, "fast.txt", changed)
		write(t, source, "small", []byte(fmt.Sprintf("small %d\n", n)))
		if n == 0 {
			write(t, source, "empty", nil)
			write(t, source, "large", bytes.Repeat([]byte("bounded fallback body\n"), 50000))
			write(t, source, "dir/child", []byte("nested\n"))
		}
		tip = commit(t, source)
	}
	command(t, source, "-c", "pack.writeReverseIndex=true", "repack", "-adf", "--window=50", "--depth=20")
	packs, e := filepath.Glob(filepath.Join(source, ".git", "objects", "pack", "*.pack"))
	if e != nil || len(packs) != 1 {
		t.Fatal("single fixture pack required")
	}
	prefix := strings.TrimSuffix(packs[0], ".pack")
	if _, e = os.Stat(prefix + ".rev"); e != nil {
		t.Fatal("reverse index required", e)
	}
	return source, tip, prefix
}

func deferredProof(t *testing.T, c map[string]int64) {
	t.Helper()
	required := []string{"conversion_fast_objects", "conversion_fast_raw_bytes", "conversion_legacy_objects", "conversion_eager_objects", "conversion_full_inflated_bytes", "conversion_reconstructed_bytes", "conversion_fast_full_inflations", "conversion_fast_full_inflated_bytes", "conversion_fast_reconstructions", "conversion_fast_reconstructed_bytes", "conversion_fast_get_calls"}
	for _, k := range required {
		if _, ok := c[k]; !ok {
			t.Fatalf("missing counter %s", k)
		}
	}
	for _, k := range []string{"conversion_fast_full_inflations", "conversion_fast_full_inflated_bytes", "conversion_fast_reconstructions", "conversion_fast_reconstructed_bytes", "conversion_fast_get_calls"} {
		if c[k] != 0 {
			t.Fatalf("fast path expanded source bodies: %s=%d", k, c[k])
		}
	}
	if c["archive_blob_admitted"] <= 0 || c["archive_blob_raw_bytes"] <= 0 || c["archive_source_bytes"] <= 0 {
		t.Fatal("no blobs retained in the source archive")
	}
	if c["archive_blob_admitted"] != c["ordered_direct_rows"] {
		t.Fatal("archive blob population differs from the published direct index")
	}
	if c["conversion_fast_objects"]+c["conversion_legacy_objects"] != c["native_workers_exited"] || c["conversion_eager_objects"] != 0 {
		t.Fatal("native conversion population incomplete or unexpectedly eager")
	}
}

func TestDeferredAuthImportReadback(t *testing.T) {
	source, tip, _ := deferredFixture(t)
	for _, mode := range []string{"archive", "reachable"} {
		t.Run(mode, func(t *testing.T) {
			counts := deferredCapture(t)
			local, e := store.NewLocal(t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			scratch := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			options := ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 8}
			if mode == "reachable" {
				options.DeltaDepth = 1
			}
			stats, e := importWithMetadataThreshold(ctx, local, options, 0)
			if e != nil {
				t.Fatal(e)
			}
			if stats.Phase != "done" || stats.ImportMode != mode {
				t.Fatalf("publication used the wrong mode or was incomplete: %+v", stats)
			}
			if mode == "archive" {
				deferredProof(t, counts())
			} else if c := counts(); c["archive_blob_admitted"] != 0 || c["fallback_raw_bytes"] <= 0 {
				t.Fatal("explicit chunk conversion did not read the source bodies")
			}
			deferredCheckReadback(t, local, source, tip, []string{"fast.txt", "small", "large", "empty", "dir/child"})
			m, _, e := readHead(t.Context(), local)
			if e != nil {
				t.Fatal(e)
			}
			ceilingCheckRetained(t, local, m, source, tip, []string{"fast.txt", "large"})
			entries, e := os.ReadDir(scratch)
			if e != nil || len(entries) != 0 {
				t.Fatal("scratch retained", e)
			}
		})
	}
}

func TestDeferredAuthCorruptBlobFailsOnRead(t *testing.T) {
	source, tip, pack := deferredFixture(t)
	var oid string
	var offset, packedSize int64
	for _, line := range strings.Split(command(t, source, "verify-pack", "-v", pack+".idx"), "\n") {
		f := strings.Fields(line)
		if len(f) != 5 || f[1] != "blob" {
			continue
		}
		var size int64
		if _, e := fmt.Sscan(f[2], &size); e != nil {
			t.Fatal(e)
		}
		if size > 4096 && size < ChunkSize {
			oid = f[0]
			if _, e := fmt.Sscan(f[3], &packedSize); e != nil {
				t.Fatal(e)
			}
			if _, e := fmt.Sscan(f[4], &offset); e != nil {
				t.Fatal(e)
			}
			break
		}
	}
	if oid == "" || packedSize < 16 {
		t.Fatal("full native blob fixture required")
	}
	if e := os.Chmod(pack+".pack", 0600); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(pack+".pack", os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	// Damage the compressed body, preserving index/layout and source headers.
	at := offset + packedSize/2
	var b [1]byte
	if _, e = f.ReadAt(b[:], at); e != nil {
		f.Close()
		t.Fatal(e)
	}
	b[0] ^= 128
	if _, e = f.WriteAt(b[:], at); e != nil {
		f.Close()
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	counts := deferredCapture(t)
	local, e := store.NewLocal(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	scratch := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	stats, e := importWithMetadataThreshold(ctx, local, ImportOptions{Repo: source, TempDir: scratch, CompressionWorkers: 8}, 0)
	if e != nil || stats.Phase != "done" || stats.ImportMode != "archive" {
		t.Fatalf("deferred source verification was not deferred: %v", e)
	}
	deferredProof(t, counts())
	r, e := New(local, DefaultCacheBytes)
	if e != nil {
		t.Fatal(e)
	}
	s, e := r.Open(ctx, tip)
	if e != nil {
		t.Fatal(e)
	}
	var c chunk
	if _, c, e = s.readBlobPart(ctx, oid, 0); e != nil || c.Hash != "git-sha1:"+oid {
		t.Fatalf("corrupt fixture did not use deferred representation: %v", e)
	}
	dst := bytes.Repeat([]byte{0xa5}, 128)
	for i := 0; i < 2; i++ {
		n, e := s.ReadAt(ctx, oid, dst, 0)
		if n != 0 || e == nil || e == io.EOF || !bytes.Equal(dst, bytes.Repeat([]byte{0xa5}, 128)) {
			t.Fatalf("corrupt data reached caller or cache: n=%d error=%v", n, e)
		}
	}
}
