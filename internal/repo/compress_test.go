package repo

import (
	"bytes"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestCompressorIndependentFrames(t *testing.T) {
	c, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(8<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	random := make([]byte, ChunkSize)
	rand.New(rand.NewSource(27)).Read(random)
	var encoded []byte
	for _, raw := range [][]byte{nil, []byte("hello"), random, bytes.Repeat([]byte("source code\n"), 1000), []byte("last frame")} {
		encoded = c.EncodeAll(raw, encoded[:0])
		decoded, err := decoder.DecodeAll(encoded, nil)
		if err != nil || !bytes.Equal(raw, decoded) {
			t.Fatalf("independent frame did not round trip: %v", err)
		}
	}
}

// BenchmarkCompressorFixture samples real HEAD blobs from the existing ignored
// fixture, without cloning or fetching. Setup and Git reads are outside timing.
// Each input is at most one chunk; the corpus contains at most 33 chunks.
func BenchmarkCompressorFixture(b *testing.B) {
	source := filepath.Join("..", "..", ".testdata", "medium-repo.git")
	if _, err := os.Stat(source); os.IsNotExist(err) {
		b.Skip("requires cached medium repository")
	} else if err != nil {
		b.Fatal(err)
	}
	tree, err := git(b.Context(), source, "ls-tree", "-r", "--format=%(objecttype) %(objectname)", "HEAD").Output()
	if err != nil {
		b.Fatal(err)
	}
	var ids []string
	for _, line := range strings.Split(string(tree), "\n") {
		if strings.HasPrefix(line, "blob ") {
			ids = append(ids, strings.TrimPrefix(line, "blob "))
		}
	}
	var corpus [][]byte
	var rawBytes int64
	for i := 0; i < len(ids); i += max(1, (len(ids)+31)/32) {
		cmd := git(b.Context(), source, "cat-file", "blob", ids[i])
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			b.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			b.Fatal(err)
		}
		raw, readErr := io.ReadAll(io.LimitReader(pipe, ChunkSize))
		if len(raw) == ChunkSize || readErr != nil {
			_ = cmd.Process.Kill()
		}
		waitErr := cmd.Wait()
		if readErr != nil {
			b.Fatal(readErr)
		}
		if waitErr != nil && len(raw) < ChunkSize {
			b.Fatal(waitErr)
		}
		if len(raw) > 0 {
			corpus = append(corpus, raw)
			rawBytes += int64(len(raw))
		}
	}
	if rawBytes == 0 {
		b.Fatal("empty compression corpus")
	}
	c, err := newCompressor()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	var dst []byte
	var compressedBytes int64
	b.SetBytes(rawBytes)
	b.ReportAllocs()
	b.Logf("%s; chunks=%d raw_bytes=%d", compressorName, len(corpus), rawBytes)
	for b.Loop() {
		compressedBytes = 0
		for _, raw := range corpus {
			dst = c.EncodeAll(raw, dst[:0])
			compressedBytes += int64(len(dst))
		}
	}
	b.ReportMetric(float64(compressedBytes)/float64(rawBytes), "compressed/raw")
}
