package gitdelta_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"testing"
	"time"

	"gyit/internal/gitdelta"
)

func TestNativeEncodingAgainstByteInterpreter(t *testing.T) {
	// Supervise this diagnostic's liveness separately from the outer suite.
	// A non-advancing literal loop must fail the test and leave no busy child.
	if os.Getenv("GYIT_NATIVE_ENCODING_CHILD") != "1" {
		executable, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestNativeEncodingAgainstByteInterpreter$", "-test.count=1")
		cmd.Env = append(os.Environ(), "GYIT_NATIVE_ENCODING_CHILD=1")
		output, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("native encoding failed to terminate within its deadline")
		}
		if err != nil {
			t.Fatalf("native encoding child failed: %v\n%s", err, output)
		}
		return
	}
	base := make([]byte, gitdelta.MaxSize)
	for i := range base {
		base[i] = byte(i*31 + i/256)
	}
	root, err := gitdelta.From(uint32(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name           string
		offset, length int
		literal        []byte
	}{
		{"empty", 0, 0, nil},
		{"one-byte-copy", 1, 1, nil},
		{"multi-byte-offset", 0x10203, 0x20304, nil},
		{"default-length", 0, 65536, nil},
		{"large-literal", 513, 256, bytes.Repeat([]byte("literal"), 60)},
		{"literal-boundary", 0, 0, bytes.Repeat([]byte{0xff}, 127)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			delta := binary.AppendUvarint(nil, uint64(len(base)))
			delta = binary.AppendUvarint(delta, uint64(tc.length+len(tc.literal)))
			if tc.length > 0 {
				delta = append(delta, 0xff, byte(tc.offset), byte(tc.offset>>8), byte(tc.offset>>16), byte(tc.offset>>24), byte(tc.length), byte(tc.length>>8), byte(tc.length>>16))
			}
			for remaining := tc.literal; len(remaining) > 0; {
				n := min(127, len(remaining))
				delta = append(delta, byte(n))
				delta = append(delta, remaining[:n]...)
				remaining = remaining[n:]
			}
			program, err := root.Compose(delta)
			if err != nil {
				t.Fatal(err)
			}
			prefix := []byte("preserve-prefix")
			encoded := program.AppendNative(bytes.Clone(prefix))
			if !bytes.HasPrefix(encoded, prefix) {
				t.Fatal("native encoder changed prefix")
			}
			got, err := interpretDelta(base, encoded[len(prefix):])
			want := append(bytes.Clone(base[tc.offset:tc.offset+tc.length]), tc.literal...)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("native encoding differs: err=%v got bytes=%d want bytes=%d", err, len(got), len(want))
			}
			if tc.name == "default-length" {
				minimal := binary.AppendUvarint(nil, uint64(len(base)))
				minimal = binary.AppendUvarint(minimal, 65536)
				minimal = append(minimal, 0x80)
				if !bytes.Equal(encoded[len(prefix):], minimal) {
					t.Fatal("default copy length was not encoded implicitly")
				}
			}
		})
	}
}

func TestMemoryAccountingTracksRetainedData(t *testing.T) {
	empty, err := gitdelta.From(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := gitdelta.From(gitdelta.MaxSize)
	if err != nil {
		t.Fatal(err)
	}
	if empty.MemoryBytes() <= 0 || empty.MemoryBytes() >= root.MemoryBytes() {
		t.Fatal("empty recipe must account for itself without retaining a copy span")
	}
	// A recipe refers to the full base, but does not own its megabyte of data.
	if root.MemoryBytes() > 1024 {
		t.Fatal("root recipe charged for caller-owned base storage")
	}
	literal := bytes.Repeat([]byte{'x'}, 8192)
	delta := binary.AppendUvarint(nil, 0)
	delta = binary.AppendUvarint(delta, uint64(len(literal)))
	for rest := literal; len(rest) > 0; {
		n := min(127, len(rest))
		delta = append(delta, byte(n))
		delta = append(delta, rest[:n]...)
		rest = rest[n:]
	}
	owned, err := empty.Compose(delta)
	if err != nil {
		t.Fatal(err)
	}
	if owned.MemoryBytes() < len(literal)+empty.MemoryBytes() || owned.MemoryBytes() > 4*len(literal) {
		t.Fatalf("owned literal accounting is outside retained-storage bounds: %d", owned.MemoryBytes())
	}
}
