package gitdelta_test

import (
	"bytes"
	"testing"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/gitdelta"
	"google.golang.org/protobuf/proto"
)

func TestCompositionRetainsOneFullBase(t *testing.T) {
	base := []byte("abcdefghij")
	p, err := gitdelta.From(uint32(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	// abcXYZhij, then insert Q before that recipe and copy across its literal.
	first := []byte{10, 9, 0x90, 3, 3, 'X', 'Y', 'Z', 0x91, 7, 3}
	p, err = p.Compose(first)
	if err != nil {
		t.Fatal(err)
	}
	first[5] = '!' // The caller may reuse its source instruction buffer.
	p, err = p.Compose([]byte{9, 8, 1, 'Q', 0x91, 1, 7})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Materialize(base)
	if err != nil || string(got) != "QbcXYZhi" {
		t.Fatalf("got %q: %v", got, err)
	}
	wire, err := p.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var message storagev1.ChunkDelta
	if err := proto.Unmarshal(wire, &message); err != nil {
		t.Fatal(err)
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&message)
	if err != nil || !bytes.Equal(canonical, wire) {
		t.Fatal("wire differs from canonical generated protobuf")
	}
	prefixed, err := p.AppendTo([]byte("prefix"))
	if err != nil || !bytes.Equal(prefixed, append([]byte("prefix"), wire...)) {
		t.Fatal("append damaged prefix or encoded bytes")
	}
	reused, err := p.MaterializeInto(make([]byte, 1, 20), base)
	if err != nil || !bytes.Equal(reused, got) {
		t.Fatal("reused output differs")
	}
	// Independently interpret the published protobuf against the original base.
	var reconstructed []byte
	for _, op := range message.Operations {
		if len(op.Literal) > 0 {
			reconstructed = append(reconstructed, op.Literal...)
		} else {
			reconstructed = append(reconstructed, base[op.Offset:op.Offset+op.Length]...)
		}
	}
	if message.Size != 8 || !bytes.Equal(got, reconstructed) {
		t.Fatal("wire recipe differs")
	}
}

func TestRejectMalformedAndOutOfBoundsPrograms(t *testing.T) {
	p, err := gitdelta.From(10)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{
		nil, {0x80}, {11, 1, 1, 'x'}, {10, 1, 0}, {10, 1, 0x91, 10, 1},
		{10, 1, 2, 'x'}, {10, 2, 1, 'x'}, {10, 1, 1, 'x', 1, 'y'},
		{10, 1, 0x90}, {10, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 2},
	} {
		if _, err := p.Compose(input); err == nil {
			t.Fatalf("accepted invalid instructions %x", input)
		}
	}
	if _, err := gitdelta.From(gitdelta.MaxSize + 1); err == nil {
		t.Fatal("accepted oversized base")
	}
	if _, err := p.Materialize([]byte("short")); err == nil {
		t.Fatal("accepted wrong base length")
	}
}

func TestDefaultCopyLengthAndImmutableSiblingPrograms(t *testing.T) {
	base := bytes.Repeat([]byte("a"), 65536)
	p, err := gitdelta.From(uint32(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	p, err = p.Compose([]byte{0x80, 0x80, 4, 0x80, 0x80, 4, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := p.Materialize(base); err != nil || !bytes.Equal(got, base) {
		t.Fatalf("default copy: %v", err)
	}
	empty, _ := gitdelta.From(0)
	original, err := empty.Compose([]byte{0, 6, 6, 'a', 'b', 'c', 'd', 'e', 'f'})
	if err != nil {
		t.Fatal(err)
	}
	left, err := original.Compose([]byte{6, 5, 0x91, 1, 3, 2, 'X', 'Y'})
	if err != nil {
		t.Fatal(err)
	}
	right, err := original.Compose([]byte{6, 3, 0x91, 3, 3})
	if err != nil {
		t.Fatal(err)
	}
	for p, want := range map[*gitdelta.Program]string{original: "abcdef", left: "bcdXY", right: "def"} {
		got, err := p.Materialize(nil)
		if err != nil || string(got) != want {
			t.Fatalf("got %q want %q: %v", got, want, err)
		}
	}
}
