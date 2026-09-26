package gitdelta_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/gitdelta"
)

// This oracle executes instructions directly on bytes. It does not use the
// production span composition, coalescing, or native encoder.
func interpretDelta(base, delta []byte) ([]byte, error) {
	baseSize, n := binary.Uvarint(delta)
	if n <= 0 || baseSize != uint64(len(base)) {
		return nil, fmt.Errorf("bad base size")
	}
	delta = delta[n:]
	size, n := binary.Uvarint(delta)
	if n <= 0 || size > gitdelta.MaxSize {
		return nil, fmt.Errorf("bad target size")
	}
	delta = delta[n:]
	var out []byte
	for len(delta) > 0 {
		op := delta[0]
		delta = delta[1:]
		if op == 0 {
			return nil, fmt.Errorf("reserved instruction")
		}
		if op < 128 {
			if int(op) > len(delta) || uint64(len(out))+uint64(op) > size {
				return nil, fmt.Errorf("literal out of bounds")
			}
			out = append(out, delta[:op]...)
			delta = delta[op:]
			continue
		}
		var fields [7]uint64
		for i := range fields {
			if op&(1<<i) != 0 {
				if len(delta) == 0 {
					return nil, fmt.Errorf("missing copy argument")
				}
				fields[i], delta = uint64(delta[0]), delta[1:]
			}
		}
		offset := fields[0] + fields[1]*256 + fields[2]*65536 + fields[3]*16777216
		length := fields[4] + fields[5]*256 + fields[6]*65536
		if length == 0 {
			length = 65536
		}
		if offset+length > uint64(len(base)) || uint64(len(out))+length > size {
			return nil, fmt.Errorf("copy out of bounds")
		}
		out = append(out, base[offset:offset+length]...)
	}
	if uint64(len(out)) != size {
		return nil, fmt.Errorf("incomplete output")
	}
	return out, nil
}

func FuzzComposeAgainstByteInterpreter(f *testing.F) {
	f.Add([]byte("abcdefghij"), []byte{10, 9, 0x90, 3, 3, 'X', 'Y', 'Z', 0x91, 7, 3}, []byte{9, 8, 1, 'Q', 0x91, 1, 7})
	f.Add([]byte{}, []byte{0, 0}, []byte{0, 1, 1, 'x'})
	f.Add([]byte("abc"), []byte{3, 1, 0x91, 2, 1}, []byte{1, 0})
	f.Add(bytes.Repeat([]byte("x"), 65536), []byte{0x80, 0x80, 4, 0x80, 0x80, 4, 0x80}, []byte{0x80, 0x80, 4, 1, 0x91, 0xff, 1})
	f.Add([]byte("x"), []byte{1, 1, 0}, []byte{})
	// Nonadjacent copies must stay separate; the next copy starts exactly
	// at a composed span boundary. Also exercise a copy after a literal.
	f.Add([]byte("abcdefghij"), []byte{10, 4, 0x91, 2, 2, 0x90, 2}, []byte{4, 2, 0x91, 2, 2})
	f.Add([]byte("abcdefghij"), []byte{10, 4, 1, 'Q', 0x91, 2, 2, 0x91, 6, 1}, []byte{4, 1, 0x91, 3, 1})
	f.Fuzz(func(t *testing.T, base, first, second []byte) {
		// Stay below the separate operation-count limit so validity can be
		// compared exactly. These bounds also keep hostile inputs inexpensive.
		if len(base) > 65536 || len(first) > 4096 || len(second) > 4096 {
			t.Skip()
		}
		program, err := gitdelta.From(uint32(len(base)))
		if err != nil {
			t.Fatal(err)
		}
		model := bytes.Clone(base)
		for _, input := range [][]byte{first, second} {
			want, oracleErr := interpretDelta(model, input)
			if len(want) > 65536 {
				t.Skip() // Keep valid compositions below the span-count limit.
			}
			owned := bytes.Clone(input)
			next, err := program.Compose(owned)
			if (err == nil) != (oracleErr == nil) {
				t.Fatalf("validity mismatch for %x: compose=%v oracle=%v", input, err, oracleErr)
			}
			if oracleErr != nil {
				return
			}
			clear(owned) // Recipes must own literals, including after composition.
			got, err := next.Materialize(base)
			if err != nil || !bytes.Equal(got, want) || next.Size() != uint32(len(want)) {
				t.Fatalf("materialized bytes differ: got=%x want=%x err=%v", got, want, err)
			}
			native, err := interpretDelta(base, next.AppendNative(nil))
			if err != nil || !bytes.Equal(native, want) {
				t.Fatal("native encoding differs from byte model", err)
			}
			old, err := program.Materialize(base)
			if err != nil || !bytes.Equal(old, model) {
				t.Fatal("composition changed its parent", err)
			}
			if len(want) != 0 {
				wire, err := next.Encode()
				if err != nil {
					t.Fatal(err)
				}
				var recipe storagev1.ChunkDelta
				if err := proto.Unmarshal(wire, &recipe); err != nil {
					t.Fatal(err)
				}
				var decoded []byte
				for _, op := range recipe.Operations {
					if len(op.Literal) > 0 {
						decoded = append(decoded, op.Literal...)
					} else {
						if uint64(op.Offset)+uint64(op.Length) > uint64(len(base)) {
							t.Fatal("wire copy exceeds root")
						}
						decoded = append(decoded, base[op.Offset:op.Offset+op.Length]...)
					}
				}
				if recipe.Size != uint32(len(want)) || !bytes.Equal(decoded, want) {
					t.Fatal("wire recipe differs from byte model")
				}
			}
			program, model = next, want
		}
	})
}

// Raw-byte fuzzing mostly encounters invalid headers. Generate valid edit
// chains too, so mutations continually reach copies across composed spans.
func FuzzComposeEditSequence(f *testing.F) {
	f.Add([]byte("abcdefghij"), []byte{2, 3, 'X', 0, 0, 'Q', 4, 1, 0})
	f.Add([]byte{}, []byte{0, 0, 'a', 0, 0, 'b', 0, 1, 'c'})
	f.Fuzz(func(t *testing.T, base, edits []byte) {
		if len(base) > 4096 || len(edits) > 192 {
			t.Skip()
		}
		program, err := gitdelta.From(uint32(len(base)))
		if err != nil {
			t.Fatal(err)
		}
		model := bytes.Clone(base)
		for i := 0; i+2 < len(edits); i += 3 {
			offset := int(edits[i]) % (len(model) + 1)
			remove := int(edits[i+1]) % (len(model) - offset + 1)
			want := append(bytes.Clone(model[:offset]), edits[i+2])
			want = append(want, model[offset+remove:]...)
			delta := binary.AppendUvarint(nil, uint64(len(model)))
			delta = binary.AppendUvarint(delta, uint64(len(want)))
			copyRange := func(start, length int) {
				if length > 0 {
					// Explicit little-endian offset and size fields; no
					// production encoder participates in the expectation.
					delta = append(delta, 0xb3, byte(start), byte(start>>8), byte(length), byte(length>>8))
				}
			}
			copyRange(0, offset)
			delta = append(delta, 1, edits[i+2])
			copyRange(offset+remove, len(model)-offset-remove)
			next, err := program.Compose(delta)
			if err != nil {
				t.Fatalf("edit %d refused: %v", i/3, err)
			}
			got, err := next.Materialize(base)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("edit %d: got=%x want=%x err=%v", i/3, got, want, err)
			}
			program, model = next, want
		}
	})
}
