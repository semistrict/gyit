package gitdelta_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"gyit/internal/gitdelta"
)

func TestFullChunkBoundaryAndBufferReuse(t *testing.T) {
	base := bytes.Repeat([]byte("01234567"), gitdelta.MaxSize/8)
	program, err := gitdelta.From(uint32(len(base)))
	if err != nil {
		t.Fatal("maximum supported base refused", err)
	}
	delta := binary.AppendUvarint(nil, uint64(len(base)))
	delta = binary.AppendUvarint(delta, uint64(len(base)))
	delta = append(delta, 0xc0, 0x10) // Copy 0x100000 bytes at offset zero.
	program, err = program.Compose(delta)
	if err != nil {
		t.Fatal("maximum supported target refused", err)
	}
	dst := make([]byte, 1, len(base))
	got, err := program.MaterializeInto(dst, base)
	if err != nil || !bytes.Equal(got, base) {
		t.Fatal("full chunk differs", err)
	}
	if &got[0] != &dst[0] {
		t.Fatal("sufficient caller capacity was not reused")
	}
	// The encoded-size limit applies to the recipe, not an existing prefix.
	wire, err := program.Encode()
	if err != nil {
		t.Fatal(err)
	}
	prefix := bytes.Repeat([]byte{0xa5}, 2*gitdelta.MaxSize)
	appended, err := program.AppendTo(bytes.Clone(prefix))
	if err != nil || !bytes.Equal(appended, append(prefix, wire...)) {
		t.Fatal("large destination prefix affected recipe encoding", err)
	}
	oversize := binary.AppendUvarint(nil, uint64(len(base)))
	oversize = binary.AppendUvarint(oversize, gitdelta.MaxSize+1)
	if _, err := program.Compose(oversize); !errors.Is(err, gitdelta.ErrLimit) {
		t.Fatal("oversized target must request bounded fallback", err)
	}
}

func TestEmptyBaseStillRequiresBothSizeHeaders(t *testing.T) {
	program, err := gitdelta.From(0)
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{nil, {0}, {0x80}, {0, 0x80}} {
		if _, err := program.Compose(input); err == nil {
			t.Fatalf("accepted truncated size headers: %x", input)
		}
	}
	if _, err := program.Compose([]byte{0, 0}); err != nil {
		t.Fatal("empty result with complete headers refused", err)
	}
}

func TestMaximumInstructionBufferCoalescesAdjacentCopies(t *testing.T) {
	base := bytes.Repeat([]byte("01234567"), gitdelta.MaxSize/8)
	program, err := gitdelta.From(uint32(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	const copies = gitdelta.MaxSize / 4
	delta := binary.AppendUvarint(nil, uint64(len(base)))
	delta = binary.AppendUvarint(delta, copies)
	delta = append(delta, 0x90, 1)
	for offset := 1; offset < copies; offset++ {
		// All seven optional fields are legal even when a byte is zero.
		delta = append(delta, 0xff, byte(offset), byte(offset>>8), byte(offset>>16), 0, 1, 0, 0)
	}
	if len(delta) != 2*gitdelta.MaxSize {
		t.Fatal("fixture is not at the instruction-buffer boundary")
	}
	next, err := program.Compose(delta)
	if err != nil {
		t.Fatal("legal maximum-sized instruction buffer refused", err)
	}
	got, err := next.Materialize(base)
	if err != nil || !bytes.Equal(got, base[:copies]) {
		t.Fatal("adjacent copies changed bytes", err)
	}
	// Hundreds of thousands of contiguous copies describe one range, so
	// retained memory must stay small rather than growing with instruction count.
	if next.MemoryBytes() > 1024 {
		t.Fatalf("coalesced range retained %d bytes", next.MemoryBytes())
	}
	if _, err := program.Compose(append(delta, 0)); !errors.Is(err, gitdelta.ErrLimit) {
		t.Fatal("oversized instruction buffer did not request fallback", err)
	}
}

func TestRejectOutputCounterWraparound(t *testing.T) {
	program, err := gitdelta.From(gitdelta.MaxSize)
	if err != nil {
		t.Fatal(err)
	}
	delta := binary.AppendUvarint(nil, gitdelta.MaxSize)
	delta = binary.AppendUvarint(delta, gitdelta.MaxSize)
	copyBytes := func(n uint32) {
		delta = append(delta, 0xf0, byte(n), byte(n>>8), byte(n>>16))
	}
	copyBytes(gitdelta.MaxSize)
	delta = append(delta, 1, 'x') // Already exceeds the declared target.
	// A tiny instruction stream can describe more than 4 GiB of output.
	// If the excess literal is accepted, these copies wrap a uint32 counter
	// back to the declared size. No payload allocation is needed to reject it.
	for range 4094 {
		copyBytes(gitdelta.MaxSize)
	}
	for n := uint32(1); n < gitdelta.MaxSize; n *= 2 {
		copyBytes(n)
	}
	copyBytes(gitdelta.MaxSize)
	if _, err := program.Compose(delta); err == nil || errors.Is(err, gitdelta.ErrLimit) {
		t.Fatal("output overflow must be rejected as malformed, not offered for fallback", err)
	}
}

func TestFragmentedRecipeOperationBudget(t *testing.T) {
	root, err := gitdelta.From(1)
	if err != nil {
		t.Fatal(err)
	}
	// The representation budget allows 131073 spans. Use independent,
	// nonadjacent one-byte copies, so no coalescing can hide excess work.
	const allowed = 131073
	for _, count := range []int{allowed, allowed + 1} {
		delta := binary.AppendUvarint(nil, 1)
		delta = binary.AppendUvarint(delta, uint64(count))
		for range count {
			delta = append(delta, 0x90, 1)
		}
		program, err := root.Compose(delta)
		if count > allowed {
			if !errors.Is(err, gitdelta.ErrLimit) {
				t.Fatal("fragmented recipe exceeded the operation budget", err)
			}
			continue
		}
		if err != nil {
			t.Fatal("fragmented recipe at the operation budget refused", err)
		}
		got, err := program.Materialize([]byte{'a'})
		if err != nil || !bytes.Equal(got, bytes.Repeat([]byte{'a'}, count)) {
			t.Fatal("fragmented recipe changed bytes", err)
		}
		// The next copy already declares more output than remains. Reject
		// that malformed instruction before composition hits its span budget;
		// ErrLimit would incorrectly tell the importer to try a fallback.
		invalid := binary.AppendUvarint(nil, allowed)
		invalid = binary.AppendUvarint(invalid, allowed+1)
		invalid = append(invalid, 0x90, 2, 0xf0, byte(allowed&255), byte((allowed>>8)&255), byte(allowed>>16))
		if _, err := program.Compose(invalid); err == nil || errors.Is(err, gitdelta.ErrLimit) {
			t.Fatal("oversized copy must be rejected before the composition budget", err)
		}
	}
}
