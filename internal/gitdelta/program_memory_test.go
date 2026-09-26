package gitdelta

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestMemoryBytesCoversOwnedAllocations(t *testing.T) {
	empty, err := From(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := From(MaxSize)
	if err != nil {
		t.Fatal(err)
	}
	input := binary.AppendUvarint(nil, 0)
	input = binary.AppendUvarint(input, 4096)
	for range 4096 {
		input = append(input, 1, 'x')
	}
	owned, err := empty.Compose(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, program := range []*Program{empty, root, owned} {
		// Independently measure the Go layout and backing capacities. Import
		// admission must account for retained capacity, not just visible length.
		minimum := int(reflect.TypeOf(*program).Size())
		minimum += cap(program.spans) * int(reflect.TypeOf(span{}).Size())
		for _, part := range program.spans {
			minimum += cap(part.literal)
		}
		if got := program.MemoryBytes(); got < minimum {
			t.Fatalf("retained storage undercounted: got %d need at least %d", got, minimum)
		}
	}
}
