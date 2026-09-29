package repo

import (
	"bytes"
	"testing"
)

func TestDeltaReservedPrefix(t *testing.T) {
	for _, test := range []struct {
		name                string
		base, program, want []byte
		valid               bool
	}{
		{"empty", nil, []byte{0, 0}, nil, true},
		{"literal", []byte("abc"), []byte{3, 3, 3, 'x', 'y', 'z'}, []byte("xyz"), true},
		{"copy", []byte("abc"), []byte{3, 2, 0x91, 1, 2}, []byte("bc"), true},
		{"literal-overrun", nil, []byte{0, 2, 3, 'x', 'y', 'z'}, nil, false},
		{"copy-overrun", []byte("abc"), []byte{3, 2, 0x90, 3}, nil, false},
		{"short-result", nil, []byte{0, 3, 2, 'x', 'y'}, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, prefix := range []int{0, 1, 16} {
				got, err := applyLimitPrefix(test.base, test.program, 64, prefix)
				if !test.valid {
					if err == nil {
						t.Fatal("reserved space masked an invalid delta length")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got[:prefix], make([]byte, prefix)) || !bytes.Equal(got[prefix:], test.want) {
					t.Fatalf("prefix=%d: got %x", prefix, got)
				}
			}
		})
	}
	for _, prefix := range []int{-1, int(^uint(0) >> 1)} {
		if _, err := applyLimitPrefix(nil, []byte{0, 1, 1, 'x'}, 64, prefix); err == nil {
			t.Fatal("accepted overflowing allocation")
		}
	}
}
