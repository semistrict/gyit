//go:build !cgo

package planner

import (
	"fmt"

	wirecodec "gyit/internal/packcodec"
)

const PrefixInputLimit = 64 << 10
const PrefixOutputLimit = 20

func metadataPrefix(dst, src []byte, rawSize uint64) (int, uint64, error) {
	if len(dst) != PrefixOutputLimit || len(src) == 0 || rawSize < 2 {
		return 0, 0, fmt.Errorf("%w: missing delta prefix", ErrMalformed)
	}
	want := min(rawSize, PrefixOutputLimit)
	n, consumed, err := wirecodec.InflateMetadataPrefix(dst[:want], src[:min(len(src), PrefixInputLimit)], rawSize)
	if err == nil {
		return n, consumed, nil
	}
	if len(src) > PrefixInputLimit && consumed == PrefixInputLimit && uint64(n) < want {
		return n, consumed, fmt.Errorf("%w: delta prefix compressed input limit", ErrUnsupported)
	}
	return n, consumed, fmt.Errorf("%w: delta prefix: %v", ErrMalformed, err)
}
