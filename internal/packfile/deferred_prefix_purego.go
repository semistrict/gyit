//go:build !cgo

package packrecipe

import (
	"fmt"

	wirecodec "gat/internal/packcodec"
)

func inflateDeferredPrefix(dst, src []byte, decodedSize int) (int, error) {
	if decodedSize < 1 {
		return 0, fmt.Errorf("invalid deferred prefix bounds")
	}
	n, _, err := wirecodec.InflateMetadataPrefix(dst, src[:min(len(src), 64<<10)], uint64(decodedSize))
	return n, err
}
