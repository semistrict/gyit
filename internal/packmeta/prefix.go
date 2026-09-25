//go:build cgo

package planner

/*
#cgo LDFLAGS: -lz
#include <zlib.h>
typedef struct { int status; unsigned long produced, consumed; } prefix_result;
static prefix_result source_prefix(unsigned char *dst, unsigned long wanted,
                                    const unsigned char *src, unsigned long length) {
  prefix_result r;
  r.produced = wanted;
  r.consumed = length;
  r.status = uncompress2(dst, &r.produced, src, &r.consumed);
  return r;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const PrefixInputLimit = 64 << 10
const PrefixOutputLimit = 20

// Only prefix metadata is decoded. The compressed input and decoded output of
// each C call are independently bounded; the rest of the body is not validated.
func metadataPrefix(dst, src []byte, rawSize uint64) (int, uint64, error) {
	if len(dst) != PrefixOutputLimit || len(src) == 0 || rawSize < 2 {
		return 0, 0, fmt.Errorf("%w: missing delta prefix", ErrMalformed)
	}
	want := min(rawSize, PrefixOutputLimit)
	r := C.source_prefix((*C.uchar)(unsafe.Pointer(&dst[0])), C.ulong(want), (*C.uchar)(unsafe.Pointer(&src[0])), C.ulong(min(len(src), PrefixInputLimit)))
	n, consumed, status := r.produced, r.consumed, r.status
	if status == C.Z_OK && uint64(n) == rawSize || status == C.Z_BUF_ERROR && uint64(n) == want && rawSize > want {
		return int(n), uint64(consumed), nil
	}
	// uncompress2 can translate input exhaustion into Z_DATA_ERROR when its
	// output buffer still has space. At the imposed input boundary we cannot
	// distinguish that truncation from later source corruption without reading
	// beyond the budget; the complete metadata fast path must decline either.
	if len(src) > PrefixInputLimit && consumed == PrefixInputLimit && uint64(n) < want && (status == C.Z_BUF_ERROR || status == C.Z_DATA_ERROR || status == C.Z_OK) {
		return int(n), uint64(consumed), fmt.Errorf("%w: delta prefix compressed input limit", ErrUnsupported)
	}
	return int(n), uint64(consumed), fmt.Errorf("%w: delta prefix status %d output %d", ErrMalformed, int(status), uint64(n))
}
