//go:build cgo

package packrecipe

/*
#cgo LDFLAGS: -lz
#include <zlib.h>
static int deferred_prefix(unsigned char *dst, unsigned long *produced,
                           const unsigned char *src, unsigned long length) {
  return uncompress2(dst, produced, src, &length);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

// This is the existing metadata-only prefix primitive. It never asks zlib for
// more than the two size varints and does not validate the remaining program.
func inflateDeferredPrefix(dst, src []byte, decodedSize int) (int, error) {
	if len(dst) == 0 || len(dst) > 20 || len(src) == 0 {
		return 0, fmt.Errorf("invalid deferred prefix bounds")
	}
	n := C.ulong(len(dst))
	status := C.deferred_prefix((*C.uchar)(unsafe.Pointer(&dst[0])), &n, (*C.uchar)(unsafe.Pointer(&src[0])), C.ulong(len(src)))
	if status == C.Z_OK && int(n) == decodedSize {
		return int(n), nil
	}
	if status == C.Z_BUF_ERROR && int(n) == len(dst) && decodedSize > len(dst) {
		return int(n), nil
	}
	return int(n), fmt.Errorf("invalid deferred delta prefix: %d", int(status))
}
