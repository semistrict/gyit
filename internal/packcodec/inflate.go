//go:build cgo

package wirecodec

/*
#cgo LDFLAGS: -lz
#include <zlib.h>
static int inflate_frame(unsigned char *dst, unsigned long size,
                         const unsigned char *src, unsigned long length) {
 unsigned long actual = size, consumed = length;
 int result = uncompress2(dst, &actual, src, &consumed);
 if (result == Z_OK && (actual != size || consumed != length)) return Z_DATA_ERROR;
 return result;
}
*/
import "C"
import (
	"bytes"
	"fmt"
	"sync"
	"unsafe"
)

func Inflate(dst, src []byte) error {
	if len(src) == 0 {
		return fmt.Errorf("empty frame")
	}
	var empty byte
	p := &empty
	if len(dst) > 0 {
		p = &dst[0]
	}
	r := C.inflate_frame((*C.uchar)(unsafe.Pointer(p)), C.ulong(len(dst)), (*C.uchar)(unsafe.Pointer(&src[0])), C.ulong(len(src)))
	if r != C.Z_OK {
		return fmt.Errorf("inflate: %d", r)
	}
	return nil
}
func Deflate(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("empty input")
	}
	dst := make([]byte, int(C.compressBound(C.ulong(len(src)))))
	n := C.ulong(len(dst))
	r := C.compress2((*C.uchar)(unsafe.Pointer(&dst[0])), &n, (*C.uchar)(unsafe.Pointer(&src[0])), C.ulong(len(src)), 1)
	if r != C.Z_OK {
		return nil, fmt.Errorf("deflate: %d", r)
	}
	return bytes.Clone(dst[:int(n)]), nil
}
func InflatePrefix(dst, src []byte) (int, error) {
	if len(src) == 0 {
		return 0, fmt.Errorf("empty frame")
	}
	var empty byte
	p := &empty
	if len(dst) > 0 {
		p = &dst[0]
	}
	n := C.ulong(len(dst))
	used := C.ulong(len(src))
	r := C.uncompress2((*C.uchar)(unsafe.Pointer(p)), &n, (*C.uchar)(unsafe.Pointer(&src[0])), &used)
	if r != C.Z_OK || int(n) != len(dst) {
		return 0, fmt.Errorf("inflate prefix: %d", r)
	}
	return int(used), nil
}

var inflateScratch sync.Pool

func InflateBounded(src []byte, limit int) ([]byte, error) {
	if len(src) == 0 || limit < 1 || limit > 2<<20 {
		return nil, fmt.Errorf("invalid inflate bound")
	}
	v := inflateScratch.Get()
	var dst []byte
	if v == nil {
		dst = make([]byte, 2<<20)
	} else {
		dst = v.([]byte)
	}
	defer inflateScratch.Put(dst)
	n := C.ulong(limit)
	used := C.ulong(len(src))
	r := C.uncompress2((*C.uchar)(unsafe.Pointer(&dst[0])), &n, (*C.uchar)(unsafe.Pointer(&src[0])), &used)
	if r != C.Z_OK || int(used) != len(src) {
		return nil, fmt.Errorf("inflate bounded: %d", r)
	}
	return bytes.Clone(dst[:int(n)]), nil
}
