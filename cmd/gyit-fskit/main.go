//go:build darwin && cgo

// The native extension links this command as a C archive.
package main

/*
#include <stdint.h>
#include <stdlib.h>
typedef struct {
 uint64_t inode;
 int64_t size;
 uint32_t mode;
 char *name;
} GyitEntry;
*/
import "C"
import (
	"context"
	"errors"
	"gyit/internal/githubfs"
	"gyit/internal/macfs"
	"gyit/internal/repo"
	"io"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

type session struct{ s githubfs.Namespace }

var sessions = struct {
	sync.RWMutex
	next  uint64
	items map[uint64]*session
}{items: make(map[uint64]*session)}

func errorCode(err error) C.int {
	if err == nil || err == io.EOF {
		return 0
	}
	if repo.IsNotFound(err) {
		return C.int(syscall.ENOENT)
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return C.int(errno)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return C.int(syscall.ETIMEDOUT)
	}
	return C.int(syscall.EIO)
}
func contextForCall() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

//export GyitOpen
func GyitOpen(data, cache, remote, token *C.char, budget C.int64_t, out *C.uint64_t) C.int {
	if data == nil || cache == nil || remote == nil || token == nil || out == nil || budget < 0 {
		return C.int(syscall.EINVAL)
	}
	if err := configureGit(); err != nil {
		return errorCode(err)
	}
	f, err := githubfs.New(githubfs.Options{DataDir: C.GoString(data), CacheDir: C.GoString(cache), RemoteBase: C.GoString(remote), Token: C.GoString(token), CacheBytes: int64(budget)})
	if err != nil {
		return errorCode(err)
	}
	sessions.Lock()
	defer sessions.Unlock()
	sessions.next++
	sessions.items[sessions.next] = &session{s: githubfs.Namespace{FS: f}}
	*out = C.uint64_t(sessions.next)
	return 0
}

//export GyitClose
func GyitClose(id C.uint64_t) {
	sessions.Lock()
	defer sessions.Unlock()
	if s := sessions.items[uint64(id)]; s != nil {
		s.s.Close()
		delete(sessions.items, uint64(id))
	}
}

//export GyitGeneration
func GyitGeneration(id C.uint64_t, path *C.char) C.uint64_t {
	sessions.RLock()
	defer sessions.RUnlock()
	if path == nil {
		return 0
	}
	if s := sessions.items[uint64(id)]; s != nil {
		return C.uint64_t(s.s.Generation(C.GoString(path)))
	}
	return 0
}

//export GyitLookup
func GyitLookup(id C.uint64_t, path *C.char, out *C.GyitEntry) C.int {
	if path == nil || out == nil {
		return C.int(syscall.EINVAL)
	}
	sessions.RLock()
	defer sessions.RUnlock()
	s := sessions.items[uint64(id)]
	if s == nil {
		return C.int(syscall.EBADF)
	}
	ctx, cancel := contextForCall()
	defer cancel()
	p := C.GoString(path)
	e, err := s.s.Lookup(ctx, p)
	if err != nil {
		return errorCode(err)
	}
	*out = C.GyitEntry{inode: C.uint64_t(macfs.Inode(p)), size: C.int64_t(e.Size), mode: C.uint32_t(e.Mode)}
	return 0
}

//export GyitInode
func GyitInode(path *C.char) C.uint64_t {
	return C.uint64_t(macfs.Inode(C.GoString(path)))
}

//export GyitList
func GyitList(id C.uint64_t, path, after *C.char, out **C.GyitEntry, count *C.int) C.int {
	if path == nil || after == nil || out == nil || count == nil {
		return C.int(syscall.EINVAL)
	}
	*out = nil
	*count = 0
	sessions.RLock()
	defer sessions.RUnlock()
	s := sessions.items[uint64(id)]
	if s == nil {
		return C.int(syscall.EBADF)
	}
	ctx, cancel := contextForCall()
	defer cancel()
	p := C.GoString(path)
	entries, err := s.s.ReadDir(ctx, p, C.GoString(after), 128)
	if err != nil {
		return errorCode(err)
	}
	if len(entries) == 0 {
		return 0
	}
	ptr := C.calloc(C.size_t(len(entries)), C.size_t(unsafe.Sizeof(C.GyitEntry{})))
	if ptr == nil {
		return C.int(syscall.ENOMEM)
	}
	array := unsafe.Slice((*C.GyitEntry)(ptr), len(entries))
	for i, e := range entries {
		child := e.Name
		if p != "" {
			child = p + "/" + child
		}
		array[i] = C.GyitEntry{inode: C.uint64_t(macfs.Inode(child)), size: C.int64_t(e.Size), mode: C.uint32_t(e.Mode), name: C.CString(e.Name)}
	}
	*out = (*C.GyitEntry)(ptr)
	*count = C.int(len(entries))
	return 0
}

//export GyitFreeEntries
func GyitFreeEntries(entries *C.GyitEntry, count C.int) {
	if entries == nil || count < 0 || count > 128 {
		return
	}
	for _, e := range unsafe.Slice(entries, int(count)) {
		C.free(unsafe.Pointer(e.name))
	}
	C.free(unsafe.Pointer(entries))
}

//export GyitRead
func GyitRead(id C.uint64_t, path *C.char, offset C.int64_t, buffer unsafe.Pointer, length C.int, read *C.int) C.int {
	if path == nil || read == nil || length < 0 || length > 8<<20 || (length > 0 && buffer == nil) {
		return C.int(syscall.EINVAL)
	}
	*read = 0
	sessions.RLock()
	defer sessions.RUnlock()
	s := sessions.items[uint64(id)]
	if s == nil {
		return C.int(syscall.EBADF)
	}
	ctx, cancel := contextForCall()
	defer cancel()
	n, err := s.s.Read(ctx, C.GoString(path), unsafe.Slice((*byte)(buffer), int(length)), int64(offset))
	*read = C.int(n)
	return errorCode(err)
}
func main() {}

//export GyitRetry
func GyitRetry(id C.uint64_t, path *C.char) C.int {
	if path == nil {
		return C.int(syscall.EINVAL)
	}
	sessions.RLock()
	defer sessions.RUnlock()
	s := sessions.items[uint64(id)]
	if s == nil {
		return C.int(syscall.EBADF)
	}
	return errorCode(s.s.Retry(C.GoString(path)))
}

//export GyitEndpoint
func GyitEndpoint(id C.uint64_t, path *C.char, out **C.char, count *C.int) C.int {
	if path == nil || out == nil || count == nil {
		return C.int(syscall.EINVAL)
	}
	sessions.RLock()
	defer sessions.RUnlock()
	s := sessions.items[uint64(id)]
	if s == nil {
		return C.int(syscall.EBADF)
	}
	ctx, cancel := contextForCall()
	defer cancel()
	b, err := s.s.Endpoint(ctx, C.GoString(path))
	if err != nil {
		return errorCode(err)
	}
	if b == nil {
		return C.int(syscall.ENOATTR)
	}
	*out = (*C.char)(C.CBytes(b))
	*count = C.int(len(b))
	return 0
}
