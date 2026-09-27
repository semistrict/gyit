//go:build darwin && cgo && gyit_virtiofs

// This C archive is linked into the direct Virtio benchmark VM.
package main

/*
#include <stdint.h>
*/
import "C"

import (
	"fmt"
	"os"
	"runtime/pprof"
	"sync"
	"time"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"gyit/internal/githubfs"
	"gyit/internal/githubmount"
)

var profile *os.File

//export GyitVirtioFinishProfile
func GyitVirtioFinishProfile() {
	if profile != nil {
		pprof.StopCPUProfile()
		profile.Close()
		profile = nil
	}
}

type backend struct {
	protocol *fuse.ProtocolServer
	source   *githubfs.FS
}

var backends = struct {
	sync.Mutex
	next uint64
	m    map[uint64]*backend
}{m: make(map[uint64]*backend)}

//export GyitVirtioOpen
func GyitVirtioOpen(data, cache, remote, checkout *C.char, ttl C.int) C.uint64_t {
	b := &backend{}
	timeout := time.Duration(ttl) * time.Second
	if C.GoString(checkout) != "" {
		root, err := fs.NewLoopbackRoot(C.GoString(checkout))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 0
		}
		opts := &fs.Options{EntryTimeout: &timeout, AttrTimeout: &timeout, NegativeTimeout: &timeout}
		b.protocol = fuse.NewProtocolServer(fs.NewNodeFS(root, opts), &opts.MountOptions)
	} else {
		source, err := githubfs.New(githubfs.Options{DataDir: C.GoString(data), CacheDir: C.GoString(cache), RemoteBase: C.GoString(remote), CacheBytes: 4 << 30})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 0
		}
		if path := os.Getenv("GYIT_VIRTIO_CPU_PROFILE"); path != "" {
			var err error
			profile, err = os.Create(path)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 0
			}
			if err = pprof.StartCPUProfile(profile); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return 0
			}
		}
		b.source = source
		b.protocol = githubmount.Protocol(source, timeout)
	}
	backends.Lock()
	defer backends.Unlock()
	backends.next++
	backends.m[backends.next] = b
	return C.uint64_t(backends.next)
}

//export GyitVirtioRequest
func GyitVirtioRequest(id C.uint64_t, input unsafe.Pointer, inputSize C.int, output unsafe.Pointer, outputSize C.int) C.int {
	if inputSize < 40 || inputSize > 8<<20 || outputSize < 0 || outputSize > 8<<20 {
		return -1
	}
	backends.Lock()
	b := backends.m[uint64(id)]
	backends.Unlock()
	if b == nil {
		return -1
	}
	in := unsafe.Slice((*byte)(input), int(inputSize))
	out := unsafe.Slice((*byte)(output), int(outputSize))
	n, status := b.protocol.HandleFlat(in, out)
	if status != fuse.OK {
		fmt.Fprintf(os.Stderr, "virtio request: %v\n", status)
		return -1
	}
	return C.int(n)
}

//export GyitVirtioClose
func GyitVirtioClose(id C.uint64_t) {
	backends.Lock()
	b := backends.m[uint64(id)]
	delete(backends.m, uint64(id))
	backends.Unlock()
	if b != nil && b.source != nil {
		b.source.Close()
	}
}
func main() {}
