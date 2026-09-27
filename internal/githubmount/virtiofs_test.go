//go:build darwin && gyit_virtiofs

package githubmount

import (
	"encoding/binary"
	"fmt"
	"gyit/internal/githubfs"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Exercise the actual Linux wire protocol on the macOS host, rather than only
// calling node methods. In particular Darwin errno and attribute layouts differ.
func TestDirectVirtioProtocol(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	os.Mkdir(source, 0700)
	git := func(args ...string) {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", source}, args...)...)
		c.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_AUTHOR_NAME=Test", "GIT_AUTHOR_EMAIL=test@example.test", "GIT_COMMITTER_NAME=Test", "GIT_COMMITTER_EMAIL=test@example.test")
		if b, e := c.CombinedOutput(); e != nil {
			t.Fatalf("git: %v: %s", e, b)
		}
	}
	git("init", "-q", "-b", "main")
	os.WriteFile(filepath.Join(source, "hello"), []byte("hello virtio\n"), 0600)
	// More than two directory pages, with varied sizes and an executable.
	if err := os.Mkdir(filepath.Join(source, "many"), 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 270; i++ {
		if err := os.WriteFile(filepath.Join(source, "many", fmt.Sprintf("file-%03d", i)), make([]byte, i), 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("hello", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	git("add", ".")
	git("commit", "-qm", "fixture")
	remote := filepath.Join(root, "remote", "acme")
	os.MkdirAll(remote, 0700)
	git("clone", "--quiet", "--bare", source, filepath.Join(remote, "repo.git"))
	namespace, err := githubfs.New(githubfs.Options{DataDir: filepath.Join(root, "data"), CacheDir: filepath.Join(root, "cache"), CacheBytes: 64 << 20, RemoteBase: "file://" + filepath.Dir(remote)})
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	ps := Protocol(namespace, time.Minute)
	var sequence atomic.Uint64
	call := func(op uint32, node uint64, payload []byte) ([]byte, int32) {
		t.Helper()
		unique := sequence.Add(1)
		in := make([]byte, 40+len(payload))
		binary.LittleEndian.PutUint32(in, uint32(len(in)))
		binary.LittleEndian.PutUint32(in[4:], op)
		binary.LittleEndian.PutUint64(in[8:], unique)
		binary.LittleEndian.PutUint64(in[16:], node)
		copy(in[40:], payload)
		out := make([]byte, 1<<20)
		n, status := ps.HandleFlat(in, out)
		if status != 0 {
			t.Fatalf("opcode %d transport %v", op, status)
		}
		if n == 0 {
			return nil, 0
		}
		if n < 16 || int(binary.LittleEndian.Uint32(out)) != n || binary.LittleEndian.Uint64(out[8:]) != unique {
			t.Fatalf("invalid reply %x", out[:n])
		}
		return out[16:n], int32(binary.LittleEndian.Uint32(out[4:]))
	}
	init := make([]byte, 64)
	binary.LittleEndian.PutUint32(init, 7)
	binary.LittleEndian.PutUint32(init[4:], 38)
	if b, e := call(26, 1, init); e != 0 || len(b) < 24 {
		t.Fatalf("init %x %d", b, e)
	}
	lookup := func(parent uint64, name string) uint64 {
		t.Helper()
		b, e := call(1, parent, append([]byte(name), 0))
		if e != 0 || len(b) != 128 {
			t.Fatalf("lookup %s: size=%d err=%d", name, len(b), e)
		}
		if name != "hello" && binary.LittleEndian.Uint64(b[16:24]) == 0 {
			t.Fatalf("stable positive lookup %s was not cached", name)
		}
		return binary.LittleEndian.Uint64(b)
	}
	host := lookup(1, "github.com")
	owner := lookup(host, "acme")
	repository := lookup(owner, "repo")
	file := lookup(repository, "hello")
	// The first lookup may cross publication; caching is conservative then.
	// A stable lookup after publication must have a positive metadata TTL.
	if b, e := call(1, repository, []byte("hello\x00")); e != 0 || len(b) != 128 || binary.LittleEndian.Uint64(b[16:24]) == 0 {
		t.Fatalf("published lookup TTL: %x %d", b, e)
	}
	rootNode := &node{source: githubfs.Namespace{FS: namespace}}
	entry, err := namespace.Lookup(t.Context(), "acme/repo/hello")
	if err != nil {
		t.Fatal(err)
	}
	if rootNode.stableMetadata("github.com/acme/repo/hello", entry, 1) {
		t.Fatal("an entry read before publication must not be cached after publication")
	}
	read := make([]byte, 40)
	binary.LittleEndian.PutUint32(read[16:], 4096)
	if b, e := call(15, file, read); e != 0 || string(b) != "hello virtio\n" {
		t.Fatalf("read %q %d", b, e)
	}
	if b, e := call(1, repository, []byte("missing\x00")); e != -2 && (e != 0 || len(b) != 128 || binary.LittleEndian.Uint64(b) != 0) {
		t.Fatalf("missing must return ENOENT or a negative entry: %x %d", b, e)
	}
	// Exercise READDIRPLUS with small replies, forcing both response overflow
	// and 128-entry backend page boundaries. Rewinds must retain the same data.
	link := lookup(repository, "link")
	if b, e := call(5, link, nil); e != 0 || string(b) != "hello" {
		t.Fatalf("readlink: %q %d", b, e)
	}
	many := lookup(repository, "many")
	opened, e := call(27, many, make([]byte, 8))
	if e != 0 || len(opened) != 16 {
		t.Fatalf("opendir: %x %d", opened, e)
	}
	fh := binary.LittleEndian.Uint64(opened)
	for pass := 0; pass < 2; pass++ {
		offset := uint64(0)
		seen := 0
		for {
			req := make([]byte, 40)
			binary.LittleEndian.PutUint64(req, fh)
			binary.LittleEndian.PutUint64(req[8:], offset)
			binary.LittleEndian.PutUint32(req[16:], 512)
			page, err := call(44, many, req)
			if err != 0 {
				t.Fatalf("readdirplus: %d", err)
			}
			if len(page) == 0 {
				break
			}
			for len(page) > 0 {
				if len(page) < 152 {
					t.Fatalf("short dirent: %x", page)
				}
				n := int(binary.LittleEndian.Uint32(page[144:]))
				length := 128 + (24+n+7)&^7
				if n < 1 || length > len(page) {
					t.Fatalf("invalid dirent name length %d", n)
				}
				name := string(page[152 : 152+n])
				if name != fmt.Sprintf("file-%03d", seen) || binary.LittleEndian.Uint64(page[48:]) != uint64(seen) || binary.LittleEndian.Uint32(page[100:])&0111 == 0 {
					t.Fatalf("wrong directory entry or attrs at %d: %q %x", seen, name, page[:128])
				}
				next := binary.LittleEndian.Uint64(page[136:])
				if next <= offset {
					t.Fatalf("directory offset did not advance")
				}
				offset = next
				seen++
				page = page[length:]
			}
		}
		if seen != 270 {
			t.Fatalf("directory pass %d: %d entries", pass, seen)
		}
	}
	release := make([]byte, 24)
	binary.LittleEndian.PutUint64(release, fh)
	if _, e := call(29, many, release); e != 0 {
		t.Fatalf("releasedir: %d", e)
	}

	// The direct transport runs independent requests concurrently. Replies
	// must remain isolated and retain their own unique IDs and contents.
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 20; j++ {
				b, e := call(15, file, read)
				if e != 0 || string(b) != "hello virtio\n" {
					t.Errorf("concurrent read: %q %d", b, e)
					return
				}
				if b, e := call(3, file, make([]byte, 16)); e != 0 || len(b) != 104 {
					t.Errorf("concurrent getattr: %d %d", len(b), e)
					return
				}
			}
		}()
	}
	readers.Wait()

	xattr := append(make([]byte, 8), []byte("user.missing\x00")...)
	if _, e := call(22, file, xattr); e != -61 {
		t.Fatalf("Linux ENODATA: %d", e)
	}
	// A read-only namespace must reject creation even without a read-only mount.
	if _, e := call(35, repository, append(make([]byte, 16), []byte("new\x00")...)); e != -30 {
		t.Fatalf("create errno %d", e)
	}
	if _, e := call(999, 1, nil); e != -38 {
		t.Fatalf("Linux ENOSYS: %d", e)
	}

	forget := make([]byte, 8)
	binary.LittleEndian.PutUint64(forget, 1)
	if b, e := call(2, file, forget); len(b) != 0 || e != 0 {
		t.Fatal("forget must have no response")
	}
}
