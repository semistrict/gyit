package githubfs

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gyit/internal/macfs"
	"gyit/internal/repo"
	"gyit/internal/store"
)

const nativeIndexLimit = 128 << 20

func indexHash(size int) hash.Hash {
	if size == sha256.Size {
		return sha256.New()
	}
	return sha1.New()
}

// rewriteIndex keeps Git's native v2 format, object IDs, and extensions. Stat
// fields are an ordinary cache of attributes actually served by this filesystem;
// neither assume-unchanged nor skip-worktree is used. Git still stats each file.
func rewriteIndex(b []byte, hashSize int, update func(string, []byte, uint16) error) error {
	if hashSize != 20 && hashSize != 32 {
		return fmt.Errorf("invalid Git hash size")
	}
	if len(b) < 12+hashSize || len(b) > nativeIndexLimit || string(b[:4]) != "DIRC" || binary.BigEndian.Uint32(b[4:]) != 2 {
		return fmt.Errorf("expected bounded native Git index v2")
	}
	h := indexHash(hashSize)
	h.Write(b[:len(b)-hashSize])
	if !bytes.Equal(h.Sum(nil), b[len(b)-hashSize:]) {
		return fmt.Errorf("invalid native index checksum")
	}
	end := len(b) - hashSize
	off := 12
	for count := binary.BigEndian.Uint32(b[8:]); count > 0; count-- {
		fixed := 40 + hashSize + 2
		if off+fixed > end {
			return fmt.Errorf("truncated native index entry")
		}
		entry := b[off:]
		flags := binary.BigEndian.Uint16(entry[40+hashSize:])
		if flags&0x7000 != 0 {
			return fmt.Errorf("extended or unmerged index is unsupported")
		}
		n := bytes.IndexByte(b[off+fixed:end], 0)
		if n < 0 {
			return fmt.Errorf("unterminated native index name")
		}
		name := string(entry[fixed : fixed+n])
		if name == "" || !filepath.IsLocal(name) {
			return fmt.Errorf("invalid native index name")
		}
		if err := update(name, entry[:40], flags); err != nil {
			return err
		}
		// Older templates may carry CE_VALID; it is never propagated to readers.
		binary.BigEndian.PutUint16(entry[40+hashSize:], flags&0x0fff)
		off += (fixed + n + 1 + 7) &^ 7
		if off > end {
			return fmt.Errorf("truncated native index padding")
		}
	}
	h.Reset()
	h.Write(b[:end])
	copy(b[end:], h.Sum(nil))
	return nil
}

func (f *FS) prepareGitIndex(ctx context.Context, source string, hashSize int) error {
	if out, err := f.git(ctx, source, "update-index", "--index-version=2").CombinedOutput(); err != nil {
		return fmt.Errorf("native index version: %w: %s", err, out)
	}
	// ls-tree reads object metadata, never worktree contents or blob bodies.
	tree, err := f.git(ctx, source, "ls-tree", "-r", "-l", "-z", "HEAD").Output()
	if err != nil {
		return err
	}
	sizes := make(map[string]uint64)
	for _, line := range bytes.Split(tree, []byte{0}) {
		if len(line) == 0 {
			continue
		}
		fields, name, ok := bytes.Cut(line, []byte{'\t'})
		if !ok {
			return fmt.Errorf("invalid Git tree entry")
		}
		header := strings.Fields(string(fields))
		if len(header) != 4 {
			return fmt.Errorf("invalid Git tree metadata")
		}
		var size uint64
		if header[1] != "commit" {
			size, err = strconv.ParseUint(header[3], 10, 64)
			if err != nil {
				return err
			}
		}
		sizes[string(name)] = size
	}
	name := filepath.Join(source, "index")
	b, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	err = rewriteIndex(b, hashSize, func(name string, stat []byte, flags uint16) error {
		size, ok := sizes[name]
		if !ok {
			return fmt.Errorf("index path missing from Git tree")
		}
		// All immutable worktree timestamps are the Unix epoch. UID/GID/inode are
		// filled for the mount when this portable template is first requested.
		clear(stat[:24])
		clear(stat[28:])
		n := uint32(size)
		if n == 0 && size != 0 {
			n = 0x80000000
		}
		binary.BigEndian.PutUint32(stat[36:], n)
		return nil
	})
	if err != nil {
		return err
	}
	return os.WriteFile(name, b, 0600)
}

func (g *gitDirectory) mountedIndex(ctx context.Context) ([]byte, func(), error) {
	return g.cache.Load(ctx, g.indexKey, func() ([]byte, error) {
		file := g.files["index"]
		if file.Size > nativeIndexLimit {
			return nil, fmt.Errorf("native index exceeds size bound")
		}
		b := make([]byte, file.Size)
		n, err := g.readRaw(ctx, "index", b, 0)
		if err != nil {
			return nil, err
		}
		if n != len(b) {
			return nil, fmt.Errorf("short native index")
		}
		err = rewriteIndex(b, g.hashSize, func(name string, stat []byte, flags uint16) error {
			// Upgrade old portable templates in the cache without reimporting or
			// mutating durable history. Only the old CE_VALID templates need this.
			if flags&0x8000 != 0 {
				entry, err := g.snapshot.Resolve(ctx, name)
				if err != nil {
					return err
				}
				size := uint32(entry.Size)
				if size == 0 && entry.Size != 0 {
					size = 0x80000000
				}
				binary.BigEndian.PutUint32(stat[36:], size)
			}
			binary.BigEndian.PutUint32(stat[20:], uint32(macfs.Inode(g.root+"/"+name)))
			binary.BigEndian.PutUint32(stat[28:], g.uid)
			binary.BigEndian.PutUint32(stat[32:], g.gid)
			return nil
		})
		return b, err
	})
}

func (g *gitDirectory) bindIndex(cache *store.DiskCache, root string, snapshot *repo.Snapshot) {
	g.uid, g.gid = uint32(os.Getuid()), uint32(os.Getgid())
	g.snapshot = snapshot
	g.cache = cache
	g.root = root
	g.hashSize = 20
	if bytes.Contains(g.files["config"].InlineData, []byte("objectformat = sha256")) {
		g.hashSize = 32
	}
	g.indexKey = fmt.Sprintf("native-index-stat-v1/%s/%s/%d/%d", root, snapshot.SHA, g.uid, g.gid)
}
