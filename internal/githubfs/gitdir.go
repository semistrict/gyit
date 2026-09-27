package githubfs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"google.golang.org/protobuf/proto"
	"gyit/internal/archive"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/repo"
	"gyit/internal/store"
)

const gitDirectoryKey = "git-directory.pb"
const gitMetadataLimit = 16 << 20

type gitDirectory struct {
	uid, gid       uint32
	snapshot       *repo.Snapshot
	cache          *store.DiskCache
	root, indexKey string
	hashSize       int
	backend        store.Store
	files          map[string]*pb.GitFile
}

// Retain the transport's exact objects, including signed commit headers. The
// common archive import shares its pack bytes; Git indexes and refs are new.
// Conversion fallbacks retain the source pack separately for native Git reads.
func (f *FS) prepareGitDirectory(ctx context.Context, source string, backend store.Store, stats repo.Stats) error {
	if out, err := f.git(ctx, source, "pack-refs", "--all").CombinedOutput(); err != nil {
		return fmt.Errorf("pack native Git refs: %w: %s", err, out)
	}
	if out, err := f.git(ctx, source, "read-tree", "HEAD").CombinedOutput(); err != nil {
		return fmt.Errorf("prepare native Git index: %w: %s", err, out)
	}
	format, err := f.git(ctx, source, "rev-parse", "--show-object-format").Output()
	if err != nil {
		return err
	}
	hashSize := 20
	if strings.TrimSpace(string(format)) == "sha256" {
		hashSize = 32
	}
	if err := f.prepareGitIndex(ctx, source, hashSize); err != nil {
		return err
	}
	manifest := &pb.GitDirectory{}
	for _, name := range []string{"HEAD", "packed-refs"} {
		file, err := os.Open(filepath.Join(source, name))
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(file, gitMetadataLimit+1))
		file.Close()
		if err != nil {
			return err
		}
		if len(data) > gitMetadataLimit {
			return fmt.Errorf("native Git %s exceeds metadata limit", name)
		}
		manifest.Files = append(manifest.Files, &pb.GitFile{Path: name, Size: int64(len(data)), InlineData: data})
	}
	config := []byte("[core]\n\trepositoryformatversion = 0\n\tbare = false\n\tlogallrefupdates = false\n")
	if hashSize == 32 {
		config = []byte("[core]\n\trepositoryformatversion = 1\n\tbare = false\n[extensions]\n\tobjectformat = sha256\n")
	}
	manifest.Files = append(manifest.Files, &pb.GitFile{Path: "config", Size: int64(len(config)), InlineData: config})
	packs, err := filepath.Glob(filepath.Join(source, "objects", "pack", "*.pack"))
	if err != nil {
		return err
	}
	if len(packs) == 0 {
		return fmt.Errorf("native Git requires a packed transport repository")
	}
	retained, prefix, retainedSize := stats.RetainedGitPack()
	files := []string{filepath.Join(source, "index")}
	for _, pack := range packs {
		files = append(files, pack, strings.TrimSuffix(pack, ".pack")+".idx")
	}
	for _, name := range files {
		file, err := os.Open(name)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return err
		}
		relative, err := filepath.Rel(source, name)
		if err != nil {
			file.Close()
			return err
		}
		entry := &pb.GitFile{Path: filepath.ToSlash(relative), Size: info.Size(), SegmentSize: archive.SegmentSize}
		actual, resolveErr := filepath.EvalSymlinks(name)
		if resolveErr != nil {
			file.Close()
			return resolveErr
		}
		if actual == retained && info.Size() == retainedSize {
			entry.SegmentPrefix = prefix
		} else {
			// These keys are private to the staging store until atomic publication.
			entry.SegmentPrefix = "git-files/" + filepath.Base(name)
			buffer := make([]byte, min(entry.Size, archive.SegmentSize))
			for off, part := int64(0), 0; off < entry.Size; part++ {
				chunk := buffer[:min(int64(len(buffer)), entry.Size-off)]
				if _, err = io.ReadFull(file, chunk); err != nil {
					break
				}
				if err = backend.Put(ctx, fmt.Sprintf("%s/%08x", entry.SegmentPrefix, part), chunk, "*"); err != nil {
					break
				}
				off += int64(len(chunk))
			}
		}
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		manifest.Files = append(manifest.Files, entry)
	}
	data, err := proto.Marshal(manifest)
	if err != nil {
		return err
	}
	return backend.Put(ctx, gitDirectoryKey, data, "*")
}

func openGitDirectory(ctx context.Context, backend store.Store) (*gitDirectory, error) {
	data, _, err := backend.Get(ctx, gitDirectoryKey, 0, -1)
	if err != nil {
		return nil, err
	}
	if len(data) > gitMetadataLimit {
		return nil, fmt.Errorf("native Git manifest exceeds metadata limit")
	}
	var manifest pb.GitDirectory
	if err := proto.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	view := &gitDirectory{backend: backend, files: make(map[string]*pb.GitFile)}
	for _, file := range manifest.Files {
		if !fs.ValidPath(file.Path) || file.Path == "." || file.Size < 0 || file.SegmentPrefix == "" && int64(len(file.InlineData)) != file.Size || file.SegmentPrefix != "" && (!fs.ValidPath(file.SegmentPrefix) || file.SegmentSize != archive.SegmentSize || len(file.InlineData) != 0) {
			return nil, fmt.Errorf("invalid native Git file descriptor")
		}
		if _, duplicate := view.files[file.Path]; duplicate {
			return nil, fmt.Errorf("duplicate native Git file")
		}
		view.files[file.Path] = file
	}
	for _, name := range []string{"HEAD", "config", "packed-refs", "index"} {
		if view.files[name] == nil {
			return nil, fmt.Errorf("native Git %s missing", name)
		}
	}
	return view, nil
}

func gitPath(relative string) (string, bool) {
	if relative == ".git" {
		return "", true
	}
	return strings.TrimPrefix(relative, ".git/"), strings.HasPrefix(relative, ".git/")
}
func (g *gitDirectory) lookup(name string) (repo.Entry, error) {
	switch name {
	case "", "refs", "refs/heads", "refs/tags", "objects", "objects/pack", "objects/info":
		return directory(path.Base(name)), nil
	}
	if file := g.files[name]; file != nil {
		return repo.Entry{Name: path.Base(name), Mode: 0100444, Size: file.Size}, nil
	}
	return repo.Entry{}, syscall.ENOENT
}
func (g *gitDirectory) readDir(name, after string, limit int) ([]repo.Entry, error) {
	entry, err := g.lookup(name)
	if err != nil {
		return nil, err
	}
	if entry.Mode != 0040000 {
		return nil, syscall.ENOTDIR
	}
	names := []string{"refs", "refs/heads", "refs/tags", "objects", "objects/pack", "objects/info"}
	for file := range g.files {
		names = append(names, file)
	}
	var entries []repo.Entry
	for _, file := range names {
		parent := path.Dir(file)
		if parent == "." {
			parent = ""
		}
		if parent == name && path.Base(file) > after {
			e, _ := g.lookup(file)
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return entries[:min(limit, len(entries))], nil
}
func (g *gitDirectory) read(ctx context.Context, name string, b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if name == "index" && g.cache != nil {
		index, release, err := g.mountedIndex(ctx)
		if err != nil {
			return 0, err
		}
		defer release()
		if off >= int64(len(index)) {
			return 0, nil
		}
		return copy(b, index[off:]), nil
	}
	return g.readRaw(ctx, name, b, off)
}
func (g *gitDirectory) readRaw(ctx context.Context, name string, b []byte, off int64) (int, error) {
	entry, err := g.lookup(name)
	if err != nil {
		return 0, err
	}
	if entry.Mode == 0040000 {
		return 0, syscall.EISDIR
	}
	file := g.files[name]
	if off >= file.Size || len(b) == 0 {
		return 0, nil
	}
	b = b[:min(int64(len(b)), file.Size-off)]
	if file.SegmentPrefix == "" {
		return copy(b, file.InlineData[off:]), nil
	}
	n := 0
	for len(b) > 0 {
		size := min(int64(len(b)), file.SegmentSize-off%file.SegmentSize)
		data, _, err := g.backend.Get(ctx, fmt.Sprintf("%s/%08x", file.SegmentPrefix, off/file.SegmentSize), off%file.SegmentSize, size)
		if err != nil {
			return n, err
		}
		if int64(len(data)) != size {
			return n, io.ErrUnexpectedEOF
		}
		copy(b, data)
		n += len(data)
		b = b[len(data):]
		off += size
	}
	return n, nil
}
