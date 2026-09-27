package githubfs

import (
	"bytes"
	"encoding/binary"
	"maps"
	"os"
	"path/filepath"
	"testing"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/macfs"
)

func TestMountedIndexStatAndLegacyUpgrade(t *testing.T) {
	f, _, _, _ := openFixture(t)
	root := "acme/project"
	waitReady(t, f, root)
	f.mu.Lock()
	j := f.jobs[Target{Owner: "acme", Repository: "project"}.Key()]
	f.mu.Unlock()
	j.mu.RLock()
	original := j.git
	j.mu.RUnlock()
	raw := make([]byte, original.files["index"].Size)
	if n, err := original.readRaw(t.Context(), "index", raw, 0); err != nil || n != len(raw) {
		t.Fatalf("template: %d %v", n, err)
	}
	for _, legacy := range []bool{false, true} {
		view := *original
		view.files = maps.Clone(original.files)
		view.indexKey += "/regression"
		if legacy {
			template := bytes.Clone(raw)
			off := 12
			for i := uint32(0); i < binary.BigEndian.Uint32(template[8:]); i++ {
				stat := template[off:]
				clear(stat[:24])
				clear(stat[28:40])
				binary.BigEndian.PutUint16(stat[60:], binary.BigEndian.Uint16(stat[60:])|0x8000)
				n := bytes.IndexByte(stat[62:], 0)
				off += (62 + n + 1 + 7) &^ 7
			}
			h := indexHash(20)
			h.Write(template[:len(template)-20])
			copy(template[len(template)-20:], h.Sum(nil))
			view.files["index"] = &pb.GitFile{Path: "index", Size: int64(len(template)), InlineData: template}
			view.indexKey += "/legacy"
		}
		b, release, err := view.mountedIndex(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		data := bytes.Clone(b)
		release()
		count := 0
		err = rewriteIndex(data, 20, func(name string, stat []byte, flags uint16) error {
			count++
			if flags&0xf000 != 0 {
				t.Fatalf("index suppresses scanning: flags %x", flags)
			}
			entry, err := f.Lookup(t.Context(), root+"/"+name)
			if err != nil {
				return err
			}
			if binary.BigEndian.Uint32(stat[20:]) != uint32(macfs.Inode("github.com/"+root+"/"+name)) || binary.BigEndian.Uint32(stat[28:]) != uint32(os.Getuid()) || binary.BigEndian.Uint32(stat[32:]) != uint32(os.Getgid()) || binary.BigEndian.Uint32(stat[36:]) != uint32(entry.Size) {
				t.Fatalf("index stat differs from mounted attributes for %q", name)
			}
			if !bytes.Equal(stat[:20], make([]byte, 20)) {
				t.Fatal("index timestamps differ from mounted epoch")
			}
			return nil
		})
		if err != nil || count == 0 {
			t.Fatalf("index verification: %d %v", count, err)
		}
	}
}

func TestRewriteIndexRejectsCorruptInput(t *testing.T) {
	for _, b := range [][]byte{nil, []byte("DIRC"), make([]byte, 80)} {
		if err := rewriteIndex(b, 20, func(string, []byte, uint16) error { t.Fatal("corrupt index callback"); return nil }); err == nil {
			t.Fatal("accepted corrupt index")
		}
	}
}

func TestPortableIndexAcceptedByNativeGit(t *testing.T) {
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			root := t.TempDir()
			nativeGit(t, root, "init", "-q", "--object-format="+format)
			for _, name := range []string{"empty", "space name", "tab\tname"} {
				content := []byte("fixture\n")
				if name == "empty" {
					content = nil
				}
				if err := os.WriteFile(filepath.Join(root, name), content, 0600); err != nil {
					t.Fatal(err)
				}
			}
			nativeGit(t, root, "add", ".")
			nativeGit(t, root, "-c", "user.name=Test", "-c", "user.email=test@example.test", "commit", "-qm", "fixture")
			bare := filepath.Join(t.TempDir(), "source.git")
			nativeGit(t, root, "clone", "--bare", "--quiet", root, bare)
			nativeGit(t, bare, "read-tree", "HEAD")
			f := &FS{}
			size := 20
			if format == "sha256" {
				size = 32
			}
			if err := f.prepareGitIndex(t.Context(), bare, size); err != nil {
				t.Fatal(err)
			}
			if got := nativeGit(t, bare, "ls-files", "-v", "-z"); !bytes.Equal(got, []byte("H empty\x00H space name\x00H tab\tname\x00")) {
				t.Fatalf("native Git rejected portable index: %q", got)
			}
		})
	}
}
