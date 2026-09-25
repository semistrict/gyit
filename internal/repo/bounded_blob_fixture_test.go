package repo

import (
	"bytes"
	"compress/zlib"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
)

type boundedBlobFixtureEntry struct {
	kind                byte
	raw, target, baseID []byte
	baseIndex           int
	id                  []byte
	declared            *uint64
	badCompressed       bool
}

func boundedBlobObjectID(kind string, body []byte) []byte {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d\x00", kind, len(body))
	h.Write(body)
	return h.Sum(nil)
}
func boundedBlobPackHeader(kind byte, size uint64) []byte {
	b := byte(size&15) | kind<<4
	size >>= 4
	var out []byte
	for size != 0 {
		out = append(out, b|128)
		b = byte(size & 127)
		size >>= 7
	}
	return append(out, b)
}
func boundedBlobOfsDistance(n uint64) []byte {
	b := []byte{byte(n & 127)}
	for n >>= 7; n != 0; n >>= 7 {
		n--
		b = append([]byte{byte(n&127) | 128}, b...)
	}
	return b
}
func boundedBlobLiteralDelta(base int, target []byte) []byte {
	b := binary.AppendUvarint(nil, uint64(base))
	b = binary.AppendUvarint(b, uint64(len(target)))
	for len(target) != 0 {
		n := min(127, len(target))
		b = append(b, byte(n))
		b = append(b, target[:n]...)
		target = target[n:]
	}
	return b
}
func boundedBlobFixture(t *testing.T, entries []boundedBlobFixtureEntry) (string, [][]byte) {
	t.Helper()
	dir := t.TempDir()
	cmd := exec.CommandContext(t.Context(), "git", "init", "--bare", "--object-format=sha1", "-q", dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("init: %v %s", err, out)
	}
	pack := []byte("PACK")
	pack = binary.BigEndian.AppendUint32(pack, 2)
	pack = binary.BigEndian.AppendUint32(pack, uint32(len(entries)))
	ids := make([][]byte, len(entries))
	offsets := make([]uint32, len(entries))
	crcs := make([]uint32, len(entries))
	for i, e := range entries {
		offsets[i] = uint32(len(pack))
		size := uint64(len(e.raw))
		if e.declared != nil {
			size = *e.declared
		}
		pack = append(pack, boundedBlobPackHeader(e.kind, size)...)
		if e.kind == 6 {
			pack = append(pack, boundedBlobOfsDistance(uint64(offsets[i]-offsets[e.baseIndex]))...)
		}
		if e.kind == 7 {
			pack = append(pack, e.baseID...)
		}
		if e.badCompressed {
			pack = append(pack, 0, 1, 2, 3)
		} else {
			var b bytes.Buffer
			w := zlib.NewWriter(&b)
			if _, err := w.Write(e.raw); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			pack = append(pack, b.Bytes()...)
		}
		crcs[i] = crc32.ChecksumIEEE(pack[offsets[i]:])
		ids[i] = e.id
		if ids[i] == nil {
			kind := "blob"
			body := e.target
			if e.kind < 5 {
				kind = []string{"", "commit", "tree", "blob", "tag"}[e.kind]
				body = e.raw
			}
			ids[i] = boundedBlobObjectID(kind, body)
		}
	}
	packHash := sha1.Sum(pack)
	pack = append(pack, packHash[:]...)
	order := make([]int, len(entries))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return bytes.Compare(ids[order[i]], ids[order[j]]) < 0 })
	idx := []byte{255, 't', 'O', 'c', 0, 0, 0, 2}
	for first := 0; first < 256; first++ {
		n := uint32(0)
		for _, id := range ids {
			if int(id[0]) <= first {
				n++
			}
		}
		idx = binary.BigEndian.AppendUint32(idx, n)
	}
	for _, i := range order {
		idx = append(idx, ids[i]...)
	}
	for _, i := range order {
		idx = binary.BigEndian.AppendUint32(idx, crcs[i])
	}
	for _, i := range order {
		idx = binary.BigEndian.AppendUint32(idx, offsets[i])
	}
	idx = append(idx, packHash[:]...)
	idxHash := sha1.Sum(idx)
	idx = append(idx, idxHash[:]...)
	prefix := filepath.Join(dir, "objects", "pack", fmt.Sprintf("pack-%x", packHash))
	if err := os.WriteFile(prefix+".pack", pack, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(prefix+".idx", idx, 0600); err != nil {
		t.Fatal(err)
	}
	rev := []byte("RIDX")
	rev = binary.BigEndian.AppendUint32(rev, 1)
	rev = binary.BigEndian.AppendUint32(rev, 1)
	ordinal := make([]uint32, len(order))
	for index, physical := range order {
		ordinal[physical] = uint32(index)
	}
	for _, index := range ordinal {
		rev = binary.BigEndian.AppendUint32(rev, index)
	}
	rev = append(rev, packHash[:]...)
	revHash := sha1.Sum(rev)
	rev = append(rev, revHash[:]...)
	if err := os.WriteFile(prefix+".rev", rev, 0600); err != nil {
		t.Fatal(err)
	}
	return prefix, ids
}
