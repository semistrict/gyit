package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"sort"
)

const nativeTreeBytes = 64 << 10

type Dirent struct {
	Name, OID     string
	Mode, RawMode uint32
}

func direntOf(e Entry) Dirent {
	return Dirent{Name: e.Name, OID: e.OID, Mode: e.Mode, RawMode: e.RawMode}
}
func (s *Snapshot) LookupName(ctx context.Context, tree, name string) (Dirent, error) {
	e, err := s.Lookup(ctx, tree, name)
	return direntOf(e), err
}
func (s *Snapshot) ReadDirNames(ctx context.Context, tree, after string, limit int) ([]Dirent, error) {
	es, err := s.ReadDir(ctx, tree, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]Dirent, len(es))
	for i, e := range es {
		out[i] = direntOf(e)
	}
	return out, nil
}
func (s *Snapshot) ResolveName(ctx context.Context, path string) (Dirent, error) {
	e, err := s.Resolve(ctx, path)
	return direntOf(e), err
}
func parseNativeTree(raw []byte) ([]Dirent, error) {
	if len(raw) > nativeTreeBytes {
		return nil, fmt.Errorf("native tree size limit")
	}
	r := bufio.NewReader(bytes.NewReader(raw))
	var out []Dirent
	for {
		mode, err := readTreeMode(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name, err := r.ReadSlice(0)
		if err != nil {
			return nil, err
		}
		name = name[:len(name)-1]
		if len(name) == 0 || len(name) > 255 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.ContainsRune(name, '/') {
			return nil, fmt.Errorf("invalid native tree name")
		}
		canonical := mode & 0170000
		switch canonical {
		case 0100000:
			canonical |= 0644
			if mode&0100 != 0 {
				canonical = 0100755
			}
		case 0040000, 0120000, 0160000:
		default:
			return nil, fmt.Errorf("invalid native tree mode")
		}
		copiedName := string(name)
		var oid [20]byte
		if _, err := io.ReadFull(r, oid[:]); err != nil {
			return nil, err
		}
		e := Dirent{Name: copiedName, OID: hex.EncodeToString(oid[:]), Mode: canonical}
		if mode != canonical {
			e.RawMode = mode
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	for i := 1; i < len(out); i++ {
		if out[i-1].Name == out[i].Name {
			return nil, fmt.Errorf("duplicate native tree name")
		}
	}
	return out, nil
}

func readTreeMode(r *bufio.Reader) (uint32, error) {
	var mode uint32
	seen := false
	for {
		part, err := r.ReadSlice(' ')
		if err == nil {
			part = part[:len(part)-1]
		}
		for _, c := range part {
			if c < '0' || c > '7' || mode > (^uint32(0)-uint32(c-'0'))/8 {
				return 0, fmt.Errorf("invalid tree mode")
			}
			mode = mode*8 + uint32(c-'0')
			seen = true
		}
		switch err {
		case nil:
			if !seen {
				return 0, fmt.Errorf("empty tree mode")
			}
			return mode, nil
		case bufio.ErrBufferFull:
			continue
		case io.EOF:
			if seen {
				return 0, io.ErrUnexpectedEOF
			}
			return 0, io.EOF
		default:
			return 0, err
		}
	}
}
