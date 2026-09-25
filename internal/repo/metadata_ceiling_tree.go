package repo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	bolt "go.etcd.io/bbolt"
)

// Archived trees retain their native representation. This validator checks
// entry names, types, and duplicates without building another directory index.
type ceilingTreeValidator struct {
	tmp    string
	reader *bufio.Reader
	names  [][]byte
	counts ceilingValidationCounts
}

type ceilingValidationCounts struct{ entries, blobLookups int64 }

type ceilingContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r ceilingContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func (v *ceilingTreeValidator) validate(ctx context.Context, body io.Reader, oidBytes int, fileSize func(context.Context, []byte) (int64, error)) (retErr error) {
	v.counts = ceilingValidationCounts{}
	if oidBytes != 20 && oidBytes != 32 {
		return fmt.Errorf("invalid tree object ID width")
	}
	input := ceilingContextReader{ctx: ctx, r: body}
	if v.reader == nil {
		v.reader = bufio.NewReader(input)
	} else {
		v.reader.Reset(input)
	}
	// The retained name pool is at most the production sort batch: 4096 names
	// of at most 255 bytes. Large trees spill names only, never raw tree bodies,
	// entry protobufs, child IDs, sizes or directory output pages.
	used := 0
	var spill *bolt.DB
	path := filepath.Join(v.tmp, "ceiling-tree-names.db")
	defer func() {
		v.reader.Reset(nil)
		if spill != nil {
			retErr = errors.Join(retErr, spill.Close(), os.Remove(path))
		}
	}()
	flush := func() error {
		if spill == nil {
			var err error
			spill, err = bolt.Open(path, 0600, &bolt.Options{NoSync: true})
			if err != nil {
				return err
			}
		}
		err := spill.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("names"))
			if err != nil {
				return err
			}
			for _, name := range v.names[:used] {
				if err := ctx.Err(); err != nil {
					return err
				}
				if b.Get(name) != nil {
					return fmt.Errorf("duplicate directory name")
				}
				if err := b.Put(name, []byte{1}); err != nil {
					return err
				}
			}
			return nil
		})
		used = 0
		return err
	}
	var child [32]byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		mode, err := readTreeMode(v.reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		name, err := v.reader.ReadSlice(0)
		if err != nil {
			return err
		}
		name = name[:len(name)-1]
		if len(name) == 0 || len(name) > 255 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.ContainsRune(name, '/') {
			return fmt.Errorf("unsupported tree entry name %q", name)
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
			return fmt.Errorf("unsupported tree mode %o", mode)
		}
		// Copy the name before ReadFull may refill the buffered reader.
		if used == len(v.names) {
			v.names = append(v.names, nil)
		}
		v.names[used] = append(v.names[used][:0], name...)
		if _, err := io.ReadFull(v.reader, child[:oidBytes]); err != nil {
			return err
		}
		if canonical != 0040000 && canonical != 0160000 {
			v.counts.blobLookups++
			if _, err := fileSize(ctx, child[:oidBytes]); err != nil {
				return fmt.Errorf("resolve tree entry %x: %w", child[:oidBytes], err)
			}
		}
		v.counts.entries++
		used++
		if used == directorySortEntries {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if spill != nil {
		return flush()
	}
	names := v.names[:used]
	sort.Slice(names, func(i, j int) bool { return bytes.Compare(names[i], names[j]) < 0 })
	for i := 1; i < len(names); i++ {
		if bytes.Equal(names[i-1], names[i]) {
			return fmt.Errorf("duplicate directory name")
		}
	}
	return nil
}
