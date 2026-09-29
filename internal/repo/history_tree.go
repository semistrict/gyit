package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

// Views into a borrowed tree; unchanged entries allocate neither names nor OIDs.
type historyTreeEntry struct {
	name, oid []byte
	mode      uint32
}

// Skip byte-identical entries in a run, validating only one copy. The common
// byte prefix can end inside a name or binary OID, so advance only through
// complete parsed entries. Differing bytes still use the ordinary ordered merge.
func skipEqualHistoryEntries(ctx context.Context, left, right *[]byte) error {
	a, b := *left, *right
	n := min(len(a), len(b))
	if n < 64 || !bytes.Equal(a[:64], b[:64]) {
		return nil
	}
	common := 64
	for common+4096 <= n && bytes.Equal(a[common:common+4096], b[common:common+4096]) {
		common += 4096
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	lo, hi := common, min(n, common+4096)
	for lo < hi {
		mid := lo + (hi-lo+1)/2
		if bytes.Equal(a[common:mid], b[common:mid]) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	common = lo
	source, acquisition := ctx.Value(historySourceKey{}).(*historySource)
	indexed := acquisition && source.indexedByGit
	consumed := 0
	for steps := 0; len(a) > 0 && consumed < common; steps++ {
		if steps%256 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		next := a
		if indexed {
			// Equal bytes cannot add a changed path. For the trusted Git import,
			// find the name terminator and skip its binary OID; re-parsing modes
			// and validating unchanged names would verify the same input again.
			// Bounds remain mandatory, and NULs inside the OID are never scanned.
			nul := bytes.IndexByte(next, 0)
			if nul < 0 || len(next)-nul-1 < 20 {
				return io.ErrUnexpectedEOF
			}
			next = next[nul+21:]
		} else if _, err := takeHistoryTreeEntry(&next); err != nil {
			return err
		}
		end := consumed + len(a) - len(next)
		if end > common {
			break
		}
		consumed = end
		a = next
	}
	*left = (*left)[consumed:]
	*right = (*right)[consumed:]
	return nil
}

func takeHistoryTreeEntry(raw *[]byte) (historyTreeEntry, error) {
	data := *raw
	if len(data) == 0 {
		return historyTreeEntry{}, io.EOF
	}
	space := bytes.IndexByte(data, ' ')
	if space <= 0 {
		return historyTreeEntry{}, fmt.Errorf("invalid tree mode")
	}
	var mode uint32
	for _, c := range data[:space] {
		if c < '0' || c > '7' || mode > (^uint32(0)-uint32(c-'0'))/8 {
			return historyTreeEntry{}, fmt.Errorf("invalid tree mode")
		}
		mode = mode*8 + uint32(c-'0')
	}
	data = data[space+1:]
	nul := bytes.IndexByte(data, 0)
	if nul < 0 || len(data)-nul-1 < 20 {
		return historyTreeEntry{}, io.ErrUnexpectedEOF
	}
	name := data[:nul]
	if len(name) == 0 || len(name) > 255 || bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte("..")) || bytes.IndexByte(name, '/') >= 0 {
		return historyTreeEntry{}, fmt.Errorf("invalid native tree name")
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
		return historyTreeEntry{}, fmt.Errorf("invalid native tree mode")
	}
	entry := historyTreeEntry{name: name, oid: data[nul+1 : nul+21], mode: canonical}
	*raw = data[nul+21:]
	return entry, nil
}

// Git compares a directory name as if it had a trailing slash.
func compareHistoryTreeEntries(a, b historyTreeEntry) int {
	n := min(len(a.name), len(b.name))
	if c := bytes.Compare(a.name[:n], b.name[:n]); c != 0 {
		return c
	}
	end := func(e historyTreeEntry) byte {
		if len(e.name) > n {
			return e.name[n]
		}
		if e.mode == 0040000 {
			return '/'
		}
		return 0
	}
	return int(end(a)) - int(end(b))
}
