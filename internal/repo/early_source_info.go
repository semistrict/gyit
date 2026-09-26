package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	sizetable "gyit/internal/globalsizes"
	metaplan "gyit/internal/packmeta"
	"gyit/internal/scratchmap"
	"io"
	"strconv"
)

type sourceInfo struct {
	ordered           *orderedArchiveDispatch
	metadata          *metaplan.Planner
	archive           *sourceArchiveImport
	table             *scratchmap.Map
	globalRecords     []sizetable.Record
	globalCollectPeak uint64
}

func (s *sourceInfo) close() error {
	s.globalRecords = nil
	var archiveErr error
	if s.ordered != nil {
		archiveErr = s.ordered.close()
		s.ordered = nil
	}
	if s.archive != nil {
		archiveErr = errors.Join(archiveErr, s.archive.close())
		s.archive = nil
	}
	if s.metadata != nil {
		archiveErr = errors.Join(archiveErr, s.metadata.Close())
		s.metadata = nil
	}
	if s.table == nil {
		return archiveErr
	}
	t := s.table
	s.table = nil
	return errors.Join(archiveErr, t.Close())
}
func (s *sourceInfo) lookup(oid []byte) (byte, int64, bool, error) {
	if s.metadata != nil {
		if len(oid) != 20 {
			return 0, 0, false, fmt.Errorf("source metadata OID width")
		}
		var id [20]byte
		copy(id[:], oid)
		return s.metadata.Lookup(id)
	}
	var v [9]byte
	ok, e := s.table.Lookup(oid, v[:])
	return v[0], int64(binary.LittleEndian.Uint64(v[1:])), ok, e
}
func loadSourceInfo(ctx context.Context, tmp, source string, oidBytes int) (result *sourceInfo, retErr error) {
	if archiveImportEnabled(ctx) {
		s, used, err := loadPackSourceInfo(ctx, tmp, source, oidBytes)
		if used || err != nil {
			return s, err
		}
	}
	collectSizes := archiveImportEnabled(ctx)
	if collectSizes && oidBytes != 20 {
		return nil, fmt.Errorf("global size inventory requires SHA1")
	}
	streamingTrace("inventory_start", -1)
	r, e := streamGitOutput(ctx, tmp, source, "", "cat-file", "--batch-all-objects", "--unordered", "--batch-check=%(objectname) %(objecttype) %(objectsize)")
	if e != nil {
		return nil, e
	}
	defer r.Close()
	table, e := scratchmap.New(tmp, oidBytes, 9)
	if e != nil {
		return nil, e
	}
	s := &sourceInfo{table: table}
	defer func() {
		if retErr != nil {
			s.close()
		}
	}()
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 65536), 1024)
	key := make([]byte, oidBytes)
	var value [9]byte
	var rows int64
	for scan.Scan() {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		oid, rest, ok := bytes.Cut(scan.Bytes(), []byte{' '})
		if !ok || len(oid) != 2*oidBytes {
			return nil, fmt.Errorf("metadata OID")
		}
		kind, size, ok := bytes.Cut(rest, []byte{' '})
		if !ok {
			return nil, fmt.Errorf("metadata size")
		}
		switch string(kind) {
		case "commit":
			value[0] = 1
		case "tree":
			value[0] = 2
		case "blob":
			value[0] = 3
		case "tag":
			value[0] = 4
		default:
			return nil, fmt.Errorf("metadata kind")
		}
		if _, e = hex.Decode(key, oid); e != nil {
			return nil, e
		}
		n, e := strconv.ParseUint(string(size), 10, 63)
		if e != nil {
			return nil, e
		}
		if collectSizes && value[0] == 3 {
			if e = s.collectGlobalSize(key, int64(n)); e != nil {
				return nil, e
			}
		}
		binary.LittleEndian.PutUint64(value[1:], n)
		if e = table.Put(key, value[:]); e != nil {
			return nil, e
		}
		rows++
	}
	if e = scan.Err(); e != nil {
		return nil, e
	}
	streamingTrace("inventory_map_rows", rows)
	streamingTrace("inventory_end", rows)
	return s, nil
}

type typedWalk struct {
	source io.ReadCloser
	scan   *bufio.Scanner
	info   *sourceInfo
	buffer []byte
	offset int
	err    error
	key    []byte
}

func newTypedWalk(source io.ReadCloser, info *sourceInfo, oidBytes int) *typedWalk {
	scan := bufio.NewScanner(source)
	scan.Buffer(make([]byte, 65536), 1<<20)
	scan.Split(metadataLine)
	return &typedWalk{source: source, scan: scan, info: info, key: make([]byte, oidBytes)}
}
func (t *typedWalk) Close() error { return t.source.Close() }
func (t *typedWalk) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if t.offset == len(t.buffer) {
		if t.err != nil {
			return 0, t.err
		}
		t.buffer = t.buffer[:0]
		t.offset = 0
		for len(t.buffer) < 65536 && t.scan.Scan() {
			line := bytes.TrimSuffix(t.scan.Bytes(), []byte{'\n'})
			oid, hint, _ := bytes.Cut(line, []byte{' '})
			if len(oid) != 2*len(t.key) {
				t.err = fmt.Errorf("walk OID")
				break
			}
			if _, e := hex.Decode(t.key, oid); e != nil {
				t.err = e
				break
			}
			kind, size, ok, e := t.info.lookup(t.key)
			if e != nil || !ok {
				t.err = fmt.Errorf("walk metadata missing: %s: %v", oid, e)
				break
			}
			name := ""
			switch kind {
			case 1:
				name = "commit"
			case 2:
				name = "tree"
			case 3:
				name = "blob"
			case 4:
				name = "tag"
			default:
				t.err = fmt.Errorf("walk kind")
			}
			if t.err != nil {
				break
			}
			t.buffer = append(t.buffer, oid...)
			t.buffer = append(t.buffer, ' ')
			t.buffer = append(t.buffer, name...)
			t.buffer = append(t.buffer, ' ')
			t.buffer = strconv.AppendInt(t.buffer, size, 10)
			t.buffer = append(t.buffer, ' ')
			t.buffer = append(t.buffer, hint...)
			t.buffer = append(t.buffer, '\n')
		}
		if len(t.buffer) < 65536 && t.err == nil {
			t.err = t.scan.Err()
			if t.err == nil {
				t.err = io.EOF
			}
		}
	}
	if len(t.buffer) == 0 {
		return 0, t.err
	}
	n := copy(p, t.buffer[t.offset:])
	t.offset += n
	return n, nil
}
