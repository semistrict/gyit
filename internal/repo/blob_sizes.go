package repo

import (
	"encoding/binary"
	"fmt"

	"gyit/internal/scratchmap"
)

// Blob sizes are import-only lookup data. A fixed-width mapped table avoids
// rewriting random B-tree pages when scanning millions of object IDs.
type blobSizes struct {
	laneBytes  []uint64
	sourceInfo *sourceInfo
	records    int64
	parent     string
	table      *scratchmap.Map
}

func (s *blobSizes) put(oid []byte, size uint64) error {
	if int64(size) < 0 {
		return fmt.Errorf("invalid blob size")
	}
	if s.table == nil {
		var err error
		s.table, err = scratchmap.New(s.parent, len(oid), 8)
		if err != nil {
			return err
		}
	}
	var value [8]byte
	binary.LittleEndian.PutUint64(value[:], size)
	if err := s.table.Put(oid, value[:]); err != nil {
		return err
	}
	s.records++
	return nil
}

// Conversion starts after all sizes are inserted. Concurrent lookups only read
// the immutable mapping; the importer joins its workers before closing it.
func (s *blobSizes) get(oid []byte) (int64, bool, error) {
	if s.sourceInfo != nil {
		kind, size, found, e := s.sourceInfo.lookup(oid)
		return size, found && kind == 3, e
	}
	if s.table == nil {
		return 0, false, nil
	}
	var value [8]byte
	found, err := s.table.Lookup(oid, value[:])
	if err != nil || !found {
		return 0, found, err
	}
	return int64(binary.LittleEndian.Uint64(value[:])), true, nil
}

func (s *blobSizes) Close() error {
	if s.sourceInfo != nil {
		return s.sourceInfo.close()
	}
	if s.table == nil {
		return nil
	}
	return s.table.Close()
}
