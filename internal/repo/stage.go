package repo

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"gyit/internal/scratchmap"
	"gyit/internal/spill"
)

// Index records stream through a bounded sort. The mutex serializes writes
// from independent blob pipelines; each worker keeps its own packs and codec.
// Shared directory-page references
// require lookups during conversion. Only a legacy upgrade tracks commit
// presence, to avoid reading fresh commit metadata again while backfilling.
// These maps are private scratch files and never form part of a publication.
type stage struct {
	mu           sync.Mutex
	tmp          string
	records      *spill.Sorter
	commits      *scratchmap.Map
	directories  [16]directoryShard
	trackCommits bool
	direct       *directBlobStage
}

func (s *stage) put(key string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.direct != nil {
		consumed, err := s.direct.add(key, v)
		if consumed || err != nil {
			return err
		}
	}
	b, err := marshal(v)
	if err != nil {
		return err
	}
	if err := s.records.Add([]byte(key), b); err != nil {
		return err
	}
	if s.trackCommits && strings.HasPrefix(key, "c/") {
		oid, err := hex.DecodeString(key[2:])
		if err != nil {
			return err
		}
		if s.commits == nil {
			s.commits, err = scratchmap.New(s.tmp, len(oid), 1)
			if err != nil {
				return err
			}
		}
		return s.commits.Put(oid, []byte{1})
	}
	return nil
}

func (s *stage) hasCommit(sha string) (bool, error) {
	if s.commits == nil {
		return false, nil
	}
	oid, err := hex.DecodeString(sha)
	if err != nil {
		return false, err
	}
	var present [1]byte
	return s.commits.Lookup(oid, present[:])
}

// Generated directory pack names and bounded offsets make their protobuf
// references smaller than 128 bytes. The two-byte length prefixes padding only
// in this disposable table; the published page/reference protobuf is unchanged.
const stagedDirectoryRefBytes = 128

// Each shard owns its mapped table. Identical pages must resolve to one
// canonical reference even when several conversion workers finish together.
type directoryShard struct {
	mu    sync.Mutex
	table *scratchmap.Map
}

func (s *directoryShard) page(hash [32]byte) (pageRef, bool, error) {
	var ref pageRef
	if s.table == nil {
		return ref, false, nil
	}
	var value [2 + stagedDirectoryRefBytes]byte
	found, err := s.table.Lookup(hash[:], value[:])
	if err != nil || !found {
		return ref, found, err
	}
	n := int(binary.LittleEndian.Uint16(value[:2]))
	if n == 0 || n > stagedDirectoryRefBytes {
		return ref, false, fmt.Errorf("invalid staged directory reference")
	}
	err = unmarshal(value[2:2+n], &ref)
	return ref, true, err
}

func (s *stage) directoryPage(hash [32]byte) (pageRef, bool, error) {
	shard := &s.directories[hash[0]&15]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.page(hash)
}

// The caller prepares space in its private pack before entering this method.
// write only appends bytes; uploads never hold a directory registry lock.
func (s *stage) saveDirectoryPage(hash [32]byte, write func() (pageRef, error)) (pageRef, error) {
	shard := &s.directories[hash[0]&15]
	shard.mu.Lock()
	defer shard.mu.Unlock()
	ref, found, err := shard.page(hash)
	if err != nil || found {
		return ref, err
	}
	ref, err = write()
	if err != nil {
		return ref, err
	}
	data, err := marshal(ref)
	if err != nil {
		return ref, err
	}
	if len(data) > stagedDirectoryRefBytes {
		return ref, fmt.Errorf("directory reference exceeds staging limit")
	}
	if shard.table == nil {
		shard.table, err = scratchmap.New(s.tmp, 32, 2+stagedDirectoryRefBytes)
		if err != nil {
			return ref, err
		}
	}
	var value [2 + stagedDirectoryRefBytes]byte
	binary.LittleEndian.PutUint16(value[:2], uint16(len(data)))
	copy(value[2:], data)
	return ref, shard.table.Put(hash[:], value[:])
}

func (s *stage) close() error {
	var err error
	if s.commits != nil {
		err = s.commits.Close()
	}
	for i := range s.directories {
		if s.directories[i].table != nil {
			err = errors.Join(err, s.directories[i].table.Close())
		}
	}
	return err
}
