package repo

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
)

type databaseSelectionCounts struct{ Commits, Trees, Blobs int64 }

// A selection owns both input streams. All reachable commit rows precede the
// all-local tree/blob rows, preserving enumerated parents, including shallow
// boundaries. Inventory commit/tag rows are never published by this selector.
// Payload hints are intentionally empty; this changes compression families.
type databaseSelection struct {
	commits, inventory  io.ReadCloser
	scan                *bufio.Scanner
	info                *sourceInfo
	oidBytes            int
	key                 []byte
	buffer              []byte
	offset              int
	inInventory         bool
	includeLocalCommits bool
	err                 error
	count               databaseSelectionCounts
	closed              bool
	closeOnce           sync.Once
	closeErr            error
}

func newDatabaseSelection(commits io.ReadCloser, inventory io.ReadCloser, info *sourceInfo, oidBytes int) *databaseSelection {
	s := &databaseSelection{commits: commits, inventory: inventory, info: info, oidBytes: oidBytes, key: make([]byte, oidBytes)}
	s.scan = bufio.NewScanner(commits)
	s.scan.Buffer(make([]byte, 64<<10), 1<<20)
	s.scan.Split(metadataLine)
	return s
}

// No commit walk exists in this mode: the only owned input is the native
// inventory stream. Raw parents are staged by the existing metadata workers.
func newAllLocalSelection(inventory io.ReadCloser, info *sourceInfo, oidBytes int) *databaseSelection {
	s := &databaseSelection{inventory: inventory, info: info, oidBytes: oidBytes, key: make([]byte, oidBytes), inInventory: true, includeLocalCommits: true}
	s.scan = bufio.NewScanner(inventory)
	s.scan.Buffer(make([]byte, 64<<10), 1024)
	return s
}

func (s *databaseSelection) counts() databaseSelectionCounts { return s.count }

func (s *databaseSelection) traceCounts() {
	streamingTrace("whole_database_commit_rows", s.count.Commits)
	streamingTrace("whole_database_tree_rows", s.count.Trees)
	streamingTrace("whole_database_blob_rows", s.count.Blobs)
	if s.includeLocalCommits {
		streamingTrace("all_local_commit_rows", s.count.Commits)
	}
}

func (s *databaseSelection) oid(raw []byte) error {
	if len(raw) != s.oidBytes*2 {
		return fmt.Errorf("whole-database selection OID width")
	}
	_, err := hex.Decode(s.key, raw)
	return err
}

func (s *databaseSelection) commit(line []byte) error {
	oid, parents, _ := bytes.Cut(line, []byte{' '})
	if err := s.oid(oid); err != nil {
		return err
	}
	kind, size, found, err := s.info.lookup(s.key)
	if err != nil {
		return err
	}
	if !found || kind != 1 || size < 0 {
		return fmt.Errorf("whole-database reachable commit metadata missing or wrong kind: %s", oid)
	}
	// Parent validation remains in prepareImportMetadataWithNative; preserve the
	// exact commit-only traversal result rather than reading raw parent headers.
	s.buffer = append(s.buffer, oid...)
	s.buffer = append(s.buffer, " commit "...)
	s.buffer = strconv.AppendInt(s.buffer, size, 10)
	s.buffer = append(s.buffer, ' ')
	s.buffer = append(s.buffer, parents...)
	s.buffer = append(s.buffer, '\n')
	s.count.Commits++
	return nil
}

func (s *databaseSelection) local(line []byte) error {
	oid, rest, ok := bytes.Cut(line, []byte{' '})
	if !ok {
		return fmt.Errorf("whole-database inventory row")
	}
	kind, size, ok := bytes.Cut(rest, []byte{' '})
	if !ok {
		return fmt.Errorf("whole-database inventory size")
	}
	if err := s.oid(oid); err != nil {
		return err
	}
	if _, err := strconv.ParseUint(string(size), 10, 63); err != nil {
		return err
	}
	switch string(kind) {
	case "commit":
		if !s.includeLocalCommits {
			return nil
		}
		s.count.Commits++
	case "tag":
		return nil
	case "tree":
		s.count.Trees++
	case "blob":
		s.count.Blobs++
	default:
		return fmt.Errorf("whole-database inventory kind")
	}
	s.buffer = append(s.buffer, line...)
	s.buffer = append(s.buffer, ' ', '\n')
	return nil
}

func (s *databaseSelection) Read(p []byte) (int, error) {
	if s.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.offset == len(s.buffer) {
		if s.err != nil {
			return 0, s.err
		}
		s.buffer = s.buffer[:0]
		s.offset = 0
		for len(s.buffer) < 64<<10 {
			if s.scan.Scan() {
				line := bytes.TrimSuffix(s.scan.Bytes(), []byte{'\n'})
				if s.inInventory {
					s.err = s.local(line)
				} else {
					s.err = s.commit(line)
				}
				if s.err != nil {
					break
				}
				continue
			}
			if err := s.scan.Err(); err != nil {
				s.err = err
				break
			}
			if s.inInventory {
				s.err = io.EOF
				break
			}
			if s.inventory == nil {
				s.err = fmt.Errorf("whole-database inventory stream missing")
				break
			}
			s.inInventory = true
			s.scan = bufio.NewScanner(s.inventory)
			s.scan.Buffer(make([]byte, 64<<10), 1024)
		}
	}
	if len(s.buffer) == 0 {
		return 0, s.err
	}
	n := copy(p, s.buffer[s.offset:])
	s.offset += n
	return n, nil
}

func (s *databaseSelection) Close() error {
	s.closeOnce.Do(func() {
		s.closed = true
		if s.commits != nil {
			s.closeErr = s.commits.Close()
		}
		if s.inventory != nil {
			s.closeErr = errors.Join(s.closeErr, s.inventory.Close())
		}
	})
	return s.closeErr
}
