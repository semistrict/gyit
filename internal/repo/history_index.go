package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"runtime"
	"strings"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/scratchmap"
	"gat/internal/spill"
	"google.golang.org/protobuf/proto"
)

const historyBlockSize = 256
const maxHistoryBlockBytes = 1 << 20
const historyBloomBytes = 64

type historyPosition uint64

func historyBlockKey(block uint64) string { return fmt.Sprintf("b/%016x", block) }
func pathProbes(path string) [4]uint16 {
	h := sha256.Sum256([]byte(path))
	var probes [4]uint16
	for i := range probes {
		probes[i] = binary.LittleEndian.Uint16(h[i*2:]) % (historyBloomBytes * 8)
	}
	return probes
}
func addChangedPath(filter []byte, path string) {
	for _, bit := range pathProbes(path) {
		filter[bit/8] |= 1 << (bit % 8)
	}
}
func mayChangePath(filter []byte, path string) bool {
	if len(filter) != historyBloomBytes {
		return true
	}
	for _, bit := range pathProbes(path) {
		if filter[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Import streams one first-parent raw diff per commit in parent-before-child
// order. Paths are NUL-delimited and never decoded as text. Bloom positives are
// conservative: root commits, type changes and merges still use exact lookups.
// Only new commit blocks are appended; already-published blocks are immutable.
func importHistory(ctx context.Context, source, input, tmp string, idx *index, base manifest) (pageRef, uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stdout, err := openHistoryStream(ctx, source, input, tmp, min(8, runtime.GOMAXPROCS(0)), historyDiffBatchSize)
	if err != nil {
		return pageRef{}, 0, err
	}
	defer stdout.Close()
	records, err := spill.New(tmp, 32<<20)
	if err != nil {
		return pageRef{}, 0, err
	}
	defer records.Close()
	var positions *scratchmap.Map
	defer func() {
		if positions != nil {
			positions.Close()
		}
	}()
	writer := &indexWriter{ctx: ctx, store: idx.store, prefix: rand.Text()}
	next := (base.HistoryCount+historyBlockSize-1)/historyBlockSize*historyBlockSize + 1
	count := base.HistoryCount
	block := &storagev1.HistoryBlock{}
	var current *storagev1.HistoryCommit
	put := func(key string, value any) error {
		b, e := marshal(value)
		if e != nil {
			return e
		}
		return records.Add([]byte(key), b)
	}
	flush := func() error {
		if len(block.Commits) == 0 {
			return nil
		}
		b, e := proto.MarshalOptions{Deterministic: true}.Marshal(block)
		if e != nil {
			return e
		}
		if len(b) > maxHistoryBlockBytes {
			return fmt.Errorf("history block exceeds size limit")
		}
		ref, e := writer.saveBytes(b)
		if e != nil {
			return e
		}
		if e = put(historyBlockKey((count-1)/historyBlockSize), ref); e != nil {
			return e
		}
		block = &storagev1.HistoryBlock{}
		return nil
	}
	finish := func() error {
		if current == nil {
			return nil
		}
		block.Commits = append(block.Commits, current)
		if err := put("g/"+hex.EncodeToString(current.Oid), historyPosition(next)); err != nil {
			return err
		}
		if positions == nil {
			var err error
			positions, err = scratchmap.New(tmp, len(current.Oid), 8)
			if err != nil {
				return err
			}
		}
		var value [8]byte
		binary.LittleEndian.PutUint64(value[:], next)
		if err := positions.Put(current.Oid, value[:]); err != nil {
			return err
		}
		count = next
		next++
		current = nil
		if len(block.Commits) == historyBlockSize {
			return flush()
		}
		return nil
	}
	scan := bufio.NewScanner(stdout)
	scan.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.IndexByte(data, 0); i >= 0 {
			return i + 1, data[:i], nil
		}
		if atEOF && len(data) > 0 {
			return 0, nil, fmt.Errorf("unterminated history token")
		}
		return 0, nil, nil
	})
	scan.Buffer(make([]byte, 4096), 1<<20)
	take := func() (string, error) {
		if !scan.Scan() {
			if e := scan.Err(); e != nil {
				return "", e
			}
			return "", io.ErrUnexpectedEOF
		}
		return scan.Text(), nil
	}
	for scan.Scan() {
		token := strings.TrimLeft(scan.Text(), "\n")
		if token == "" {
			continue
		}
		if strings.HasPrefix(token, ":") {
			fields := strings.Fields(token)
			if current == nil || len(fields) != 5 || len(fields[4]) != 1 || !strings.Contains("AMDT", fields[4]) {
				return pageRef{}, 0, fmt.Errorf("invalid raw history diff")
			}
			path, e := take()
			if e != nil {
				return pageRef{}, 0, e
			}
			// A file replaced by a directory changes descendants too (emitted by -r).
			addChangedPath(current.ChangedPaths, path)
			continue
		}
		if err := finish(); err != nil {
			return pageRef{}, 0, err
		}
		oid, e := hex.DecodeString(token)
		if e != nil || (len(oid) != 20 && len(oid) != 32) {
			return pageRef{}, 0, fmt.Errorf("invalid history commit ID")
		}
		treeText, e := take()
		if e != nil {
			return pageRef{}, 0, e
		}
		tree, e := hex.DecodeString(treeText)
		if e != nil || len(tree) != len(oid) {
			return pageRef{}, 0, fmt.Errorf("invalid history tree ID")
		}
		parentText, e := take()
		if e != nil {
			return pageRef{}, 0, e
		}
		parentIDs := strings.Fields(parentText)
		if len(parentIDs) > maxLogParents {
			return pageRef{}, 0, fmt.Errorf("commit exceeds bounded parent limit")
		}
		current = &storagev1.HistoryCommit{Oid: oid, Tree: tree, ChangedPaths: make([]byte, historyBloomBytes)}
		for _, sha := range parentIDs {
			var position historyPosition
			parentOID, err := hex.DecodeString(sha)
			if err != nil || len(parentOID) != len(oid) {
				return pageRef{}, 0, fmt.Errorf("invalid history parent ID")
			}
			var value [8]byte
			found := false
			if positions != nil {
				found, e = positions.Lookup(parentOID, value[:])
			}
			if e == nil {
				if found {
					position = historyPosition(binary.LittleEndian.Uint64(value[:]))
				} else {
					e = idx.get(ctx, "g/"+sha, &position)
				}
			}
			if e != nil {
				return pageRef{}, 0, fmt.Errorf("history parent %s: %w", sha, e)
			}
			if position == 0 || uint64(position) >= next {
				return pageRef{}, 0, fmt.Errorf("invalid topological history parent")
			}
			current.Parents = append(current.Parents, uint64(position))
		}
	}
	if err := scan.Err(); err != nil {
		return pageRef{}, 0, err
	}
	if err := finish(); err != nil {
		return pageRef{}, 0, err
	}
	if err := stdout.Close(); err != nil {
		return pageRef{}, 0, err
	}
	if err := flush(); err != nil {
		return pageRef{}, 0, err
	}
	if err := writer.flush(); err != nil {
		return pageRef{}, 0, err
	}
	root, err := idx.updateSorted(ctx, records)
	return root, count, err
}

// Each blame retains one decoded history block as a bounded work buffer. Encoded
// blocks use the existing shared read cache. No whole graph or checkout is read.
type historyCursor struct {
	idx     *index
	block   uint64
	commits []*storagev1.HistoryCommit
}

func (c *historyCursor) get(ctx context.Context, position uint64) (*storagev1.HistoryCommit, error) {
	if position == 0 {
		return nil, fmt.Errorf("invalid zero history position")
	}
	block := (position - 1) / historyBlockSize
	if c.commits == nil || c.block != block {
		var ref pageRef
		if err := c.idx.get(ctx, historyBlockKey(block), &ref); err != nil {
			return nil, err
		}
		if ref.Length > maxHistoryBlockBytes {
			return nil, fmt.Errorf("oversized history block")
		}
		b, err := c.idx.pageBytes(ctx, ref)
		if err != nil {
			return nil, err
		}
		var p storagev1.HistoryBlock
		if err := proto.Unmarshal(b, &p); err != nil {
			return nil, err
		}
		if len(p.Commits) == 0 || len(p.Commits) > historyBlockSize {
			return nil, fmt.Errorf("invalid history block size")
		}
		for i, n := range p.Commits {
			if (len(n.Oid) != 20 && len(n.Oid) != 32) || len(n.Tree) != len(n.Oid) || len(n.ChangedPaths) != historyBloomBytes || len(n.Parents) > maxLogParents {
				return nil, fmt.Errorf("invalid history commit")
			}
			for _, parent := range n.Parents {
				if parent == 0 || parent >= block*historyBlockSize+uint64(i)+1 {
					return nil, fmt.Errorf("invalid history parent")
				}
			}
		}
		c.commits = p.Commits
		c.block = block
	}
	offset := (position - 1) % historyBlockSize
	if offset >= uint64(len(c.commits)) {
		return nil, fmt.Errorf("history position outside block")
	}
	return c.commits[offset], nil
}
