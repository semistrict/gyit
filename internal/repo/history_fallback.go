package repo

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	pb "gyit/internal/gen/gyit/storage/v1"
)

// Compute only this query's comparison, without writing derived index data or
// requesting blobs. Missing objects remain a coverage gap, never end of history.
func (r *unindexedHistoryReader) load(ctx context.Context, sha string) (*historyIngestCommit, []byte, error) {
	firstParent := r.firstParent
	c, tree, err := r.commit(ctx, sha)
	if err != nil {
		return nil, nil, err
	}
	n := len(c.Parents)
	if firstParent && n > 1 {
		n = 1
	}
	mask := make([]byte, (max(1, n)+7)/8)
	c.ParentTimes = make([]int64, len(c.Parents))
	current, err := r.pathValue(ctx, tree)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		if current.OID != "" {
			mask[0] = 1
		}
		return c, mask, nil
	}
	for i := range n {
		parent, parentTree, err := r.commit(ctx, hex.EncodeToString(c.Parents[i]))
		if err != nil {
			return nil, nil, err
		}
		c.ParentTimes[i] = parent.metadata.CommitTime
		previous := current
		if parentTree != tree {
			previous, err = r.pathValue(ctx, parentTree)
			if err != nil {
				return nil, nil, err
			}
		}
		if previous.OID == current.OID && previous.Mode == current.Mode {
			// The walker follows only this first TREESAME parent. Later parents
			// cannot affect display or traversal, and need not be available yet.
			break
		}
		mask[i/8] |= 1 << uint(i%8)
	}
	return c, mask, nil
}

// An absent pathname is an empty value with no error. An absent tree object is
// an error: confusing those cases would invent changes at shallow boundaries.
func (p *Progressive) historyPathValue(ctx context.Context, tree, path string) (Entry, error) {
	if strings.HasSuffix(path, "/") {
		name := strings.TrimSuffix(path, "/")
		if name == "." {
			name = ""
		}
		e, err := p.historyPathValue(ctx, tree, name)
		if e.Mode != 0040000 {
			e = Entry{}
		}
		return e, err
	}
	entry := Entry{OID: tree, Mode: 0040000}
	if path == "" {
		return entry, nil
	}
	for _, name := range strings.Split(path, "/") {
		if err := ctx.Err(); err != nil {
			return Entry{}, err
		}
		if entry.Mode != 0040000 {
			return Entry{}, nil
		}
		raw, release, err := p.borrowObject(ctx, entry.OID)
		if err != nil {
			release()
			return Entry{}, err
		}
		if len(raw) == 0 || raw[0] != 2 {
			release()
			return Entry{}, fmt.Errorf("history expected tree")
		}
		entry = Entry{}
		data := raw[1:]
		for len(data) > 0 {
			child, e := takeHistoryTreeEntry(&data)
			if e != nil {
				release()
				return Entry{}, e
			}
			if bytes.Equal(child.name, []byte(name)) {
				entry = Entry{OID: hex.EncodeToString(child.oid), Mode: child.mode}
				break
			}
		}
		release()
		if entry.OID == "" {
			return Entry{}, nil
		}
	}
	return entry, nil
}

type unindexedHistoryDisplay struct {
	sha      string
	metadata *pb.CommitRecord
}

// Reuse the last parsed parent and path value when the walk proceeds to it.
// Retention is constant, independent of repository or traversal size.
type unindexedHistoryReader struct {
	p                     *Progressive
	path                  string
	firstParent           bool
	commitSHA, commitTree string
	parsed                *historyIngestCommit
	valueTree             string
	value                 Entry
}

func (r *unindexedHistoryReader) commit(ctx context.Context, sha string) (*historyIngestCommit, string, error) {
	if r.parsed != nil && r.commitSHA == sha {
		return r.parsed, r.commitTree, nil
	}
	c, tree, err := r.p.historyBatchHeader(ctx, sha)
	if err == nil {
		r.commitSHA, r.commitTree, r.parsed = sha, tree, c
	}
	return c, tree, err
}

// Traversal needs identity, parents, tree and committer time. Author/message
// parsing is deferred until a commit is actually emitted, just as it is for
// indexed history's separate display frames. Returned fields own their bytes.
func (p *Progressive) historyBatchHeader(ctx context.Context, sha string) (*historyIngestCommit, string, error) {
	raw, release, err := p.borrowObject(ctx, sha)
	defer release()
	if err != nil {
		return nil, "", err
	}
	if len(raw) == 0 || raw[0] != 1 {
		return nil, "", fmt.Errorf("history expected commit")
	}
	oid, err := hex.DecodeString(sha)
	if err != nil {
		return nil, "", err
	}
	c := &historyIngestCommit{HistoryBatchCommit: &pb.HistoryBatchCommit{Oid: oid}, metadata: &pb.CommitRecord{}}
	data := raw[1:]
	var tree string
	for {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		line, next, ok := bytes.Cut(data, []byte{'\n'})
		if !ok {
			return nil, "", io.ErrUnexpectedEOF
		}
		data = next
		if len(line) == 0 {
			break
		}
		key, value, _ := bytes.Cut(line, []byte{' '})
		switch string(key) {
		case "tree":
			tree = string(value)
			if !validProgressiveOID(tree) {
				return nil, "", fmt.Errorf("invalid history tree identity")
			}
		case "parent":
			if len(value) != 40 || len(c.Parents) >= maxLogParents {
				return nil, "", fmt.Errorf("history parent width or count")
			}
			id := make([]byte, 20)
			if _, err := hex.Decode(id, value); err != nil {
				return nil, "", err
			}
			c.Parents = append(c.Parents, id)
		case "committer":
			_, stamp, _, _, err := parseIdentity(nil, value[max(0, len(value)-256):], true)
			if err != nil {
				return nil, "", err
			}
			c.metadata.CommitTime = stamp
		}
	}
	if tree == "" {
		return nil, "", fmt.Errorf("commit missing tree")
	}
	return c, tree, nil
}

func (r *unindexedHistoryReader) pathValue(ctx context.Context, tree string) (Entry, error) {
	if r.valueTree == tree {
		return r.value, nil
	}
	value, err := r.p.historyPathValue(ctx, tree, r.path)
	if err == nil {
		r.valueTree, r.value = tree, value
	}
	return value, err
}
