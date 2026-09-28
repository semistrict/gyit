package repo

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"gyit/internal/store"
)

// HistoryRepository translates existing history queries into reads of the same
// immutable Git packs. It does not create another commit catalog or clone.
func (p *Progressive) HistoryRepository() *Repository {
	return &Repository{progressive: p, store: p.store, cache: p.cache, borrowedDisk: true}
}
func (p *Progressive) historyIndex() *index {
	return &index{progressive: p, store: p.store, cache: p.cache}
}
func isHexRevision(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range strings.ToLower(s) {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// commitFetchDepth is a query-local prefetch hint, never a traversal boundary.
// Missing parents beyond the fetched slice still trigger another acquisition.
type commitFetchDepth struct{}

func (p *Progressive) ensureCommit(ctx context.Context, oid string) error {
	if p.DemandCommits == nil {
		return p.Ensure(ctx, []string{oid})
	}
	if _, err := p.ObjectSize(ctx, oid); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	depth, _ := ctx.Value(commitFetchDepth{}).(int)
	return p.DemandCommits(ctx, []string{oid}, max(1, min(depth, 64)))
}

func (p *Progressive) historyRecord(ctx context.Context, key string, out any) error {
	prefix, oid, ok := strings.Cut(key, "/")
	if !ok || (prefix != "c" && prefix != "p" && prefix != "o") {
		return store.ErrNotFound
	}
	ensure := p.Ensure
	if prefix == "c" || prefix == "p" {
		ensure = func(ctx context.Context, ids []string) error { return p.ensureCommit(ctx, ids[0]) }
	}
	if err := ensure(ctx, []string{oid}); err != nil {
		return fmt.Errorf("acquire history object %s: %w", oid, err)
	}
	raw, kind, err := p.object(ctx, oid)
	if err != nil {
		return err
	}
	var tree string
	var info commitInfo
	var ids []string
	if kind == 1 {
		tree, info, err = parseBufferedCommitParents(bufio.NewReader(bytes.NewReader(raw)), &ids, 20)
		if err != nil {
			return err
		}
	}
	switch v := out.(type) {
	case *commitInfo:
		if kind != 1 || prefix != "c" {
			return fmt.Errorf("expected commit metadata")
		}
		*v = info
	case *parents:
		if kind != 1 || prefix != "p" {
			return fmt.Errorf("expected commit parents")
		}
		v.Parents = ids
	case *object:
		if prefix != "o" {
			return store.ErrNotFound
		}
		kinds := map[byte]string{1: "commit", 2: "tree", 3: "blob", 4: "tag"}
		*v = object{Kind: kinds[kind], Size: int64(len(raw)), Tree: tree}
	default:
		return fmt.Errorf("unsupported progressive history record %q", prefix)
	}
	return nil
}
func (p *Progressive) historyScan(ctx context.Context, prefix, after string, limit int) ([]item, error) {
	if !strings.HasPrefix(prefix, "o/") {
		return nil, nil
	}
	if after != "" {
		after = "g/" + strings.TrimPrefix(after, "o/")
	}
	items, err := p.index().scan(ctx, "g/"+strings.TrimPrefix(prefix, "o/"), after, limit)
	if err != nil {
		return nil, err
	}
	for i := range items {
		items[i].Key = "o/" + strings.TrimPrefix(items[i].Key, "g/")
		// Consumers use these scans only to enumerate candidate object identities.
		items[i].Value = nil
	}
	return items, nil
}
