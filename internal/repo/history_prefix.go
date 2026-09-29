//go:build !js

package repo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

// RebuildHistoryPrefix regenerates an all-path traversal prefix from published
// acquisition objects. It repairs fragmented old metadata without walking its
// remote SHA index. Only the selected entry point changes: immutable parent
// links carry the new frames, and uncovered edges use the ordinary index.
// Existing coverage/frontiers and old publications remain unchanged.
//
// Private Store publications are staging data, not a second reader cache. The
// normal importer, frame linker, and shared bounded decoded cache are reused.
// All referenced immutable data is uploaded before one real HEAD CAS.
func (p *Progressive) RebuildHistoryPrefix(ctx context.Context, sha string, limit int, gitdirs ...string) (resultErr error) {
	if !validProgressiveOID(sha) || limit < 1 || limit > 65536 {
		return fmt.Errorf("history prefix requires a revision and 1..65536 commits")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case p.historyBuild <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.historyBuild }()
	// This is layout maintenance for an existing indexed entry, not an
	// independent source of coverage records or ingestion progress.
	var previous pb.HistoryBatchLocation
	if err := p.get(ctx, historyBatchKey+sha, &previous); err != nil {
		return err
	}
	if previous.Batch == nil {
		return fmt.Errorf("missing indexed history entry")
	}

	scratch, err := os.MkdirTemp(p.temp, "history-prefix-")
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, os.RemoveAll(scratch)) }()
	root := filepath.Join(scratch, "objects")
	local, err := store.NewLocal(root)
	if err != nil {
		return err
	}
	work, err := NewProgressive(ctx, local, nil, scratch)
	if err != nil {
		return err
	}
	work.cache = p.cache
	err = work.ingestHistoryInputBounded(ctx, []string{sha}, func() (*historySource, error) { return p.publishedHistorySource(ctx, gitdirs) }, nil, nil, uint64(limit))
	if err != nil && !errors.Is(err, ErrHistoryIndexPending) {
		return err
	}
	// A budget larger than the commit limit includes every private frame and
	// forces linking even when the prefix fits in one small container.
	if err := work.compactHistory(ctx, sha, uint64(limit)+1); err != nil {
		return err
	}
	var entry pb.HistoryBatchLocation
	if err := work.get(ctx, historyBatchKey+sha, &entry); err != nil {
		return err
	}
	if entry.Batch == nil || !strings.HasPrefix(entry.Batch.Pack, "index/"+compactHistoryGraphPrefix) {
		return fmt.Errorf("history prefix has no linked entry point")
	}
	uploads := newPublicationUploads(ctx, p.store)
	defer uploads.close()
	if err := uploadHistoryPrefix(ctx, local, root, uploads); err != nil {
		return err
	}
	if err := uploads.wait(); err != nil {
		return err
	}
	metadata := newPublicationUploads(ctx, p.store)
	defer metadata.close()
	p.writer.Lock()
	defer p.writer.Unlock()
	return p.stageHistory(func(stage *historyStage) error {
		if err := progressivePut(stage.buckets["changes"], historyBatchKey+sha, &entry); err != nil {
			return err
		}
		return p.publishIndex(ctx, metadata, func(idx *index) (pageRef, error) {
			idx.pageRanges = true
			return idx.update(ctx, stage.buckets["changes"])
		})
	})
}

// Iterate private Store files in fixed-size directory batches. Global lookup
// pages, lock files, and pre-compaction graph copies are not durable payloads.
func uploadHistoryPrefix(ctx context.Context, local store.Store, root string, uploads *publicationUploads) error {
	var visit func(string) error
	visit = func(key string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := os.Open(filepath.Join(root, filepath.FromSlash(key)))
		if err != nil {
			return err
		}
		defer f.Close()
		for {
			entries, readErr := f.ReadDir(128)
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			for _, entry := range entries {
				name := key + "/" + entry.Name()
				if key == "index" && !strings.HasPrefix(entry.Name(), "progressive-history-v2-") {
					continue
				}
				if strings.HasPrefix(name, "index/progressive-history-v2-graph-") && !strings.HasPrefix(name, "index/"+compactHistoryGraphPrefix) {
					continue
				}
				if entry.IsDir() {
					if err := visit(name); err != nil {
						return err
					}
					continue
				}
				if strings.HasSuffix(name, ".lock") {
					continue
				}
				info, err := entry.Info()
				if err != nil {
					return err
				}
				if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > indexPackSize {
					return fmt.Errorf("invalid staged history object")
				}
				data, _, err := local.Get(ctx, name, 0, -1)
				if err != nil {
					return err
				}
				if err := uploads.Put(ctx, name, data, ""); err != nil {
					return err
				}
			}
			if errors.Is(readErr, io.EOF) {
				return nil
			}
		}
	}
	return visit("index")
}
