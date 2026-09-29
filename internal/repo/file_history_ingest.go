//go:build !js

package repo

import (
	"context"
	"errors"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"gyit/internal/spill"
	"os"
)

// IngestHistory indexes all paths in bounded, newest-first batches. Queries
// never create index data. Previously published coverage survives interruption.
func (p *Progressive) IngestHistory(ctx context.Context, sha string, gitdirs ...string) error {
	return p.ingestHistoryTips(ctx, []string{sha}, gitdirs...)
}

// ImportHistoryPacks prepares history while the same immutable acquisition
// packs are imported. Their recipes join the first history publication, after
// all referenced data is durable. The caller keeps gitdir stable until return.
func (p *Progressive) ImportHistoryPacks(ctx context.Context, sha, gitdir string) error {
	if !validProgressiveOID(sha) {
		return fmt.Errorf("invalid history revision")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	packRecords, err := spill.New(p.temp, 1<<20)
	if err != nil {
		return err
	}
	done := make(chan struct{})
	var importErr error
	var changed bool
	go func() {
		defer close(done)
		changed, importErr = p.stagePackRecords(ctx, gitdir, packRecords.Add)
		if importErr != nil {
			cancel()
		}
	}()
	defer func() { cancel(); <-done; packRecords.Close() }()
	ready := func(ctx context.Context) error {
		select {
		case <-done:
			return importErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Publication workers run in order and ingestion joins the final worker
	// before returning. Only the first needs to include the staged pack records.
	included := false
	include := func(ctx context.Context, records *spill.Sorter) error {
		if included {
			return nil
		}
		if err := ready(ctx); err != nil {
			return err
		}
		// Both stages use the same bounded sort budget and filesystem. Move
		// ownership of the existing runs instead of copying and sorting every
		// pack recipe a second time before the first publication.
		if err := records.Take(packRecords); err != nil {
			return err
		}
		included = true
		return nil
	}
	err = p.ingestHistoryInput(ctx, []string{sha}, func() (*historySource, error) {
		return openHistorySource([]string{gitdir})
	}, include, done)
	if err != nil && !errors.Is(err, ErrHistoryIndexPending) {
		cancel()
		<-done
		if importErr != nil && !errors.Is(importErr, context.Canceled) {
			return errors.Join(importErr, err)
		}
		return err
	}
	// A completed tip or a depth-one input can produce no history publication.
	// The explicit import contract still requires its packs to become durable.
	if waitErr := ready(ctx); waitErr != nil {
		cancel()
		<-done
		if importErr != nil {
			return importErr
		}
		return waitErr
	}
	if !included && changed {
		// No history batch was emitted (for example, depth one or a covered
		// tip). Preserve ImportPacks' contract without inventing coverage.
		metadata := newPublicationUploads(ctx, p.store)
		defer metadata.close()
		p.writer.Lock()
		publishErr := p.publishIndex(ctx, metadata, func(idx *index) (pageRef, error) { return idx.updateSorted(ctx, packRecords) })
		p.writer.Unlock()
		if publishErr != nil {
			return publishErr
		}
	}
	if err != nil {
		return err
	}
	return p.CompactHistory(ctx, sha)
}
func (p *Progressive) ingestHistoryTips(ctx context.Context, tips []string, gitdirs ...string) error {
	if err := p.ingestHistoryBatches(ctx, tips, gitdirs...); err != nil {
		return err
	}
	for _, tip := range tips {
		if err := p.CompactHistory(ctx, tip); err != nil {
			return err
		}
	}
	return nil
}

type historyStage struct {
	db      *bolt.DB
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
}

func (p *Progressive) stageHistory(fn func(*historyStage) error) error {
	f, err := os.CreateTemp(p.temp, "history-*.db")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	defer os.Remove(name)
	db, err := bolt.Open(name, 0600, &bolt.Options{NoSync: true})
	if err != nil {
		return err
	}
	defer db.Close()
	stage := &historyStage{db: db, buckets: map[string]*bolt.Bucket{}}
	stage.tx, err = db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if stage.tx != nil {
			_ = stage.tx.Rollback()
		}
	}()
	for _, name := range []string{"changes", "queue", "missing", "seen"} {
		stage.buckets[name], err = stage.tx.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
	}
	return fn(stage)
}
func (s *historyStage) checkpoint() error {
	if err := s.tx.Commit(); err != nil {
		return err
	}
	s.tx = nil
	var err error
	s.tx, err = s.db.Begin(true)
	if err != nil {
		return err
	}
	for name := range s.buckets {
		s.buckets[name] = s.tx.Bucket([]byte(name))
	}
	return nil
}
