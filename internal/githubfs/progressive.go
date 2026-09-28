package githubfs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/repo"
	"gyit/internal/store"
)

const progressiveDirectory = "repositories-progressive-v1"

type progressiveRepository struct {
	backend           store.Store
	once              sync.Once
	err               error
	reader            *repo.Progressive
	source            string
	historySource     string
	historyOperations chan struct{}
	remote            string
	operations        chan struct{}
	background        sync.Map
}

func (p *progressiveRepository) lock(ctx context.Context) error {
	select {
	case p.operations <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *progressiveRepository) unlock() { <-p.operations }

func (f *FS) progressiveRepository(ctx context.Context, t Target) (*progressiveRepository, error) {
	key := t.Owner + "/" + t.Repository
	f.mu.Lock()
	if f.progressiveRepos == nil {
		f.progressiveRepos = make(map[string]*progressiveRepository)
	}
	p := f.progressiveRepos[key]
	if p == nil {
		p = &progressiveRepository{operations: make(chan struct{}, 1), historyOperations: make(chan struct{}, 1)}
		f.progressiveRepos[key] = p
	}
	f.mu.Unlock()
	p.once.Do(func() {
		dir := filepath.Join(f.opts.DataDir, progressiveDirectory, storeID(t, ""))
		p.source = filepath.Join(dir, "acquisition.git")
		p.remote = strings.TrimRight(f.opts.RemoteBase, "/") + "/" + t.Owner + "/" + t.Repository + ".git"
		if p.err = os.MkdirAll(p.source, 0700); p.err != nil {
			return
		}
		if out, err := f.git(ctx, p.source, "init", "--bare", "--quiet").CombinedOutput(); err != nil {
			p.err = fmt.Errorf("initialize acquisition: %w: %s", err, out)
			return
		}
		for _, kv := range [][2]string{{"remote.origin.url", p.remote}, {"remote.origin.promisor", "true"}, {"remote.origin.partialclonefilter", "blob:none"}, {"gc.auto", "0"}, {"maintenance.auto", "false"}} {
			if out, err := f.git(ctx, p.source, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
				p.err = fmt.Errorf("configure acquisition: %w: %s", err, out)
				return
			}
		}
		location := filepath.Join(dir, "objects")
		if f.opts.StoreRoot != "" {
			location = strings.TrimRight(f.opts.StoreRoot, "/") + "/" + storeID(t, "")
		}
		backend, err := store.Open(ctx, location, "", "")
		if err != nil {
			p.err = err
			return
		}
		p.backend = backend
		p.reader, p.err = repo.NewProgressive(ctx, backend, f.cache, dir)
		if p.err != nil {
			return
		}

		// Keep foreground commit acquisition independent of long background fetches.
		// Both lanes publish into the same immutable pool through its single writer.
		p.historySource = filepath.Join(dir, "history-demand.git")
		if out, err := f.git(ctx, "", "init", "--bare", "--quiet", p.historySource).CombinedOutput(); err != nil {
			p.err = fmt.Errorf("initialize history acquisition: %w: %s", err, out)
			return
		}
		for _, kv := range [][2]string{{"remote.origin.url", p.remote}, {"remote.origin.promisor", "true"}, {"remote.origin.partialclonefilter", "tree:0"}, {"gc.auto", "0"}, {"maintenance.auto", "false"}} {
			if out, err := f.git(ctx, p.historySource, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
				p.err = fmt.Errorf("configure history acquisition: %w: %s", err, out)
				return
			}
		}
		p.reader.DemandCommits = func(ctx context.Context, oids []string, depth int) error {
			select {
			case p.historyOperations <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-p.historyOperations }()
			var missing []string
			for _, oid := range oids {
				if _, err := p.reader.ObjectSize(ctx, oid); errors.Is(err, store.ErrNotFound) {
					missing = append(missing, oid)
				} else if err != nil {
					return err
				}
			}
			if len(missing) == 0 {
				return nil
			}
			cmd := f.git(ctx, p.historySource, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--no-write-fetch-head", "--filter=tree:0", fmt.Sprintf("--depth=%d", max(1, min(depth, 64))), "--stdin", "origin")
			cmd.Stdin = strings.NewReader(strings.Join(missing, "\n") + "\n")
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("fetch requested history: %w: %s", err, f.redact(string(out)))
			}
			return p.reader.ImportPacks(ctx, p.historySource)
		}
		p.reader.ResolveRevision = func(ctx context.Context, name string) (string, error) {
			target := t
			target.Revision = name
			return f.resolve(ctx, p.remote, target)
		}
		p.reader.Demand = func(ctx context.Context, oids []string) error {
			if err := p.lock(ctx); err != nil {
				return err
			}
			defer p.unlock()
			missing := make([]string, 0, len(oids))
			for _, oid := range oids {
				if _, err := p.reader.ObjectSize(ctx, oid); errors.Is(err, store.ErrNotFound) {
					missing = append(missing, oid)
				} else if err != nil {
					return err
				}
			}
			if len(missing) == 0 {
				return nil
			}
			cmd := f.git(ctx, p.source, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--no-write-fetch-head", "--filter=blob:none", "--stdin", "origin")
			cmd.Stdin = strings.NewReader(strings.Join(missing, "\n") + "\n")
			if out, err := cmd.CombinedOutput(); err != nil {
				return fmt.Errorf("fetch requested files: %w: %s", err, f.redact(string(out)))
			}
			return p.reader.ImportPacks(ctx, p.source)
		}
	})
	return p, p.err
}
func (f *FS) prepareProgressive(ctx context.Context, t Target, progress func(string)) (*progressiveRepository, *repo.Snapshot, error) {
	p, err := f.progressiveRepository(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	progress("Resolving requested revision…")
	sha, err := f.resolve(ctx, p.remote, t)
	if err != nil {
		return nil, nil, err
	}
	if _, err = p.reader.ObjectSize(ctx, sha); errors.Is(err, store.ErrNotFound) {
		if err = p.lock(ctx); err != nil {
			return nil, nil, err
		}
		progress("Fetching current snapshot trees (depth one, blobless)…")
		cmd := f.git(ctx, p.source, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--depth=1", "--filter=blob:none", "origin", sha)
		out, e := cmd.CombinedOutput()
		if e == nil {
			e = p.reader.ImportPacks(ctx, p.source)
		}
		p.unlock()
		if e != nil {
			return nil, nil, fmt.Errorf("fetch snapshot trees: %w: %s", e, f.redact(string(out)))
		}
	} else if err != nil {
		return nil, nil, err
	}
	s, err := p.reader.Open(ctx, sha)
	if err != nil {
		return nil, nil, err
	}
	progress("Preparing root attributes…")
	// Fetch root blobs as a batch for exact stat, without any REST size dependency.
	if _, err = s.ReadDir(ctx, s.Tree, "", 128); err != nil {
		return nil, nil, err
	}
	return p, s, nil
}
func (f *FS) startProgressiveBackground(p *progressiveRepository, s *repo.Snapshot, j *job) {
	if _, loaded := p.background.LoadOrStore(s.SHA, true); loaded {
		return
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.wg.Add(1)
	f.mu.Unlock()
	go func() {
		defer f.wg.Done()
		state := &pb.ProgressiveState{}
		if old, err := p.reader.State(f.ctx, s.SHA); err == nil {
			state = old
		}
		report := func(message string) {
			j.progress(message)
			state.Progress = message
			_ = p.reader.SetState(f.ctx, s.SHA, state)
		}
		fail := func(err error) {
			if f.ctx.Err() == nil {
				report("Background import paused: " + f.redact(err.Error()))
			}
			p.background.Delete(s.SHA)
		}
		if !state.SnapshotComplete {
			report("Downloading current snapshot contents; history follows in background.")
			prepared, err := p.reader.HasPreparedSnapshot(f.ctx)
			if err != nil {
				fail(err)
				return
			}
			if !prepared {
				if err := p.lock(f.ctx); err != nil {
					return
				}
				cmd := f.git(f.ctx, p.source, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--depth=1", "--refetch", "--no-filter", "origin", s.SHA)
				out, fetchErr := cmd.CombinedOutput()
				if fetchErr == nil {
					fetchErr = p.reader.ImportPacks(f.ctx, p.source)
				}
				p.unlock()
				if fetchErr != nil {
					fail(fmt.Errorf("fetch snapshot contents: %w: %s", fetchErr, f.redact(string(out))))
					return
				}
			}
			// Subsequent snapshots use tree identity to find only missing blobs;
			// PrepareSnapshot coalesces them before building changed pages.

			report("Preparing complete filesystem metadata…")
			if err = p.reader.PrepareSnapshot(f.ctx, s.SHA); err != nil {
				fail(err)
				return
			}
			state.SnapshotComplete = true
			report("Current snapshot ready; acquiring full commit and tree history…")
		}
		if state.HistoryComplete {
			if err := p.reader.IngestHistory(f.ctx, s.SHA, p.source, p.historySource); err != nil {
				fail(err)
			}
			return
		}
		// Deepen in resumable batches, yielding the acquisition lock between
		// them so a new foreground revision can make progress.
		for deepen := 64; ; deepen = min(deepen*2, 2048) {
			if err := p.lock(f.ctx); err != nil {
				return
			}
			args := []string{"fetch", "--quiet", "--keep", "--no-auto-maintenance", "--filter=blob:none", "--tags", "--force", fmt.Sprintf("--deepen=%d", deepen), "origin", s.SHA, "+refs/heads/*:refs/heads/*"}
			out, err := f.git(f.ctx, p.source, args...).CombinedOutput()
			if err == nil {
				err = p.reader.ImportPacks(f.ctx, p.source)
			}
			var shallow []byte
			if err == nil {
				shallow, err = f.git(f.ctx, p.source, "rev-parse", "--is-shallow-repository").Output()
			}
			p.unlock()
			if err != nil {
				fail(fmt.Errorf("fetch background history: %w: %s", err, f.redact(string(out))))
				return
			}
			if strings.TrimSpace(string(shallow)) == "false" {
				break
			}
			report(fmt.Sprintf("Current snapshot ready; importing history in batches of %d generations…", deepen))
			select {
			case <-f.ctx.Done():
				return
			case <-time.After(250 * time.Millisecond):
			}
		}
		report("Indexing file history…")
		if err := p.reader.IngestHistory(f.ctx, s.SHA, p.source, p.historySource); err != nil {
			fail(err)
			return
		}
		state.HistoryComplete = true
		report("Current snapshot and full commit/tree history ready. Historical file contents remain available on demand.")
	}()
}
