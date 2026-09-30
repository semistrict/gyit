package githubfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sync/singleflight"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/repo"
	"gyit/internal/store"
)

const progressiveDirectory = "repositories-progressive-v1"

// A lane serializes one kind of acquisition against one Git source.
type lane chan struct{}

func newLane() lane { return make(lane, 1) }
func (l lane) acquire(ctx context.Context) error {
	select {
	case l <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (l lane) release() { <-l }

// Shared setup belongs to the filesystem lifetime, never to the request that
// starts it: that caller may give up waiting, and a failure is retried later.
// Fields assigned by setup are read only after ready.
type progressiveRepository struct {
	setup          singleflight.Group
	ready          atomic.Bool
	backend        store.Store
	reader         *repo.Progressive
	source         string
	historySource  string
	ancestrySource string
	earlyMu        sync.Mutex
	earlyAncestry  *ancestryAcquisition
	snapshotSource string
	historyDemands singleflight.Group
	remote         string
	background     sync.Map

	// Lanes are independent so a long transfer in one never stalls another.
	// source has two: blob fetches leave its shallow boundary alone, while Git
	// allows only one fetch at a time to move it (shallow.lock).
	blobOperations     lane
	shallowOperations  lane
	historyOperations  lane
	snapshotOperations lane
	ancestryOperations lane
}

func (f *FS) progressiveRepository(ctx context.Context, t Target) (*progressiveRepository, error) {
	key := t.Owner + "/" + t.Repository
	f.mu.Lock()
	if f.progressiveRepos == nil {
		f.progressiveRepos = make(map[string]*progressiveRepository)
	}
	p := f.progressiveRepos[key]
	if p == nil {
		p = &progressiveRepository{blobOperations: newLane(), shallowOperations: newLane(), historyOperations: newLane(), snapshotOperations: newLane(), ancestryOperations: newLane()}
		f.progressiveRepos[key] = p
	}
	f.mu.Unlock()
	if p.ready.Load() {
		return p, nil
	}
	result := p.setup.DoChan("", func() (any, error) {
		if p.ready.Load() {
			return nil, nil
		}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			return nil, context.Canceled
		}
		f.wg.Add(1)
		f.mu.Unlock()
		defer f.wg.Done()
		if err := f.setupProgressive(f.ctx, p, t); err != nil {
			return nil, err
		}
		p.ready.Store(true)
		return nil, nil
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-result:
		if r.Err != nil {
			return nil, r.Err
		}
		return p, nil
	}
}

func closeStore(s store.Store) error {
	if c, ok := s.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// setupProgressive is idempotent, so a failed attempt is simply repeated.
func (f *FS) setupProgressive(ctx context.Context, p *progressiveRepository, t Target) error {
	dir := filepath.Join(f.opts.DataDir, progressiveDirectory, storeID(t, ""))
	p.source = filepath.Join(dir, "acquisition.git")
	p.remote = strings.TrimRight(f.opts.RemoteBase, "/") + "/" + t.Owner + "/" + t.Repository + ".git"
	if err := os.MkdirAll(p.source, 0700); err != nil {
		return err
	}
	if out, err := f.git(ctx, p.source, "init", "--bare", "--quiet").CombinedOutput(); err != nil {
		return fmt.Errorf("initialize acquisition: %w: %s", err, out)
	}
	for _, kv := range [][2]string{{"remote.origin.url", p.remote}, {"remote.origin.promisor", "true"}, {"remote.origin.partialclonefilter", "blob:none"}, {"gc.auto", "0"}, {"maintenance.auto", "false"}} {
		if out, err := f.git(ctx, p.source, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
			return fmt.Errorf("configure acquisition: %w: %s", err, out)
		}
	}
	// A retry keeps the reader opened by an earlier, partly failed attempt.
	if p.reader == nil {
		location := filepath.Join(dir, "objects")
		if f.opts.StoreRoot != "" {
			location = strings.TrimRight(f.opts.StoreRoot, "/") + "/" + storeID(t, "")
		}
		backend, err := store.Open(ctx, location, "", "")
		if err != nil {
			return err
		}
		reader, err := repo.NewProgressive(ctx, backend, f.cache, dir)
		if err != nil {
			return errors.Join(err, closeStore(backend))
		}
		p.backend, p.reader = backend, reader
	}

	// Keep foreground commit acquisition independent of long background fetches.
	// Both lanes publish into the same immutable pool through its single writer.
	p.historySource = filepath.Join(dir, "history-demand.git")
	if out, err := f.git(ctx, "", "init", "--bare", "--quiet", p.historySource).CombinedOutput(); err != nil {
		return fmt.Errorf("initialize history acquisition: %w: %s", err, out)
	}
	for _, kv := range [][2]string{{"remote.origin.url", p.remote}, {"remote.origin.promisor", "true"}, {"remote.origin.partialclonefilter", "tree:0"}, {"gc.auto", "0"}, {"maintenance.auto", "false"}} {
		if out, err := f.git(ctx, p.historySource, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
			return fmt.Errorf("configure history acquisition: %w: %s", err, out)
		}
	}
	p.snapshotSource = filepath.Join(dir, "snapshot-acquisition.git")
	if out, err := f.git(ctx, "", "init", "--bare", "--quiet", p.snapshotSource).CombinedOutput(); err != nil {
		return fmt.Errorf("initialize snapshot acquisition: %w: %s", err, out)
	}
	// Keep this source free of shallow boundaries. Expanding a shallow
	// source makes the server rebuild ancestry packs instead of using its
	// fast full-fetch path. Incremental updates still negotiate known refs.
	p.ancestrySource = filepath.Join(dir, "ancestry.git")
	if out, err := f.git(ctx, "", "init", "--bare", "--quiet", p.ancestrySource).CombinedOutput(); err != nil {
		return fmt.Errorf("initialize ancestry acquisition: %w: %s", err, out)
	}
	for _, kv := range [][2]string{{"remote.origin.url", p.remote}, {"remote.origin.promisor", "true"}, {"remote.origin.partialclonefilter", "blob:none"}, {"gc.auto", "0"}, {"maintenance.auto", "false"}} {
		if out, err := f.git(ctx, p.ancestrySource, "config", kv[0], kv[1]).CombinedOutput(); err != nil {
			return fmt.Errorf("configure ancestry acquisition: %w: %s", err, out)
		}
	}
	p.reader.UsePublishedHistorySources(p.ancestrySource, p.historySource, p.source)
	p.reader.DemandHistory = func(ctx context.Context, sha string) error { return f.acquireHistoryWindow(ctx, p, sha) }
	p.reader.DemandCommits = func(ctx context.Context, oids []string, depth int) error {
		if err := p.historyOperations.acquire(ctx); err != nil {
			return err
		}
		defer p.historyOperations.release()
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
	p.reader.Demand = func(ctx context.Context, oids []string) error {
		if err := p.blobOperations.acquire(ctx); err != nil {
			return err
		}
		defer p.blobOperations.release()
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
		// Match Git's promisor fetch: known commits do not imply that their
		// promised blobs are present, so these explicit wants skip negotiation.
		cmd := f.git(ctx, p.source, "-c", "fetch.negotiationAlgorithm=noop", "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--no-write-fetch-head", "--filter=blob:none", "--stdin", "origin")
		cmd.Stdin = strings.NewReader(strings.Join(missing, "\n") + "\n")
		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("fetch requested files: %w: %s", err, f.redact(string(out)))
		}
		return p.reader.ImportPacks(ctx, p.source)
	}
	return nil
}
func (f *FS) prepareProgressive(ctx context.Context, t Target, progress func(string)) (*progressiveRepository, *repo.Snapshot, error) {
	p, err := f.progressiveRepository(ctx, t)
	if err != nil {
		return nil, nil, err
	}
	progress("Resolving requested revision…")
	sha, err := f.resolve(ctx, p, t)
	if err != nil {
		return nil, nil, err
	}
	if _, err = p.reader.ObjectSize(ctx, sha); errors.Is(err, store.ErrNotFound) {
		// The selected ancestry is needed regardless of queries. Start its
		// network transfer alongside depth-one setup, but defer Store import
		// until the ordinary background history worker takes ownership.
		f.startEarlyAncestry(p, sha)
		if err = p.shallowOperations.acquire(ctx); err != nil {
			return nil, nil, err
		}
		progress("Fetching current snapshot trees (depth one, blobless)…")
		// A local ref lets later deepen requests advertise this acquired history.
		cmd := f.git(ctx, p.source, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--depth=1", "--filter=blob:none", "origin", sha+":refs/gyit/history/"+sha)
		out, e := cmd.CombinedOutput()
		if e == nil {
			e = p.reader.ImportPacks(ctx, p.source)
		}
		p.shallowOperations.release()
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
	done := make(chan struct{})
	if _, loaded := p.background.LoadOrStore(s.SHA, done); loaded {
		return
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		close(done)
		return
	}
	f.wg.Add(1)
	f.mu.Unlock()
	go func() {
		defer f.wg.Done()
		defer close(done)
		state := &pb.ProgressiveState{}
		if old, err := p.reader.State(f.ctx, s.SHA); err == nil {
			state = old
		}
		if state.SnapshotComplete && state.HistoryComplete {
			if err := p.reader.CompactHistory(f.ctx, s.SHA); err != nil && f.ctx.Err() == nil {
				j.progress("History graph packing paused: " + f.redact(err.Error()))
				p.background.Delete(s.SHA)
			}
			return
		}
		// Completion comes from the data's own publication. Serialize reporting
		// so an earlier read cannot overwrite a newer notice, without another CAS.
		var statusMu sync.Mutex
		report := func() error {
			statusMu.Lock()
			defer statusMu.Unlock()
			state, err := p.reader.State(f.ctx, s.SHA)
			if err != nil {
				return err
			}
			j.progress(state.Progress)
			return nil
		}
		snapshotReady := state.SnapshotComplete
		snapshotDone := make(chan error, 1)
		go func() {
			if snapshotReady {
				snapshotDone <- nil
				return
			}
			err := f.prepareBackgroundSnapshot(f.ctx, p, s)
			if err == nil {
				err = report()
			}
			snapshotDone <- err
		}()
		historyErr := f.ingestBackgroundHistory(f.ctx, p, s, j.progress)
		if historyErr != nil && f.ctx.Err() == nil {
			// Waiting readers see a recorded error; failing to record it is reported too.
			historyErr = errors.Join(historyErr, p.reader.SetHistoryError(f.ctx, s.SHA, f.redact(historyErr.Error())))
		} else if historyErr == nil {
			historyErr = report()
		}
		snapshotErr := <-snapshotDone
		if err := errors.Join(historyErr, snapshotErr); err != nil {
			if f.ctx.Err() == nil {
				j.progress("Background import paused: " + f.redact(err.Error()))
			}
			p.background.Delete(s.SHA)
			return
		}
	}()
}

func (f *FS) prepareBackgroundSnapshot(ctx context.Context, p *progressiveRepository, s *repo.Snapshot) error {
	if err := p.snapshotOperations.acquire(ctx); err != nil {
		return err
	}
	defer p.snapshotOperations.release()
	prepared, err := p.reader.HasPreparedSnapshot(ctx)
	if err != nil {
		return err
	}
	if !prepared {
		cmd := f.git(ctx, p.snapshotSource, "fetch", "--quiet", "--keep", "--no-tags", "--no-auto-maintenance", "--depth=1", p.remote, s.SHA)
		out, fetchErr := cmd.CombinedOutput()
		if fetchErr == nil {
			fetchErr = p.reader.ImportPacks(ctx, p.snapshotSource)
		}
		if fetchErr != nil {
			return fmt.Errorf("fetch snapshot contents: %w: %s", fetchErr, f.redact(string(out)))
		}
	}
	// This acquisition lane is independent of concurrent history fetches. Only
	// packs whose durable import finished above are used for local tree reads.
	return p.reader.PrepareSnapshot(ctx, s.SHA, p.snapshotSource)
}

// Full acquisition runs alongside growing shallow windows. A slow full pack
// cannot stall intermediate coverage; a fast full pack avoids many deepen RPCs.
func (f *FS) ingestBackgroundHistory(ctx context.Context, p *progressiveRepository, s *repo.Snapshot, progress func(string)) error {
	if err := p.reader.SetHistoryError(ctx, s.SHA, ""); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	var fullDone, fullAcquired chan struct{}
	var fullErr error
	defer func() {
		cancel()
		if fullDone != nil {
			<-fullDone
		}
	}()
	waitFull := func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fullDone:
			if errors.Is(fullErr, repo.ErrHistoryIndexPending) {
				return fmt.Errorf("file history is missing commit/tree objects after full ancestry acquisition")
			}
			return fullErr
		}
	}
	for deepen := 64; ; {
		progress("Publishing newest available file history…")
		passCtx, stopPass := context.WithCancel(ctx)
		passDone, watcherDone := make(chan struct{}), make(chan struct{})
		go func(acquired <-chan struct{}) {
			defer close(watcherDone)
			select {
			case <-acquired:
				stopPass()
			case <-passDone:
			}
		}(fullAcquired)
		err := p.reader.IngestHistory(passCtx, s.SHA, p.source, p.ancestrySource, p.historySource)
		close(passDone)
		stopPass()
		<-watcherDone
		select {
		case <-fullAcquired:
			return waitFull()
		default:
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, repo.ErrHistoryIndexPending) {
			return err
		}
		if fullDone == nil {
			fullDone = make(chan struct{})
			fullAcquired = make(chan struct{})
			early := p.takeEarlyAncestry(s.SHA)
			go func() {
				defer close(fullDone)
				fullErr = f.acquireFullAncestry(ctx, p, s.SHA, early, fullAcquired, progress)
			}()
		}
		// Publish the first shallow window without delay. Later windows give
		// the full fetch a short opportunity to finish before doing more work.
		wait := time.Duration(0)
		if deepen > 64 {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-fullDone:
			timer.Stop()
			return waitFull()
		case <-fullAcquired:
			timer.Stop()
			return waitFull()
		case <-timer.C:
		}
		coverage, err := p.reader.HistoryProgress(ctx, s.SHA)
		if err != nil {
			return err
		}
		progress(fmt.Sprintf("File history: %d commits indexed; acquiring older ancestry…", coverage.CoveredCommits))
		select {
		case p.shallowOperations <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-fullDone:
			return waitFull()
		case <-fullAcquired:
			return waitFull()
		}
		fetchCtx, stopFetch := context.WithCancel(ctx)
		stopWatching, watchExited := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(watchExited)
			select {
			case <-fullAcquired:
				stopFetch()
			case <-stopWatching:
			}
		}()
		err = f.fetchHistory(fetchCtx, p.source, s.SHA, deepen)
		close(stopWatching)
		stopFetch()
		<-watchExited
		// A complete local ancestry source supersedes more shallow acquisition.
		// The combined importer still gates coverage on durable pack publication.
		fullReady := false
		select {
		case <-fullAcquired:
			fullReady = true
		default:
		}
		if fullReady {
			err = nil
		} else if err == nil {
			progress(fmt.Sprintf("File history: %d commits indexed; publishing acquired objects…", coverage.CoveredCommits))
			err = p.reader.ImportPacks(ctx, p.source)
		}
		p.shallowOperations.release()
		if fullReady {
			return waitFull()
		}
		if err != nil {
			return err
		}
		deepen = min(deepen*16, 16384)
	}
}

// acquireFullAncestry closes acquired once the complete ancestry is local, then
// indexes it. An early transfer is speculative: if it failed, fetch again.
func (f *FS) acquireFullAncestry(ctx context.Context, p *progressiveRepository, sha string, early *ancestryAcquisition, acquired chan<- struct{}, progress func(string)) error {
	fetched := false
	if early != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-early.done:
			fetched = early.err == nil
		}
	}
	if err := p.ancestryOperations.acquire(ctx); err != nil {
		return err
	}
	defer p.ancestryOperations.release()
	if !fetched {
		if err := f.fetchHistory(ctx, p.ancestrySource, sha, 0); err != nil {
			return err
		}
	}
	close(acquired)
	progress("Indexing full ancestry while acquired objects publish…")
	return p.reader.ImportHistoryPacks(ctx, sha, p.ancestrySource)
}

func (f *FS) fetchHistory(ctx context.Context, source, sha string, deepen int) error {
	args := []string{"fetch", "--quiet", "--keep", "--no-auto-maintenance", "--filter=blob:none", "--no-tags"}
	if deepen > 0 {
		args = append(args, fmt.Sprintf("--deepen=%d", deepen))
	}
	args = append(args, "origin", sha+":refs/gyit/history/"+sha)
	return f.runHistoryFetch(ctx, source, args...)
}

func (f *FS) runHistoryFetch(ctx context.Context, source string, args ...string) error {
	cmd := f.git(ctx, source, args...)
	// Git's HTTP helper is not necessarily marked for parent signal cleanup.
	// Isolate the whole fetch and terminate its process group, giving Git and
	// index-pack their normal lock-file cleanup instead of killing the parent
	// alone and leaving a remote helper attached to the output pipes.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("fetch background history: %w: %s", err, f.redact(string(out)))
	}
	return nil
}
