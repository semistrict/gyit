//go:build !js

package repo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime/trace"
	"time"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/spill"
	"gyit/internal/store"
)

const historyPublicationCommits = 8192

func (p *Progressive) historyWriter(ctx context.Context, backend store.Store, prefix string) *indexWriter {
	return &indexWriter{ctx: ctx, store: backend, prefix: prefix, packLimit: progressiveContainerBytes, admit: func(ctx context.Context, key string, packed []byte) {
		if p.cache.disk == nil && p.cache.max == 0 {
			return
		}
		// Cache admission is optional and bounded by the reader's expansion
		// limit. Coverage remains gated on durable uploads and HEAD CAS,
		// including when backend queues rather than completes an upload.
		if decoded, err := decodeHistoryContainer(ctx, packed); err == nil {
			p.cache.put("history-container/"+key, decoded)
		}
	}}
}

// ingestHistoryBatches publishes independently covered commits, newest ancestry
// first. Both the frontier and postings spill to disk. Coverage, continuation
// and data become visible together through one HEAD CAS.
func (p *Progressive) ingestHistoryBatches(ctx context.Context, tips []string, gitdirs ...string) error {
	return p.ingestHistoryInput(ctx, tips, func() (*historySource, error) {
		return p.publishedHistorySource(ctx, gitdirs)
	}, nil, nil)
}

func (p *Progressive) ingestHistoryInput(ctx context.Context, tips []string, openSource func() (*historySource, error), include func(context.Context, *spill.Sorter) error, available <-chan struct{}) error {
	return p.ingestHistoryInputBounded(ctx, tips, openSource, include, available, 0)
}

// A nonzero limit builds a bounded prefix in a private staging repository.
// Its ordinary publication still records the unprocessed frontier.
func (p *Progressive) ingestHistoryInputBounded(ctx context.Context, tips []string, openSource func() (*historySource, error), include func(context.Context, *spill.Sorter) error, available <-chan struct{}, limit uint64) error {
	select {
	case p.historyBuild <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.historyBuild }()
	if err := ctx.Err(); err != nil {
		return err
	}
	var pending []string
	for _, sha := range tips {
		if !validProgressiveOID(sha) {
			return fmt.Errorf("invalid history revision")
		}
		state, err := p.HistoryProgress(ctx, sha)
		if err != nil {
			return err
		}
		if !state.Complete {
			pending = append(pending, sha)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	source, err := openSource()
	if err != nil {
		return err
	}
	defer source.close()
	ctx = context.WithValue(ctx, historySourceKey{}, source)
	enc, err := newCompressor()
	if err != nil {
		return err
	}
	defer enc.Close()
	return p.stageHistory(func(stage *historyStage) error {
		graphs, err := stage.tx.CreateBucket([]byte("graph-batches"))
		if err != nil {
			return err
		}
		stage.buckets["graph-batches"] = graphs
		var graphFrames uint64
		var publishing *historyPublication
		defer func() {
			if publishing != nil {
				publishing.abort()
				_ = publishing.wait()
			}
		}()
		preparation := newHistoryPreparation(ctx, p)
		defer preparation.close()
		pathRecords, err := spill.New(p.temp, historyChangeMemory)
		if err != nil {
			return err
		}
		defer func() {
			if pathRecords != nil {
				_ = pathRecords.Close()
			}
		}()
		var covered uint64
		// Keep a first-parent run contiguous in graph frames. Side branches
		// remain queued newest-discovered first, rather than scattering each
		// run across the breadth of the merge graph. This affects preparation
		// and physical locality only; readers retain their date/TREESAME order.
		head, tail := uint64(1)<<63, uint64(1)<<63
		push := func(sha string, front bool) error {
			if !validProgressiveOID(sha) {
				return fmt.Errorf("invalid history frontier commit")
			}
			var key [8]byte
			if front {
				if head == 0 {
					return fmt.Errorf("history frontier sequence exhausted")
				}
				head--
				binary.BigEndian.PutUint64(key[:], head)
			} else {
				if tail == ^uint64(0) {
					return fmt.Errorf("history frontier sequence exhausted")
				}
				binary.BigEndian.PutUint64(key[:], tail)
				tail++
			}
			return stage.buckets["queue"].Put(key[:], []byte(sha))
		}
		for _, sha := range pending {
			state, err := p.HistoryProgress(ctx, sha)
			if err != nil {
				return err
			}
			covered = max(covered, state.CoveredCommits)
			if state.Frontier == nil {
				if err := push(sha, false); err != nil {
					return err
				}
				continue
			}
			for ref := state.Frontier; ref != nil; {
				if err := ctx.Err(); err != nil {
					return err
				}
				var page pb.HistoryFrontierPage
				if err := p.readHistoryPage(ctx, ref, &page); err != nil {
					return err
				}
				if len(page.Commits) == 0 || len(page.Commits) > 256 {
					return fmt.Errorf("invalid history frontier")
				}
				for _, oid := range page.Commits {
					if err := push(hex.EncodeToString(oid), false); err != nil {
						return err
					}
				}
				ref = page.Next
			}
		}
		batch := &pb.HistoryBatch{Version: historyBatchVersion}
		display := &pb.HistoryDisplay{}
		dirty := false
		// Read frames remain <=64 KiB, but publication cadence is independent
		// of that read unit. Publish the first frame immediately; then amortize
		// remote CAS and COW costs across at most 8192 commits / one second
		// of work. This also avoids routinely exceeding GCS's same-object
		// overwrite rate on HEAD.
		uploads := newPublicationUploads(ctx, p.store)
		defer func() {
			if uploads != nil {
				uploads.close()
			}
		}()
		w := p.historyWriter(ctx, uploads, "progressive-history-v2-"+rand.Text())
		headers := p.historyWriter(ctx, uploads, linkedHistoryGraphPrefix+rand.Text())
		submitted := false
		publicationCommits := 0
		lastPublication := time.Now()
		save := func(m proto.Message) (*pb.PageReference, error) {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if err != nil {
				return nil, err
			}
			if len(raw) > 64<<10 {
				return nil, fmt.Errorf("history batch page exceeds 64 KiB")
			}
			ref, err := w.saveBytes(enc.EncodeAll(raw, nil))
			return encodePageRef(ref), err
		}
		clearBucket := func(name string) error {
			if err := stage.tx.DeleteBucket([]byte(name)); err != nil {
				return err
			}
			b, err := stage.tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			stage.buckets[name] = b
			return nil
		}
		flush := func(final bool) error {
			if len(batch.Commits) == 0 && !dirty && !final {
				return nil
			}
			publicationCommits += len(batch.Commits)
			if len(batch.Commits) > 0 {
				batch.PathFilter = make([]byte, historyFilterBytes)
				root, err := writeHistoryPaths(func(emit func([]byte, []byte) error) error {
					return pathRecords.Walk(ctx, emit)
				}, batch.PathFilter, save)
				if err != nil {
					return err
				}
				batch.Paths = root
				batch.Display, err = save(display)
				if err != nil {
					return err
				}
				raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
				if err != nil {
					return err
				}
				if len(raw) > 64<<10 {
					return fmt.Errorf("history graph page exceeds 64 KiB")
				}
				var sequence [8]byte
				binary.BigEndian.PutUint64(sequence[:], graphFrames)
				graphFrames++
				if err := stage.buckets["graph-batches"].Put(sequence[:], raw); err != nil {
					return err
				}
			}
			batch = &pb.HistoryBatch{Version: historyBatchVersion}
			display = &pb.HistoryDisplay{}
			if err := pathRecords.Close(); err != nil {
				return err
			}
			pathRecords = nil
			if !final {
				pathRecords, err = spill.New(p.temp, historyChangeMemory)
				if err != nil {
					return err
				}
			}
			if !final && !submitted && publicationCommits < historyPublicationCommits && available != nil {
				// Pack recipes cannot publish yet. Keep ready frames in this
				// first bounded publication instead of freezing a tiny batch
				// whose CAS must wait for exactly the same prerequisite.
				select {
				case <-available:
				default:
					return stage.checkpoint()
				}
			}
			if !final && submitted && publicationCommits < historyPublicationCommits && time.Since(lastPublication) < time.Second {
				return stage.checkpoint()
			}
			if final {
				changed, _ := stage.buckets["changes"].Cursor().First()
				missing, _ := stage.buckets["missing"].Cursor().First()
				if changed == nil && graphFrames == 0 && missing != nil {
					// No new coverage: the previous frontier (or selected tip
					// when absent) is still a safe continuation. In particular,
					// depth-one acquisition cannot yet compare a parent tree.
					// Keep completion publications even without new records.
					return nil
				}
			}
			if err := writeLinkedHistoryFrames(ctx, stage, "graph-batches", headers, enc); err != nil {
				return err
			}
			if err := clearBucket("graph-batches"); err != nil {
				return err
			}
			graphFrames = 0
			frontier, err := writeHistoryFrontier(stage, save)
			if err != nil {
				return err
			}
			state := &pb.HistoryIngestion{Frontier: frontier, CoveredCommits: covered, Complete: frontier == nil}
			for _, sha := range pending {
				if err := progressivePut(stage.buckets["changes"], historyIngestionKey+sha, state); err != nil {
					return err
				}
			}
			if err := w.flush(); err != nil {
				return err
			}
			if err := headers.flush(); err != nil {
				return err
			}
			if publishing != nil {
				if err := publishing.wait(); err != nil {
					return err
				}
			}
			trace.Logf(ctx, "history-batch", "commits=%d covered=%d complete=%t", publicationCommits, covered, frontier == nil)
			publishing, err = p.publishHistoryAsync(ctx, stage.buckets["changes"], uploads, include)
			if err != nil {
				return err
			}
			// Ownership moved to the publication worker. Only a new upload
			// group may be canceled/closed by the producer's cleanup below.
			uploads = nil
			if err := clearBucket("changes"); err != nil {
				return err
			}
			uploads = newPublicationUploads(ctx, p.store)
			w = p.historyWriter(ctx, uploads, "progressive-history-v2-"+rand.Text())
			headers = p.historyWriter(ctx, uploads, linkedHistoryGraphPrefix+rand.Text())
			publicationCommits = 0
			submitted = true
			lastPublication = time.Now()
			dirty = false
			return stage.checkpoint()
		}
		// Unavailable commits go to a separate durable frontier. Continue other
		// available branches instead of stopping at the first shallow boundary.
		for steps := 0; ; steps++ {
			if limit > 0 && covered >= limit {
				break
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			k, v := stage.buckets["queue"].Cursor().First()
			if k == nil {
				break
			}
			key, sha := bytes.Clone(k), string(v)
			if stage.buckets["seen"].Get([]byte(sha)) != nil {
				preparation.forget(sha)
				if err := stage.buckets["queue"].Delete(key); err != nil {
					return err
				}
				continue
			}
			prepared := preparation.get(sha)
			if prepared.complete {
				prepared.close()
				if err := stage.buckets["queue"].Delete(key); err != nil {
					return err
				}
				dirty = true
				continue
			}
			commit := prepared.commit
			err = prepared.err
			// Preparation may finish out of order, but only this frontier's
			// next commit can enter a frame or advance published coverage.
			if err == nil && prepared.paths != nil {
				if len(batch.Commits) > 0 && (len(batch.Commits) >= min(64, historyPublicationCommits-publicationCommits) || (proto.Size(display)+proto.Size(commit.metadata)+256 > 60<<10 || proto.Size(batch)+proto.Size(commit.HistoryBatchCommit)+historyFilterBytes+256 > 60<<10)) {
					if err := flush(false); err != nil {
						prepared.close()
						return err
					}
				}
				ordinal := uint32(len(batch.Commits))
				err = prepared.paths.walk(ctx, func(path string, mask []byte) error {
					key := make([]byte, len(path)+5)
					copy(key, path)
					binary.BigEndian.PutUint32(key[len(path)+1:], ordinal)
					return pathRecords.Add(key, mask)
				})
				if err == nil {
					batch.Commits = append(batch.Commits, commit.HistoryBatchCommit)
					display.Commits = append(display.Commits, commit.metadata)
					covered++
				}
			}
			prepared.close()
			if err != nil && !errors.Is(err, store.ErrNotFound) {
				return err
			}
			if err != nil {
				if e := stage.buckets["missing"].Put([]byte(sha), []byte{1}); e != nil {
					return e
				}
			} else {
				for i, parent := range commit.Parents {
					if err := push(hex.EncodeToString(parent), i == 0); err != nil {
						return err
					}
				}
			}
			if err := stage.buckets["queue"].Delete(key); err != nil {
				return err
			}
			if err := stage.buckets["seen"].Put([]byte(sha), []byte{1}); err != nil {
				return err
			}
			dirty = true
			if steps%256 == 255 {
				if err := stage.checkpoint(); err != nil {
					return err
				}
			}
		}
		if err := flush(true); err != nil {
			return err
		}
		if publishing != nil {
			if err := publishing.wait(); err != nil {
				return err
			}
		}
		if k, _ := stage.buckets["queue"].Cursor().First(); limit > 0 && k != nil {
			return ErrHistoryIndexPending
		}
		if k, _ := stage.buckets["missing"].Cursor().First(); k != nil {
			return ErrHistoryIndexPending
		}
		return nil
	})
}

func writeHistoryFrontier(stage *historyStage, save func(proto.Message) (*pb.PageReference, error)) (*pb.PageReference, error) {
	page := &pb.HistoryFrontierPage{}
	var next *pb.PageReference
	flush := func() error {
		if len(page.Commits) == 0 {
			return nil
		}
		for i, j := 0, len(page.Commits)-1; i < j; i, j = i+1, j-1 {
			page.Commits[i], page.Commits[j] = page.Commits[j], page.Commits[i]
		}
		page.Next = next
		ref, err := save(page)
		if err != nil {
			return err
		}
		next = ref
		page = &pb.HistoryFrontierPage{}
		return nil
	}
	for _, name := range []string{"missing", "queue"} {
		cursor := stage.buckets[name].Cursor()
		for k, v := cursor.Last(); k != nil; k, v = cursor.Prev() {
			id := v
			if name == "missing" {
				id = k
			}
			oid, err := hex.DecodeString(string(id))
			if err != nil || len(oid) != 20 {
				return nil, fmt.Errorf("invalid frontier oid")
			}
			page.Commits = append(page.Commits, oid)
			if len(page.Commits) == 256 {
				if err := flush(); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return next, nil
}

// SetHistoryError records acquisition failures for waiting readers. It does not
// change covered commits or discard the last resumable frontier.
func (p *Progressive) SetHistoryError(ctx context.Context, sha, message string) error {
	p.writer.Lock()
	defer p.writer.Unlock()
	p.mu.RLock()
	root, expected := p.root, p.token
	p.mu.RUnlock()
	idx := &index{store: p.store, cache: p.cache, root: root, containers: true}
	state := &pb.HistoryIngestion{}
	err := idx.get(ctx, historyIngestionKey+sha, state)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if state.Error == message {
		// A no-op has no CAS write, but must still detect a stale writer.
		// Compare against the version paired with the state we read, since
		// readers may refresh p.root/p.token without acquiring p.writer.
		_, current, err := p.store.Get(ctx, "HEAD", 0, -1)
		if errors.Is(err, store.ErrNotFound) {
			if expected == "*" {
				return nil
			}
			return store.ErrConflict
		}
		if err != nil {
			return err
		}
		if current != expected {
			return store.ErrConflict
		}
		return nil
	}
	state.Error = message
	return p.stage(func(b *bolt.Bucket) error {
		if err := progressivePut(b, historyIngestionKey+sha, state); err != nil {
			return err
		}
		return p.publish(ctx, b)
	})
}

// Emit only differing identities/modes, including directory paths. No blob
// bytes or file sizes are needed. Identical trees skip their entire subtrees.
func (p *Progressive) historyTreeDelta(ctx context.Context, now, old string, emit func(string) error) error {
	return p.historyTreeDeltaWhere(ctx, now, old, nil, emit)
}

// A filter may prune entire directories before fetching their tree objects.
// Callers must include the ancestors of every path they want to compare.
func (p *Progressive) historyTreeDeltaWhere(ctx context.Context, now, old string, include func(string) bool, emit func(string) error) error {
	return p.historyTreeDeltaPaths(ctx, now, old, include, emit, false)
}

// Directory-only postings preserve trailing-slash semantics across file/directory
// replacements without requiring a reader to fetch the historical trees.
func (p *Progressive) historyTreeDeltaPaths(ctx context.Context, now, old string, include func(string) bool, emit func(string) error, directories bool) error {
	// Tree records already use Git's name ordering (directories compare as
	// name + slash). Merge those streams directly; a file/directory type change
	// appears as a deletion plus an insertion and ORs the same parent bit.
	entries := func(e Entry) ([]byte, func(), error) {
		if e.Mode != 0040000 || e.OID == "" {
			return nil, func() {}, nil
		}
		raw, release, err := p.borrowObject(ctx, e.OID)
		if err != nil {
			return nil, release, err
		}
		if len(raw) == 0 || raw[0] != 2 {
			return nil, release, fmt.Errorf("history expected tree")
		}
		return raw[1:], release, nil
	}
	workspace := 0
	var walk func(string, Entry, Entry, int) error
	walk = func(path string, a, b Entry, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if depth > 512 {
			return fmt.Errorf("history path depth")
		}
		if a.OID == b.OID && a.Mode == b.Mode {
			return nil
		}
		if include != nil && !include(path) {
			return nil
		}
		if err := emit(path); err != nil {
			return err
		}
		if a.Mode != 0040000 && b.Mode != 0040000 {
			return nil
		}
		if directories && path != "" {
			if err := emit(path + "/"); err != nil {
				return err
			}
		}
		left, releaseLeft, err := entries(a)
		defer releaseLeft()
		if err != nil {
			return err
		}
		right, releaseRight, err := entries(b)
		defer releaseRight()
		if err != nil {
			return err
		}
		leftBytes, rightBytes := len(left), len(right)
		workspace += leftBytes + rightBytes
		defer func() { workspace -= leftBytes + rightBytes }()
		if workspace > 32<<20 {
			return fmt.Errorf("history tree traversal exceeds 32 MiB workspace")
		}
		if err := skipEqualHistoryEntries(ctx, &left, &right); err != nil {
			return err
		}
		x, ex := takeHistoryTreeEntry(&left)
		y, ey := takeHistoryTreeEntry(&right)
		for visited := 0; ex != io.EOF || ey != io.EOF; visited++ {
			if visited%256 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if ex != nil && ex != io.EOF {
				return ex
			}
			if ey != nil && ey != io.EOF {
				return ey
			}
			cmp := compareHistoryTreeEntries(x, y)
			takeLeft := ex == nil && (ey == io.EOF || cmp <= 0)
			takeRight := ey == nil && (ex == io.EOF || cmp >= 0)
			equal := takeLeft && takeRight && x.mode == y.mode && bytes.Equal(x.oid, y.oid)
			if !equal {
				var ea, eb Entry
				var name []byte
				if takeLeft {
					name = x.name
					ea = Entry{OID: hex.EncodeToString(x.oid), Mode: x.mode}
				}
				if takeRight {
					name = y.name
					eb = Entry{OID: hex.EncodeToString(y.oid), Mode: y.mode}
				}
				child := string(name)
				if path != "" {
					child = path + "/" + string(name)
				}
				if err := walk(child, ea, eb, depth+1); err != nil {
					return err
				}
			}
			// Probe for another equal run only after an unchanged entry.
			// Dense rewrites should not pay for a failed probe at every file.
			if equal {
				if err := skipEqualHistoryEntries(ctx, &left, &right); err != nil {
					return err
				}
			}
			if takeLeft {
				x, ex = takeHistoryTreeEntry(&left)
			}
			if takeRight {
				y, ey = takeHistoryTreeEntry(&right)
			}
		}
		return nil
	}
	// The root path denotes changes to files under the tree. An empty
	// root commit has no changes, just like Git log -- .
	const emptyTree = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
	if now == emptyTree {
		now = ""
	}
	if old == emptyTree {
		old = ""
	}
	a, b := Entry{}, Entry{}
	if now != "" {
		a = Entry{OID: now, Mode: 0040000}
	}
	if old != "" {
		b = Entry{OID: old, Mode: 0040000}
	}
	return walk("", a, b, 0)
}

func writeHistoryPaths(walk func(func([]byte, []byte) error) error, filter []byte, save func(proto.Message) (*pb.PageReference, error)) (*pb.PageReference, error) {
	// At most 32 references per tree level; a huge root commit must not
	// accumulate one in-memory reference for every path page.
	levels := make([][]*pb.DirectoryChild, 1)
	var add func(int, *pb.DirectoryChild) error
	add = func(level int, child *pb.DirectoryChild) error {
		if level > 32 {
			return fmt.Errorf("history path index depth")
		}
		if level == len(levels) {
			levels = append(levels, nil)
		}
		levels[level] = append(levels[level], child)
		if len(levels[level]) < 32 {
			return nil
		}
		ref, err := save(&pb.HistoryPathPage{Children: levels[level]})
		if err != nil {
			return err
		}
		maxName := levels[level][31].MaxName
		levels[level] = nil
		return add(level+1, &pb.DirectoryChild{MaxName: maxName, Page: ref})
	}
	page := &pb.HistoryPathPage{}
	pageSize := 0
	flush := func() error {
		if len(page.Entries) == 0 {
			return nil
		}
		ref, err := save(page)
		if err != nil {
			return err
		}
		if err := add(0, &pb.DirectoryChild{MaxName: page.Entries[len(page.Entries)-1].Path, Page: ref}); err != nil {
			return err
		}
		page = &pb.HistoryPathPage{}
		pageSize = 0
		return nil
	}
	var posting *pb.HistoryPathPosting
	appendPosting := func() error {
		if posting == nil {
			return nil
		}
		postingSize := proto.Size(posting)
		if len(page.Entries) > 0 && pageSize+postingSize > progressivePageBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		page.Entries = append(page.Entries, posting)
		// Entries are immutable after append. Count their field-1 tag and
		// length prefix once instead of rescanning the entire page each time.
		pageSize += protowire.SizeTag(1) + protowire.SizeBytes(postingSize)
		return nil
	}
	err := walk(func(k, v []byte) error {
		if len(k) < 5 || k[len(k)-5] != 0 {
			return fmt.Errorf("invalid history staging key")
		}
		name := k[:len(k)-5]
		if posting == nil || !bytes.Equal(posting.Path, name) {
			if err := appendPosting(); err != nil {
				return err
			}
			historyFilterAdd(filter, string(name))
			posting = &pb.HistoryPathPosting{Path: bytes.Clone(name)}
		}
		posting.Ordinals = append(posting.Ordinals, binary.BigEndian.Uint32(k[len(k)-4:]))
		posting.DifferentParents = append(posting.DifferentParents, bytes.Clone(v))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if err := appendPosting(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	for level := 0; level < len(levels); level++ {
		children := levels[level]
		if len(children) == 0 {
			continue
		}
		if level == len(levels)-1 && len(children) == 1 {
			return children[0].Page, nil
		}
		ref, err := save(&pb.HistoryPathPage{Children: children})
		if err != nil {
			return nil, err
		}
		levels[level] = nil
		if err := add(level+1, &pb.DirectoryChild{MaxName: children[len(children)-1].MaxName, Page: ref}); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
