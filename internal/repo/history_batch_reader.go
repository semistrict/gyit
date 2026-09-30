package repo

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

const historyBatchKey = "history/v2/commit/"
const historyIngestionKey = "history/v2/ingestion/"

// Version 3 adds directory-only postings keyed by path plus a trailing slash.
// Version 2 frames remain readable; their directory-only readers recheck trees.
const (
	historyBatchVersionUndirected = 2
	historyBatchVersion           = 3
)

func knownHistoryBatchVersion(v uint64) bool {
	return v == historyBatchVersionUndirected || v == historyBatchVersion
}

func (p *Progressive) HistoryProgress(ctx context.Context, sha string) (*pb.HistoryIngestion, error) {
	var state pb.HistoryIngestion
	err := p.get(ctx, historyIngestionKey+sha, &state)
	if errors.Is(err, store.ErrNotFound) {
		return &state, nil
	}
	return &state, err
}

func (p *Progressive) readHistoryBatch(ctx context.Context, location *pb.HistoryBatchLocation) (*pb.HistoryBatch, error) {
	if location.Batch == nil {
		return nil, fmt.Errorf("missing history batch reference")
	}
	var batch pb.HistoryBatch
	if err := p.readHistoryPage(ctx, location.Batch, &batch); err != nil {
		return nil, err
	}
	if !knownHistoryBatchVersion(uint64(batch.Version)) || batch.Display == nil || len(batch.PathFilter) != historyFilterBytes || len(batch.Commits) == 0 || len(batch.Commits) > 64 || int(location.Ordinal) >= len(batch.Commits) {
		return nil, fmt.Errorf("invalid history batch")
	}
	for _, c := range batch.Commits {
		if len(c.Oid) != 20 || len(c.Parents) > maxLogParents || len(c.Parents) != len(c.ParentTimes) {
			return nil, fmt.Errorf("invalid history commit")
		}
		for _, parent := range c.Parents {
			if len(parent) != 20 {
				return nil, fmt.Errorf("invalid history parent")
			}
		}
	}
	if len(batch.Links) > 128 {
		return nil, fmt.Errorf("history link count")
	}
	if len(batch.Links) > 0 {
		parents := make(map[string]bool)
		for _, c := range batch.Commits {
			for _, oid := range c.Parents {
				parents[string(oid)] = true
			}
		}
		for _, link := range batch.Links {
			if len(link.CommitOid) != 20 || !parents[string(link.CommitOid)] || link.Location == nil || link.Location.Batch == nil || link.Location.Ordinal >= 64 {
				return nil, fmt.Errorf("invalid history parent link")
			}
			delete(parents, string(link.CommitOid))
		}
	}
	return &batch, nil
}
func (p *Progressive) batchPath(ctx context.Context, batch *pb.HistoryBatch, path string) ([][]byte, error) {
	result := make([][]byte, len(batch.Commits))
	if !historyFilterMatch(batch.PathFilter, path) {
		return result, nil
	}
	ref := batch.Paths
	for depth := 0; ref != nil; depth++ {
		if depth > 32 {
			return nil, fmt.Errorf("history path index depth")
		}
		var page pb.HistoryPathPage
		if err := p.readHistoryPage(ctx, ref, &page); err != nil {
			return nil, err
		}
		if len(page.Children) > 32 || (len(page.Children) > 0 && len(page.Entries) > 0) {
			return nil, fmt.Errorf("invalid history path page")
		}
		if len(page.Children) > 0 {
			i := sort.Search(len(page.Children), func(i int) bool { return string(page.Children[i].MaxName) >= path })
			if i == len(page.Children) {
				return result, nil
			}
			ref = page.Children[i].Page
			continue
		}
		i := sort.Search(len(page.Entries), func(i int) bool { return string(page.Entries[i].Path) >= path })
		if i == len(page.Entries) || string(page.Entries[i].Path) != path {
			return result, nil
		}
		posting := page.Entries[i]
		if len(posting.Ordinals) != len(posting.DifferentParents) {
			return nil, fmt.Errorf("invalid history path posting")
		}
		previous := -1
		for i, ordinal := range posting.Ordinals {
			if int(ordinal) <= previous || int(ordinal) >= len(result) {
				return nil, fmt.Errorf("invalid history posting ordinal")
			}
			previous = int(ordinal)
			mask := posting.DifferentParents[i]
			parents := max(1, len(batch.Commits[ordinal].Parents))
			if len(mask) != (parents+7)/8 {
				return nil, fmt.Errorf("invalid history parent bitmap")
			}
			result[ordinal] = mask
		}
		return result, nil
	}
	return result, nil
}

// Poll only at a coverage gap. The token check prevents an older refresh from
// overwriting a concurrent local writer's newer publication. Normal reads stay
// on immutable references and never acquire the writer's publication lock.
func (p *Progressive) historyLocation(ctx context.Context, sha, tip string, hint func(pageRef, []byte), fallback func() error) (*pb.HistoryBatchLocation, error) {
	demanded := false
	for {
		var location pb.HistoryBatchLocation
		idx := p.index()
		idx.pageRanges = true
		if err := idx.getPrefetch(ctx, historyBatchKey+sha, &location, hint); err == nil {
			return &location, nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if err := fallback(); err == nil {
			return nil, nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !demanded && p.DemandHistory != nil {
			demanded = true
			if err := p.DemandHistory(ctx, sha); err != nil {
				return nil, err
			}
			continue
		}
		p.mu.RLock()
		token, changedLocally := p.token, p.historyChanged
		p.mu.RUnlock()
		latest, err := newProgressive(ctx, p.store, p.cache.disk, p.temp)
		if err != nil {
			return nil, err
		}
		p.mu.Lock()
		changed := p.token != latest.token
		if p.token == token {
			p.root, p.token = latest.root, latest.token
		}
		p.mu.Unlock()
		if changed {
			continue
		}
		state, err := p.HistoryProgress(ctx, tip)
		if err != nil {
			return nil, err
		}
		if state.Error != "" {
			return nil, fmt.Errorf("history ingestion failed: %s", state.Error)
		}
		if state.Complete {
			// Publication may have advanced between the first lookup and state.
			if err := p.get(ctx, historyBatchKey+sha, &location); err == nil {
				return &location, nil
			} else if !errors.Is(err, store.ErrNotFound) {
				return nil, err
			}
			return nil, fmt.Errorf("completed history is missing commit %s", sha)
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-changedLocally:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// The heap follows the same date-priority, parent insertion and TREESAME rules
// as the general log walker. Parent timestamps travel with covered commits, so
// an unavailable parent never delays an already-proven current result.
func (s *Snapshot) indexedFileLog(ctx context.Context, path string, opt LogOptions, emit func(LogEntry) error) (resultErr error) {
	if path == "." {
		path = ""
	}
	if path == "./" {
		path = ""
	}
	view := historyReadView{p: s.progressive, base: ctx, ctx: ctx}
	if s.progressive.historyReadView != nil {
		state, err := s.progressive.HistoryProgress(ctx, s.SHA)
		if err != nil {
			return err
		}
		if !state.Complete {
			if err := view.refresh(); err != nil {
				return err
			}
		}
	}
	defer view.close()
	readAhead := newHistoryPrefetch(ctx, s.progressive)
	defer readAhead.close()
	type cachedBatch struct {
		key      string
		ref      *pb.PageReference
		batch    *pb.HistoryBatch
		masks    [][]byte
		ordinals map[string]uint32
		links    map[string]*pb.HistoryBatchLocation
		display  []*pb.CommitRecord
	}
	cache := make([]cachedBatch, 0, 8)
	var rawDisplay unindexedHistoryDisplay
	rawReader := &unindexedHistoryReader{p: s.progressive, path: path, firstParent: opt.FirstParent}
	load := func(sha string, location *pb.HistoryBatchLocation) (*pb.HistoryBatchCommit, []byte, error) {
		if view.local() {
			raw, mask, err := rawReader.load(view.ctx, sha)
			if err == nil {
				rawDisplay = unindexedHistoryDisplay{sha: sha}
				return raw.HistoryBatchCommit, mask, nil
			}
			if !errors.Is(err, store.ErrNotFound) {
				return nil, nil, err
			}
		}
		var err error
		for _, c := range cache {
			if location != nil {
				break
			}
			if ordinal, ok := c.ordinals[sha]; ok {
				location = &pb.HistoryBatchLocation{Batch: c.ref, Ordinal: ordinal}
				break
			}
		}
		if location == nil {
			for _, c := range cache {
				if linked := c.links[sha]; linked != nil {
					location = linked
					break
				}
			}
		}
		if location == nil {
			var raw *historyIngestCommit
			var mask []byte
			location, err = s.progressive.historyLocation(ctx, sha, s.SHA, readAhead.indexSiblings, func() error {
				if err := view.refresh(); err != nil {
					return err
				}
				var e error
				raw, mask, e = rawReader.load(view.ctx, sha)
				return e
			})
			if err != nil {
				return nil, nil, err
			}
			if location == nil {
				rawDisplay = unindexedHistoryDisplay{sha: sha}
				return raw.HistoryBatchCommit, mask, nil
			}
		}
		if location.Batch == nil {
			return nil, nil, fmt.Errorf("missing history batch")
		}
		var item cachedBatch
		for i, c := range cache {
			if c.key == location.Batch.Hash {
				item = c
				copy(cache[i:], cache[i+1:])
				cache = cache[:len(cache)-1]
				break
			}
		}
		if item.batch == nil {
			item.batch, err = s.progressive.readHistoryBatch(ctx, location)
			if err != nil {
				return nil, nil, err
			}
			if len(item.batch.Links) == 0 {
				readAhead.after(location.Batch, path)
			}
			indexPath := path
			if item.batch.Version == historyBatchVersionUndirected {
				indexPath = strings.TrimSuffix(path, "/")
			}
			item.masks, err = s.progressive.batchPath(ctx, item.batch, indexPath)
			if err != nil {
				return nil, nil, err
			}
			item.key, item.ref = location.Batch.Hash, location.Batch
			item.ordinals = make(map[string]uint32, len(item.batch.Commits))
			item.links = make(map[string]*pb.HistoryBatchLocation, len(item.batch.Links))
			for _, link := range item.batch.Links {
				item.links[hex.EncodeToString(link.CommitOid)] = link.Location
			}
			for i, c := range item.batch.Commits {
				item.ordinals[hex.EncodeToString(c.Oid)] = uint32(i)
			}
		}
		if len(cache) == 8 {
			cache = cache[1:]
		}
		cache = append(cache, item)
		if int(location.Ordinal) >= len(item.batch.Commits) {
			return nil, nil, fmt.Errorf("history ordinal bounds")
		}
		c := item.batch.Commits[location.Ordinal]
		if hex.EncodeToString(c.Oid) != sha {
			return nil, nil, fmt.Errorf("history commit identity mismatch")
		}
		if item.batch.Version == historyBatchVersionUndirected && strings.HasSuffix(path, "/") && len(item.masks[location.Ordinal]) != 0 {
			// Older frames do not distinguish a directory from a same-named file.
			// Recheck changed values directly, never enumerate their descendants.
			_, mask, err := rawReader.load(view.ctx, sha)
			return c, mask, err
		}
		return c, item.masks[location.Ordinal], nil
	}
	metadata := func(sha string) (*pb.CommitRecord, error) {
		if rawDisplay.sha == sha {
			if rawDisplay.metadata == nil {
				c, _, err := s.progressive.historyBatchCommit(view.ctx, sha)
				if err != nil {
					return nil, err
				}
				rawDisplay.metadata = c.metadata
			}
			return rawDisplay.metadata, nil
		}
		for i := range cache {
			ordinal, ok := cache[i].ordinals[sha]
			if !ok {
				continue
			}
			if cache[i].display == nil {
				var page pb.HistoryDisplay
				if err := s.progressive.readHistoryPage(ctx, cache[i].batch.Display, &page); err != nil {
					return nil, err
				}
				if len(page.Commits) != len(cache[i].batch.Commits) {
					return nil, fmt.Errorf("history display count")
				}
				for _, m := range page.Commits {
					if m == nil || len(m.Author) > maxLogAuthor || len(m.Committer) > maxLogAuthor || len(m.Message) > maxLogMessage {
						return nil, fmt.Errorf("invalid history display record")
					}
				}
				cache[i].display = page.Commits
			}
			return cache[i].display[ordinal], nil
		}
		return nil, fmt.Errorf("missing covered history display")
	}
	walk := newLogTraversal(ctx, s.progressive.temp)
	defer func() { resultErr = errors.Join(resultErr, walk.close()) }()
	linear := true
	if err := walk.push(logCandidate{sha: s.SHA}); err != nil {
		return err
	}
	for written := 0; opt.Unlimited || written < opt.Count; {
		candidate, ok, err := walk.pop()
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		c, mask, err := load(candidate.sha, candidate.location)
		if err != nil {
			return err
		}
		n := len(c.Parents)
		if opt.FirstParent && n > 1 {
			n = 1
		}
		next := make([]int, 0, n)
		show := false
		if n == 0 {
			show = len(mask) > 0 && mask[0]&1 != 0
		} else {
			show = true
			for i := 0; i < n; i++ {
				if len(mask) == 0 || mask[i/8]&(1<<uint(i%8)) == 0 {
					show = false
					next = append(next, i)
					break
				}
			}
			if show {
				for i := 0; i < n; i++ {
					next = append(next, i)
				}
			}
		}
		if show {
			m, err := metadata(candidate.sha)
			if err != nil {
				return err
			}
			entry := LogEntry{SHA: candidate.sha, Author: m.Author, Message: m.Message, AuthorTime: m.AuthorTime, AuthorOffset: m.AuthorOffsetMinutes, MessageTruncated: m.MessageTruncated, AuthorTruncated: m.AuthorTruncated}
			for _, parent := range c.Parents {
				entry.Parents = append(entry.Parents, hex.EncodeToString(parent))
			}
			if !opt.FullCommitIDs {
				entry.ShortSHA, err = abbreviate(ctx, s.idx, &index{store: s.idx.store, cache: s.idx.cache}, entry.SHA)
				if err != nil {
					return err
				}
			}
			if err := s.abbreviateLogParents(ctx, &entry); err != nil {
				return err
			}
			if err := view.yield(func() error { return emit(entry) }); err != nil {
				return err
			}
			written++
			if !opt.Unlimited && written == opt.Count {
				return nil
			}
		}
		if len(next) > 1 {
			linear = false
		} else if linear {
			if err := walk.clearSeen(); err != nil {
				return err
			}
		}
		for _, i := range next {
			sha := hex.EncodeToString(c.Parents[i])
			// Keep immutable locations with the queued candidate: a delayed
			// merge parent can outlive every frame in the small decoded window.
			var location *pb.HistoryBatchLocation
			for _, item := range cache {
				if ordinal, ok := item.ordinals[sha]; ok {
					location = &pb.HistoryBatchLocation{Batch: item.ref, Ordinal: ordinal}
					break
				}
				if linked := item.links[sha]; linked != nil {
					location = linked
					break
				}
			}

			if err := walk.push(logCandidate{sha: sha, time: c.ParentTimes[i], location: location}); err != nil {
				return err
			}
		}
	}
	return nil
}

const historyFilterBytes = 2048

func historyFilterAdd(filter []byte, path string) {
	sum := sha256.Sum256([]byte(path))
	for i := 0; i < 4; i++ {
		bit := binary.LittleEndian.Uint32(sum[i*4:]) % (historyFilterBytes * 8)
		filter[bit/8] |= 1 << uint(bit%8)
	}
}
func historyFilterMatch(filter []byte, path string) bool {
	sum := sha256.Sum256([]byte(path))
	for i := 0; i < 4; i++ {
		bit := binary.LittleEndian.Uint32(sum[i*4:]) % (historyFilterBytes * 8)
		if filter[bit/8]&(1<<uint(bit%8)) == 0 {
			return false
		}
	}
	return true
}
