//go:build !js

package repo

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"sort"

	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

// IngestHistory indexes every path in every new reachable commit. It is a
// writer operation, independent of queries, result limits and cache contents.
// Work queues, parent dependencies and unpublished pages spill to the staging
// database in checkpointed batches. The decoder cache is capped at 32 MiB;
// directory comparisons and staging batches use additional temporary memory.
func (p *Progressive) IngestHistory(ctx context.Context, sha string, gitdirs ...string) error {
	return p.ingestHistoryTips(ctx, []string{sha}, gitdirs...)
}

func (p *Progressive) ingestHistoryTips(ctx context.Context, tips []string, gitdirs ...string) error {
	select {
	case p.historyBuild <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.historyBuild }()
	if err := ctx.Err(); err != nil {
		return err
	}
	pending := make([]string, 0, len(tips))
	for _, sha := range tips {
		if !validProgressiveOID(sha) {
			return fmt.Errorf("invalid history revision")
		}
		var existing pb.FileHistoryRoot
		if err := p.get(ctx, historyRootPrefix+sha, &existing); errors.Is(err, store.ErrNotFound) {
			pending = append(pending, sha)
		} else if err != nil {
			return err
		}
	}
	if len(pending) == 0 {
		return nil
	}

	source, err := openHistorySource(gitdirs)
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
	uploads := newPublicationUploads(ctx, p.store)
	defer uploads.close()
	writer := &indexWriter{ctx: ctx, store: uploads, prefix: "progressive-history-" + rand.Text(), packLimit: progressiveContainerBytes}
	return p.stageHistory(func(stage *historyStage) error {
		buckets := stage.buckets
		changes := buckets["changes"]
		put := func(b *bolt.Bucket, k string, m proto.Message) error { return progressivePut(b, k, m) }
		for _, sha := range pending {
			if err := buckets["queue"].Put([]byte(sha), []byte{1}); err != nil {
				return err
			}
		}
		discovered := 0
		for {
			if discovered%128 == 0 {
				if err := stage.checkpoint(); err != nil {
					return err
				}
				changes = buckets["changes"]
			}
			discovered++
			if err := ctx.Err(); err != nil {
				return err
			}
			k, _ := buckets["queue"].Cursor().First()
			if k == nil {
				break
			}
			id := string(k)
			if err := buckets["queue"].Delete(k); err != nil {
				return err
			}
			if buckets["seen"].Get([]byte(id)) != nil {
				continue
			}
			if err := buckets["seen"].Put([]byte(id), []byte{1}); err != nil {
				return err
			}
			var root pb.FileHistoryRoot
			if err := p.get(ctx, historyRootPrefix+id, &root); err == nil {
				if err = put(buckets["roots"], id, &root); err != nil {
					return err
				}
				continue
			} else if !errors.Is(err, store.ErrNotFound) {
				return err
			}
			raw, kind, err := p.object(ctx, id)
			if err != nil {
				if errors.Is(err, store.ErrNotFound) {
					return ErrHistoryIndexPending
				}
				return err
			}
			if kind != 1 {
				return fmt.Errorf("history expected commit")
			}
			var parents []string
			tree, info, err := parseBufferedCommitParents(bufio.NewReader(bytes.NewReader(raw)), &parents, 20)
			if err != nil {
				return err
			}
			if len(parents) > maxLogParents {
				return fmt.Errorf("too many history parents")
			}
			metadata, err := marshal(info)
			if err != nil {
				return err
			}
			var record pb.CommitRecord
			if err = proto.Unmarshal(metadata, &record); err != nil {
				return err
			}
			commit := &pb.FileHistoryCommit{Sha: id, Tree: tree, Parents: parents, Metadata: &record}
			if err = put(buckets["commits"], id, commit); err != nil {
				return err
			}
			for _, parent := range parents {
				if err = buckets["queue"].Put([]byte(parent), []byte{1}); err != nil {
					return err
				}
				if err = buckets["edges"].Put([]byte(parent+id), []byte{1}); err != nil {
					return err
				}
			}
		}
		err := buckets["commits"].ForEach(func(k, v []byte) error {
			var c pb.FileHistoryCommit
			if err := proto.Unmarshal(v, &c); err != nil {
				return err
			}
			n := 0
			unique := map[string]bool{}
			for _, parent := range c.Parents {
				if unique[parent] {
					continue
				}
				unique[parent] = true
				if buckets["roots"].Get([]byte(parent)) == nil {
					n++
				}
			}
			if n == 0 {
				return buckets["ready"].Put(k, []byte{1})
			}
			return buckets["pending"].Put(k, []byte{byte(n >> 8), byte(n)})
		})
		if err != nil {
			return err
		}
		b := &historyBuilder{p: p, ctx: ctx, pages: buckets["pages"], refs: buckets["page-refs"], cache: source.cache}
		b.save = func(m proto.Message) (pageRef, error) {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
			if err != nil {
				return pageRef{}, err
			}
			if len(raw) > 64<<10 {
				return pageRef{}, fmt.Errorf("history page exceeds 64 KiB")
			}
			compressed := enc.EncodeAll(raw, nil)
			hash := fmt.Sprintf("%x", sha256.Sum256(compressed))
			if saved := buckets["page-refs"].Get([]byte(hash)); saved != nil {
				var ref pb.PageReference
				if err := proto.Unmarshal(saved, &ref); err != nil {
					return pageRef{}, err
				}
				return decodePageRef(&ref), nil
			}
			ref, err := writer.saveBytes(compressed)
			if err != nil {
				return ref, err
			}
			if err := progressivePut(buckets["page-refs"], hash, encodePageRef(ref)); err != nil {
				return ref, err
			}

			if err = b.pages.Put([]byte(ref.Hash), raw); err != nil {
				return ref, err
			}
			b.cache.put("history-page/"+ref.Hash, raw)
			return ref, nil
		}
		built := 0
		for {
			if built%64 == 0 {
				if err := stage.checkpoint(); err != nil {
					return err
				}
				changes = buckets["changes"]
				b.pages = buckets["pages"]
				b.refs = buckets["page-refs"]
			}
			built++
			if err := ctx.Err(); err != nil {
				return err
			}
			k, _ := buckets["ready"].Cursor().First()
			if k == nil {
				break
			}
			id := string(k)
			if err := buckets["ready"].Delete(k); err != nil {
				return err
			}
			var c pb.FileHistoryCommit
			if err := proto.Unmarshal(buckets["commits"].Get([]byte(id)), &c); err != nil {
				return err
			}
			var parents []*pb.FileHistoryRoot
			for _, parent := range c.Parents {
				var root pb.FileHistoryRoot
				if raw := buckets["roots"].Get([]byte(parent)); raw == nil {
					return fmt.Errorf("history parent missing")
				} else if err := proto.Unmarshal(raw, &root); err != nil {
					return err
				}
				parents = append(parents, &root)
			}
			ref, err := b.save(&c)
			if err != nil {
				return err
			}
			b.commit = &c
			b.commitRef = encodePageRef(ref)
			b.parents = parents
			states := make([]*pb.FileHistoryState, len(parents))
			for i, root := range parents {
				states[i] = root.State
			}
			state, err := b.build(Entry{OID: c.Tree, Mode: 0040000}, states, 0)
			if err != nil {
				return err
			}
			root := &pb.FileHistoryRoot{State: state, Record: b.commitRef, Commit: &pb.FileHistoryCommit{Sha: c.Sha, Tree: c.Tree, Parents: c.Parents, Metadata: &pb.CommitRecord{CommitTime: c.Metadata.CommitTime}}}
			if err = put(buckets["roots"], id, root); err != nil {
				return err
			}
			if err = put(changes, historyRootPrefix+id, root); err != nil {
				return err
			}
			cursor := buckets["edges"].Cursor()
			for key, _ := cursor.Seek([]byte(id)); key != nil && bytes.HasPrefix(key, []byte(id)); key, _ = cursor.Next() {
				child := append([]byte(nil), key[40:]...)
				count := buckets["pending"].Get(child)
				if len(count) != 2 {
					return fmt.Errorf("invalid history dependency")
				}
				n := int(count[0])<<8 | int(count[1])
				n--
				if n == 0 {
					if err = buckets["ready"].Put(child, []byte{1}); err != nil {
						return err
					}
				}
				if err = buckets["pending"].Put(child, []byte{byte(n >> 8), byte(n)}); err != nil {
					return err
				}
			}
		}
		for _, sha := range pending {
			if buckets["roots"].Get([]byte(sha)) == nil {
				return fmt.Errorf("incomplete history dependency graph")
			}
		}
		if err := put(changes, "history/ready", &pb.ProgressiveState{HistoryComplete: true}); err != nil {
			return err
		}
		if err := writer.flush(); err != nil {
			return err
		}
		p.writer.Lock()
		defer p.writer.Unlock()
		return p.publishUploads(ctx, changes, uploads)
	})
}

type historyBuilder struct {
	p         *Progressive
	ctx       context.Context
	pages     *bolt.Bucket
	refs      *bolt.Bucket
	cache     *cache
	save      func(proto.Message) (pageRef, error)
	commit    *pb.FileHistoryCommit
	commitRef *pb.PageReference
	parents   []*pb.FileHistoryRoot
}

func sameHistoryValue(e Entry, s *pb.FileHistoryState) bool {
	if s == nil {
		return e.OID == ""
	}
	return e.OID == s.Oid && e.Mode == s.Mode
}
func (b *historyBuilder) read(ref *pb.PageReference, m proto.Message) error {
	if ref == nil {
		return nil
	}
	key := ref.Hash
	if err := progressivePut(b.refs, key, ref); err != nil {
		return err
	}
	if raw, ok := b.cache.get("history-page/" + key); ok {
		return proto.Unmarshal(raw, m)
	}
	if raw := b.pages.Get([]byte(key)); raw != nil {
		return proto.Unmarshal(raw, m)
	}
	raw, err := b.p.historyBytes(b.ctx, decodePageRef(ref))
	if err != nil {
		return err
	}
	b.cache.put("history-page/"+key, raw)
	return proto.Unmarshal(raw, m)
}
func (b *historyBuilder) directory(s *pb.FileHistoryState) (map[string]*pb.FileHistoryState, error) {
	out := make(map[string]*pb.FileHistoryState)
	if s == nil || s.Directory == nil {
		return out, nil
	}
	total := 0
	var visit func(*pb.PageReference, int) error
	visit = func(ref *pb.PageReference, depth int) error {
		if depth > 32 {
			return fmt.Errorf("history directory depth")
		}
		var page pb.FileHistoryDirectory
		if err := b.read(ref, &page); err != nil {
			return err
		}
		total += proto.Size(&page)
		if total > 64<<20 {
			return fmt.Errorf("history directory workspace exceeded")
		}
		for _, e := range page.Entries {
			out[string(e.Name)] = capHistoryState(e.State, s.DirectoryGate)
		}
		for _, c := range page.Children {
			if err := visit(c.Page, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	return out, visit(s.Directory, 0)
}
func (b *historyBuilder) saveDirectory(entries []*pb.FileHistoryEntry) (*pb.PageReference, error) {
	var children []*pb.DirectoryChild
	for start := 0; start < len(entries); {
		end := start + 1
		for end < len(entries) && proto.Size(&pb.FileHistoryDirectory{Entries: entries[start : end+1]}) <= progressivePageBytes {
			end++
		}
		ref, err := b.save(&pb.FileHistoryDirectory{Entries: entries[start:end]})
		if err != nil {
			return nil, err
		}
		children = append(children, &pb.DirectoryChild{MaxName: entries[end-1].Name, Page: encodePageRef(ref)})
		start = end
	}
	if len(children) == 0 {
		return nil, nil
	}
	for len(children) > 1 {
		var next []*pb.DirectoryChild
		for start := 0; start < len(children); start += 64 {
			end := min(start+64, len(children))
			ref, err := b.save(&pb.FileHistoryDirectory{Children: children[start:end]})
			if err != nil {
				return nil, err
			}
			next = append(next, &pb.DirectoryChild{MaxName: children[end-1].MaxName, Page: encodePageRef(ref)})
		}
		children = next
	}
	return children[0].Page, nil
}
func (b *historyBuilder) build(cur Entry, parents []*pb.FileHistoryState, depth int) (*pb.FileHistoryState, error) {
	if err := b.ctx.Err(); err != nil {
		return nil, err
	}
	if depth > 512 {
		return nil, fmt.Errorf("history path depth")
	}
	now := b.commit.Metadata.CommitTime
	if len(parents) > 0 && sameHistoryValue(cur, parents[0]) {
		return capHistoryState(parents[0], now), nil
	}
	result := &pb.FileHistoryState{Oid: cur.OID, Mode: cur.Mode}
	var cursors []*pb.FileHistoryCursor
	for i, state := range parents {
		prev := capHistoryState(state, int64(^uint64(0)>>1))
		prev.Directory = nil
		prev.DirectoryGate = 0
		cursors = append(cursors, &pb.FileHistoryCursor{Sha: b.parents[i].Commit.Sha, Time: b.parents[i].Commit.Metadata.CommitTime, State: prev})
	}
	if cur.OID != "" || len(parents) > 0 {
		event := &pb.FileHistoryEvent{Commit: b.commitRef, Parents: cursors}
		ref, err := b.save(event)
		if err != nil {
			return nil, err
		}
		link := &pb.FileHistoryLink{Event: encodePageRef(ref), Commit: b.commit.Sha, Time: now, Gate: now}
		result.Normal = link
		result.FirstParent = link
		for _, prev := range parents {
			if sameHistoryValue(cur, prev) {
				result.Normal = capHistoryState(prev, now).Normal
				break
			}
		}
	}
	hasDirectory := cur.Mode == 0040000
	for _, prev := range parents {
		hasDirectory = hasDirectory || (prev != nil && prev.Directory != nil)
	}
	if !hasDirectory {
		return result, nil
	}
	current := map[string]Entry{}
	names := map[string]bool{}
	if cur.Mode == 0040000 {
		raw, kind, err := b.p.object(b.ctx, cur.OID)
		if err != nil {
			return nil, err
		}
		if kind != 2 {
			return nil, fmt.Errorf("history expected tree")
		}
		entries, err := parseNativeTree(raw)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			current[e.Name] = Entry{OID: e.OID, Mode: e.Mode}
			names[e.Name] = true
		}
	}
	old := make([]map[string]*pb.FileHistoryState, len(parents))
	for i, prev := range parents {
		var err error
		old[i], err = b.directory(prev)
		if err != nil {
			return nil, err
		}
		for name := range old[i] {
			names[name] = true
		}
	}
	keys := make([]string, 0, len(names))
	for name := range names {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	var entries []*pb.FileHistoryEntry
	for _, name := range keys {
		ps := make([]*pb.FileHistoryState, len(parents))
		for i := range parents {
			ps[i] = old[i][name]
		}
		s, err := b.build(current[name], ps, depth+1)
		if err != nil {
			return nil, err
		}
		entries = append(entries, &pb.FileHistoryEntry{Name: []byte(name), State: s})
	}
	ref, err := b.saveDirectory(entries)
	if err != nil {
		return nil, err
	}
	result.Directory = ref
	result.DirectoryGate = now
	return result, nil
}

// bbolt retains all dirty nodes until commit. Checkpoint the unpublished work
// regularly; only the final object-store HEAD CAS makes any history visible.
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
	for _, name := range []string{"changes", "commits", "queue", "seen", "roots", "edges", "pending", "ready", "pages", "page-refs"} {
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
