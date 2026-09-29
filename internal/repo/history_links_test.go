package repo

import (
	"encoding/hex"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// Only the tip has a global location in this fixture. Every remaining graph
// frame must be reached through its checked immutable parent link.
func TestHistoryFollowsFrameLinks(t *testing.T) {
	testHistoryFrameLinks(t, 3, false)
}

func TestHistoryFrameLinksSurviveDeferredMergeParent(t *testing.T) {
	testHistoryFrameLinks(t, 12, true)
}

func testHistoryFrameLinks(t *testing.T, count int, deferred bool) {
	backend, ids, _, _ := historyPrefetchFixture(t, count, false)
	p, e := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	enc, e := newCompressor()
	if e != nil {
		t.Fatal(e)
	}
	defer enc.Close()
	w := &indexWriter{ctx: t.Context(), store: backend, prefix: "progressive-history-v2-graph-linked-test", packLimit: progressiveContainerBytes}
	var next, last *pb.HistoryBatchLocation
	for i := len(ids) - 1; i >= 0; i-- {
		var old pb.HistoryBatchLocation
		if e = p.get(t.Context(), historyBatchKey+ids[i], &old); e != nil {
			t.Fatal(e)
		}
		batch, e := p.readHistoryBatch(t.Context(), &old)
		if e != nil {
			t.Fatal(e)
		}
		if next != nil {
			oid, _ := hex.DecodeString(ids[i+1])
			batch.Links = []*pb.HistoryBatchLink{{CommitOid: oid, Location: next}}
		}
		if deferred && i == len(ids)-2 {
			batch.Commits[0].Parents, batch.Commits[0].ParentTimes, batch.Links = nil, nil, nil
		}
		if deferred && i == 0 {
			oid, _ := hex.DecodeString(ids[len(ids)-1])
			batch.Commits[0].Parents = append(batch.Commits[0].Parents, oid)
			batch.Commits[0].ParentTimes = append(batch.Commits[0].ParentTimes, -1)
			batch.Links = append(batch.Links, &pb.HistoryBatchLink{CommitOid: oid, Location: last})
			paths, err := proto.Marshal(&pb.HistoryPathPage{Entries: []*pb.HistoryPathPosting{{Path: []byte("file"), Ordinals: []uint32{0}, DifferentParents: [][]byte{{3}}}}})
			if err != nil {
				t.Fatal(err)
			}
			ref, err := w.saveBytes(enc.EncodeAll(paths, nil))
			if err != nil {
				t.Fatal(err)
			}
			batch.Paths = encodePageRef(ref)
		}
		raw, e := proto.Marshal(batch)
		if e != nil {
			t.Fatal(e)
		}
		ref, e := w.saveBytes(enc.EncodeAll(raw, nil))
		if e != nil {
			t.Fatal(e)
		}
		next = &pb.HistoryBatchLocation{Batch: encodePageRef(ref)}
		if last == nil {
			last = next
		}
	}
	if e = w.flush(); e != nil {
		t.Fatal(e)
	}
	location, e := proto.Marshal(next)
	if e != nil {
		t.Fatal(e)
	}
	state, e := proto.Marshal(&pb.HistoryIngestion{Complete: true, CoveredCommits: uint64(count)})
	if e != nil {
		t.Fatal(e)
	}
	index := &indexWriter{ctx: t.Context(), store: backend, prefix: "linked-tip-index"}
	root, e := index.save(page{Items: []item{{Key: historyBatchKey + ids[0], Value: location}, {Key: historyIngestionKey + ids[0], Value: state}}})
	if e != nil {
		t.Fatal(e)
	}
	if e = index.flush(); e != nil {
		t.Fatal(e)
	}
	manifest, e := proto.Marshal(&pb.ProgressiveManifest{Version: 1, Index: encodePageRef(root.ID)})
	if e != nil {
		t.Fatal(e)
	}
	if e = backend.Put(t.Context(), "HEAD", manifest, p.token); e != nil {
		t.Fatal(e)
	}
	reader := directoryReader(t, &historyReadOnlyStore{Store: backend}, t.TempDir(), 32<<20)
	snapshot := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
	var got []string
	e = snapshot.LogWithOptions(t.Context(), LogOptions{Count: count, Paths: []string{"file"}, FullCommitIDs: true}, func(v LogEntry) error { got = append(got, v.SHA); return nil })
	if e != nil || fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("linked history %v, want %v: %v", got, ids, e)
	}
}

func TestHistoryRejectsInvalidFrameLinks(t *testing.T) {
	for _, mode := range []string{"foreign-parent", "duplicate", "ordinal", "missing-location", "wrong-destination"} {
		t.Run(mode, func(t *testing.T) {
			backend, ids, _, _ := historyPrefetchFixture(t, 2, false)
			p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var first, second pb.HistoryBatchLocation
			if err := p.get(t.Context(), historyBatchKey+ids[0], &first); err != nil {
				t.Fatal(err)
			}
			if err := p.get(t.Context(), historyBatchKey+ids[1], &second); err != nil {
				t.Fatal(err)
			}
			batch, err := p.readHistoryBatch(t.Context(), &first)
			if err != nil {
				t.Fatal(err)
			}
			parent, _ := hex.DecodeString(ids[1])
			link := &pb.HistoryBatchLink{CommitOid: parent, Location: &second}
			batch.Links = []*pb.HistoryBatchLink{link}
			switch mode {
			case "foreign-parent":
				link.CommitOid = make([]byte, 20)
			case "duplicate":
				batch.Links = append(batch.Links, link)
			case "ordinal":
				link.Location.Ordinal = 64
			case "missing-location":
				link.Location = nil
			case "wrong-destination":
				link.Location = &first
			}
			enc, err := newCompressor()
			if err != nil {
				t.Fatal(err)
			}
			defer enc.Close()
			raw, err := proto.Marshal(batch)
			if err != nil {
				t.Fatal(err)
			}
			writer := &indexWriter{ctx: t.Context(), store: backend, prefix: "progressive-history-v2-graph-invalid", packLimit: progressiveContainerBytes}
			ref, err := writer.saveBytes(enc.EncodeAll(raw, nil))
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.flush(); err != nil {
				t.Fatal(err)
			}
			bad := &pb.HistoryBatchLocation{Batch: encodePageRef(ref)}
			if mode != "wrong-destination" {
				if _, err := p.readHistoryBatch(t.Context(), bad); err == nil {
					t.Fatal("malformed history link accepted")
				}
				return
			}
			// A structurally valid link must still prove the requested commit identity
			// when followed. The first result is valid; the linked parent is not.
			location, err := proto.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			iw := &indexWriter{ctx: t.Context(), store: backend, prefix: "invalid-destination-index"}
			root, err := iw.save(page{Items: []item{{Key: historyBatchKey + ids[0], Value: location}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := iw.flush(); err != nil {
				t.Fatal(err)
			}
			manifest, err := proto.Marshal(&pb.ProgressiveManifest{Version: 1, Index: encodePageRef(root.ID)})
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.Put(t.Context(), "HEAD", manifest, p.token); err != nil {
				t.Fatal(err)
			}
			reader := directoryReader(t, backend, t.TempDir(), 32<<20)
			snap := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
			count := 0
			err = snap.LogWithOptions(t.Context(), LogOptions{Count: 2, Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { count++; return nil })
			if err == nil || count != 1 {
				t.Fatalf("bad destination returned %d results: %v", count, err)
			}
		})
	}
}
