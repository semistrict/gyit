//go:build !js

package repo

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

func TestHistoryWriterLinksFramesAcrossCheckpoint(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 130, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	enc, err := newCompressor()
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	err = p.stageHistory(func(stage *historyStage) error {
		bucket, err := stage.tx.CreateBucket([]byte("frames"))
		if err != nil {
			return err
		}
		stage.buckets["frames"] = bucket
		for i, id := range ids {
			var old pb.HistoryBatchLocation
			if err := p.get(t.Context(), historyBatchKey+id, &old); err != nil {
				return err
			}
			batch, err := p.readHistoryBatch(t.Context(), &old)
			if err != nil {
				return err
			}
			raw, err := proto.Marshal(batch)
			if err != nil {
				return err
			}
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(i))
			if err := bucket.Put(key[:], raw); err != nil {
				return err
			}
		}
		writer := p.historyWriter(t.Context(), backend, "progressive-history-v2-graph-linked-writer-test")
		if err := writeLinkedHistoryFrames(t.Context(), stage, "frames", writer, enc); err != nil {
			return err
		}
		if err := writer.flush(); err != nil {
			return err
		}
		for i, id := range ids {
			var location pb.HistoryBatchLocation
			if err := proto.Unmarshal(stage.buckets["changes"].Get([]byte(historyBatchKey+id)), &location); err != nil {
				return err
			}
			batch, err := p.readHistoryBatch(t.Context(), &location)
			if err != nil {
				return err
			}
			if i == len(ids)-1 {
				if len(batch.Links) != 0 {
					return fmt.Errorf("root has links")
				}
				continue
			}
			if len(batch.Links) != 1 || hex.EncodeToString(batch.Links[0].CommitOid) != ids[i+1] {
				return fmt.Errorf("frame %d lost parent link", i)
			}
			var want pb.HistoryBatchLocation
			if err := proto.Unmarshal(stage.buckets["changes"].Get([]byte(historyBatchKey+ids[i+1])), &want); err != nil {
				return err
			}
			if !proto.Equal(batch.Links[0].Location, &want) {
				return fmt.Errorf("frame %d links to old location", i)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCompactionLinksIncompleteHistoryWithoutChangingCoverage(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 3, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// A pending frontier is durable, even though this tiny branch is covered.
	state := &pb.HistoryIngestion{CoveredCommits: 3, Frontier: &pb.PageReference{Pack: "pending-frontier"}, Error: "prior diagnostic"}
	err = p.stageHistory(func(stage *historyStage) error {
		if err := progressivePut(stage.buckets["changes"], historyIngestionKey+ids[0], state); err != nil {
			return err
		}
		return p.publish(t.Context(), stage.buckets["changes"])
	})
	if err != nil {
		t.Fatal(err)
	}
	var before pb.HistoryBatchLocation
	if err := p.get(t.Context(), historyBatchKey+ids[0], &before); err != nil {
		t.Fatal(err)
	}
	if err := p.CompactHistory(t.Context(), ids[0]); err != nil {
		t.Fatal(err)
	}
	after, err := p.HistoryProgress(t.Context(), ids[0])
	if err != nil || !proto.Equal(state, after) {
		t.Fatalf("coverage changed: %v: %v", after, err)
	}
	var current pb.HistoryBatchLocation
	if err := p.get(t.Context(), historyBatchKey+ids[0], &current); err != nil {
		t.Fatal(err)
	}
	batch, err := p.readHistoryBatch(t.Context(), &current)
	if err != nil || len(batch.Links) != 1 {
		t.Fatalf("missing parent link: %v %v", batch, err)
	}
	old, err := p.readHistoryBatch(t.Context(), &before)
	if err != nil || len(old.Links) != 0 {
		t.Fatalf("old publication changed: %v %v", old, err)
	}
}

func TestCompactionUpgradesLegacyPackedHistory(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 3, false)
	counted := &progressiveCountStore{Store: backend}
	p, err := NewProgressive(t.Context(), counted, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = p.stageHistory(func(stage *historyStage) error {
		if err := progressivePut(stage.buckets["changes"], historyIngestionKey+ids[0], &pb.HistoryIngestion{Complete: true, CoveredCommits: 3, GraphCompacted: true}); err != nil {
			return err
		}
		return p.publish(t.Context(), stage.buckets["changes"])
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.CompactHistory(t.Context(), ids[0]); err != nil {
		t.Fatal(err)
	}
	state, err := p.HistoryProgress(t.Context(), ids[0])
	if err != nil || !state.Complete || !state.GraphCompacted || !state.GraphLinks || state.CoveredCommits != 3 {
		t.Fatalf("upgrade changed closure: %v %v", state, err)
	}
	for i, id := range ids[:len(ids)-1] {
		var location pb.HistoryBatchLocation
		if err := p.get(t.Context(), historyBatchKey+id, &location); err != nil {
			t.Fatal(err)
		}
		batch, err := p.readHistoryBatch(t.Context(), &location)
		if err != nil || len(batch.Links) != 1 {
			t.Fatalf("missing upgraded link: %v %v", batch, err)
		}
		var target pb.HistoryBatchLocation
		if err := p.get(t.Context(), historyBatchKey+ids[i+1], &target); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(batch.Links[0].Location, &target) {
			t.Fatal("compaction retained an old destination")
		}
	}
	before := counted.puts
	if err := p.CompactHistory(t.Context(), ids[0]); err != nil {
		t.Fatal(err)
	}
	if counted.puts != before {
		t.Fatal("linked compacted closure was rewritten")
	}
}

func TestBoundedHistoryCompactionPreservesFullCoverage(t *testing.T) {
	backend, ids, _, _ := historyPrefetchFixture(t, 8, false)
	p, err := NewProgressive(t.Context(), backend, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	before, err := p.HistoryProgress(t.Context(), ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := p.compactHistory(t.Context(), ids[0], 3); err != nil {
		t.Fatal(err)
	}
	after, err := p.HistoryProgress(t.Context(), ids[0])
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, after) {
		t.Fatalf("bounded pass changed coverage/closure: %v => %v", before, after)
	}
	var location pb.HistoryBatchLocation
	if err := p.get(t.Context(), historyBatchKey+ids[0], &location); err != nil {
		t.Fatal(err)
	}
	batch, err := p.readHistoryBatch(t.Context(), &location)
	if err != nil || len(batch.Links) != 1 {
		t.Fatalf("missing prefix link: %v %v", batch, err)
	}
	reader := directoryReader(t, backend, t.TempDir(), 32<<20)
	snap := &Snapshot{SHA: ids[0], progressive: reader, idx: reader.index()}
	var got []string
	if err := snap.LogWithOptions(t.Context(), LogOptions{Count: len(ids), Paths: []string{"file"}, FullCommitIDs: true}, func(e LogEntry) error { got = append(got, e.SHA); return nil }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(ids) {
		t.Fatalf("prefix/boundary history %v, want %v", got, ids)
	}
}
