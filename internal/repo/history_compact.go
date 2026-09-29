//go:build !js

package repo

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
)

const linkedHistoryGraphPrefix = "progressive-history-v2-graph-linked-"
const compactHistoryGraphPrefix = "progressive-history-v2-graph-packed-linked-"

// CompactHistory packs published graph frames since the nearest linked,
// compacted ancestor tips. Incomplete ancestry stops at uncovered commits;
// repacking never advances coverage or changes its frontier. It changes only
// immutable-frame locations, never traversal records, path postings, messages, or file contents. Old publications stay valid.
// Small updates accumulate at most three fragmented containers before packing.
// Larger completed batches also fill the newest container densely, even when
// they used fewer publications, so a first page need not span partial packs.
func (p *Progressive) CompactHistory(ctx context.Context, sha string) error {
	return p.compactHistory(ctx, sha, 0)
}

// A frame budget permits bounded maintenance of a still-growing prefix. A
// limited pass never claims that the whole ancestry has been compacted.
func (p *Progressive) compactHistory(ctx context.Context, sha string, frameBudget uint64) error {
	if !validProgressiveOID(sha) {
		return fmt.Errorf("invalid history revision")
	}
	select {
	case p.historyBuild <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-p.historyBuild }()
	state, err := p.HistoryProgress(ctx, sha)
	if err != nil {
		return err
	}
	closureComplete := state.Complete
	if state.GraphCompacted && state.GraphLinks {
		return nil
	}
	return p.stageHistory(func(stage *historyStage) error {
		for _, name := range []string{"compact-batches", "compact-commits", "compact-frames", "compact-containers"} {
			bucket, err := stage.tx.CreateBucket([]byte(name))
			if err != nil {
				return err
			}
			stage.buckets[name] = bucket
		}
		var frames uint64
		head, tail := uint64(1)<<63, uint64(1)<<63
		containers := 0
		needsLinks := false
		push := func(oid []byte, front bool) error {
			if len(oid) != 20 {
				return fmt.Errorf("invalid history parent")
			}
			var key [8]byte
			if front {
				if head == 0 {
					return fmt.Errorf("history compaction sequence exhausted")
				}
				head--
				binary.BigEndian.PutUint64(key[:], head)
			} else {
				if tail == ^uint64(0) {
					return fmt.Errorf("history compaction sequence exhausted")
				}
				binary.BigEndian.PutUint64(key[:], tail)
				tail++
			}
			return stage.buckets["queue"].Put(key[:], oid)
		}
		oid, _ := hex.DecodeString(sha)
		if err := push(oid, false); err != nil {
			return err
		}
		for visited := 0; ; visited++ {
			if frameBudget > 0 && frames >= frameBudget {
				closureComplete = false
				break
			}
			if visited%256 == 255 {
				if err := stage.checkpoint(); err != nil {
					return err
				}
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			key, value := stage.buckets["queue"].Cursor().First()
			if key == nil {
				break
			}
			id := hex.EncodeToString(value)
			if err := stage.buckets["queue"].Delete(key); err != nil {
				return err
			}
			if stage.buckets["seen"].Get([]byte(id)) != nil {
				continue
			}
			if err := stage.buckets["seen"].Put([]byte(id), []byte{1}); err != nil {
				return err
			}
			ancestor, err := p.HistoryProgress(ctx, id)
			if err != nil {
				return err
			}
			if ancestor.GraphCompacted && ancestor.GraphLinks {
				continue
			}
			data := stage.buckets["compact-commits"].Get([]byte(id))
			if data == nil {
				var location pb.HistoryBatchLocation
				if err := p.get(ctx, historyBatchKey+id, &location); err != nil {
					if errors.Is(err, store.ErrNotFound) && !closureComplete {
						continue
					}
					return err
				}
				batch, err := p.readHistoryBatch(ctx, &location)
				if err != nil {
					return err
				}
				if hex.EncodeToString(batch.Commits[location.Ordinal].Oid) != id {
					return fmt.Errorf("history commit identity mismatch")
				}
				for _, commit := range batch.Commits {
					if err := progressivePut(stage.buckets["compact-commits"], hex.EncodeToString(commit.Oid), commit); err != nil {
						return err
					}
				}
				// Already packed frames can be shared by different ancestry walks.
				// Keep their existing locations while checking closure for this tip.
				if !strings.HasPrefix(location.Batch.Pack, "index/"+linkedHistoryGraphPrefix) && !strings.HasPrefix(location.Batch.Pack, "index/"+compactHistoryGraphPrefix) {
					needsLinks = true
				}
				if !strings.HasPrefix(location.Batch.Pack, "index/"+compactHistoryGraphPrefix) && stage.buckets["compact-frames"].Get([]byte(location.Batch.Hash)) == nil {
					if err := stage.buckets["compact-frames"].Put([]byte(location.Batch.Hash), []byte{1}); err != nil {
						return err
					}
					if stage.buckets["compact-containers"].Get([]byte(location.Batch.Pack)) == nil {
						containers++
						if err := stage.buckets["compact-containers"].Put([]byte(location.Batch.Pack), []byte{1}); err != nil {
							return err
						}
					}
					var sequence [8]byte
					binary.BigEndian.PutUint64(sequence[:], frames)
					frames++
					raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(batch)
					if err != nil {
						return err
					}
					if err := stage.buckets["compact-batches"].Put(sequence[:], raw); err != nil {
						return err
					}
				}
				data = stage.buckets["compact-commits"].Get([]byte(id))
			}
			var commit pb.HistoryBatchCommit
			if err := proto.Unmarshal(data, &commit); err != nil {
				return err
			}
			for i, parent := range commit.Parents {
				if err := push(parent, i == 0); err != nil {
					return err
				}
			}
		}
		if frames == 0 || (!needsLinks && frameBudget == 0 && containers < 4 && (frames < 8 || containers < 2)) {
			return nil
		}
		enc, err := newCompressor()
		if err != nil {
			return err
		}
		defer enc.Close()
		uploads := newPublicationUploads(ctx, p.store)
		defer uploads.close()
		writer := p.historyWriter(ctx, uploads, compactHistoryGraphPrefix+rand.Text())
		if err := writeLinkedHistoryFrames(ctx, stage, "compact-batches", writer, enc); err != nil {
			return err
		}

		if err := writer.flush(); err != nil {
			return err
		}
		if err := uploads.wait(); err != nil {
			return err
		}
		metadata := newPublicationUploads(ctx, p.store)
		defer metadata.close()
		p.writer.Lock()
		defer p.writer.Unlock()
		// Preserve concurrently updated diagnostic state. Coverage is immutable.
		state, err := p.HistoryProgress(ctx, sha)
		if err != nil {
			return err
		}
		if closureComplete {
			state.GraphCompacted = true
			state.GraphLinks = true
		}
		if err := progressivePut(stage.buckets["changes"], historyIngestionKey+sha, state); err != nil {
			return err
		}
		return p.publishIndex(ctx, metadata, func(idx *index) (pageRef, error) {
			// Compaction changes scattered commit locations, not every object
			// recipe sharing their old containers. Retain only touched pages.
			idx.pageRanges = true
			return idx.update(ctx, stage.buckets["changes"])
		})
	})
}
