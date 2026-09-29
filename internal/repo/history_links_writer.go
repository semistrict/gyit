//go:build !js

package repo

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// Rewrite a disk-backed sequence in reverse. Destinations already written in
// this publication may be linked; other edges keep their global-index lookup.
// This prevents immutable-reference cycles and never references a future CAS.
// One frame is decoded at a time and dirty staging pages checkpoint every 64.
func writeLinkedHistoryFrames(ctx context.Context, stage *historyStage, bucket string, writer *indexWriter, enc *zstd.Encoder) error {
	cursor := stage.buckets[bucket].Cursor()
	rewritten := 0
	for key, raw := cursor.Last(); key != nil; key, raw = cursor.Prev() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(raw) > 64<<10 {
			return fmt.Errorf("history graph page bounds")
		}
		var batch pb.HistoryBatch
		if err := proto.Unmarshal(raw, &batch); err != nil {
			return err
		}
		// Compaction must not leave links pointing back to the fragmented frames.
		batch.Links = nil
		present := make(map[string]bool, len(batch.Commits))
		for _, c := range batch.Commits {
			present[string(c.Oid)] = true
		}
		size := proto.Size(&batch)
		for _, c := range batch.Commits {
			for _, parent := range c.Parents {
				if present[string(parent)] || len(batch.Links) >= 128 {
					continue
				}
				present[string(parent)] = true
				data := stage.buckets["changes"].Get([]byte(historyBatchKey + hex.EncodeToString(parent)))
				if data == nil {
					continue
				}
				var location pb.HistoryBatchLocation
				if err := proto.Unmarshal(data, &location); err != nil {
					return err
				}
				if location.Batch == nil {
					return fmt.Errorf("history link missing frame")
				}
				link := &pb.HistoryBatchLink{CommitOid: bytes.Clone(parent), Location: &location}
				added := proto.Size(&pb.HistoryBatch{Links: []*pb.HistoryBatchLink{link}})
				if size+added > 64<<10 {
					continue
				}
				batch.Links = append(batch.Links, link)
				size += added
			}
		}
		encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(&batch)
		if err != nil {
			return err
		}
		ref, err := writer.saveBytes(enc.EncodeAll(encoded, nil))
		if err != nil {
			return err
		}
		for ordinal, c := range batch.Commits {
			if err := progressivePut(stage.buckets["changes"], historyBatchKey+hex.EncodeToString(c.Oid), &pb.HistoryBatchLocation{Batch: encodePageRef(ref), Ordinal: uint32(ordinal)}); err != nil {
				return err
			}
		}
		rewritten++
		if rewritten%64 == 0 {
			after := bytes.Clone(key)
			if err := stage.checkpoint(); err != nil {
				return err
			}
			cursor = stage.buckets[bucket].Cursor()
			cursor.Seek(after)
		}
	}
	return nil
}
