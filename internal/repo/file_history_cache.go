package repo

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// Keep a decoded container in one file in the existing bounded cache. History
// pages are small and numerous: caching each prefetched frame separately makes
// cold queries spend more time creating files than reading the object store.
func (p *Progressive) historyBytes(ctx context.Context, ref pageRef) ([]byte, error) {
	if !strings.HasPrefix(ref.Pack, "index/progressive-history-") || ref.Offset < 0 || ref.Length <= 0 || ref.Length > 128<<10 || ref.Offset > progressiveContainerBytes-ref.Length {
		return nil, fmt.Errorf("history metadata range bounds")
	}
	data, release, err := p.cache.borrow(ctx, "history-container/"+ref.Pack, func() ([]byte, error) {
		packed, _, err := p.store.Get(ctx, ref.Pack, 0, -1)
		if err != nil {
			return nil, err
		}
		if len(packed) == 0 || len(packed) > progressiveContainerBytes {
			return nil, fmt.Errorf("history container size")
		}
		pages, err := decodeDirectoryContainer(ctx, packed)
		if err != nil {
			return nil, err
		}
		container := &pb.FileHistoryCachedContainer{}
		for offset, page := range pages {
			container.Frames = append(container.Frames, &pb.FileHistoryCachedFrame{Offset: offset, Length: page.length, Hash: page.hash, Data: page.raw})
		}
		sort.Slice(container.Frames, func(i, j int) bool { return container.Frames[i].Offset < container.Frames[j].Offset })
		return proto.Marshal(container)
	})
	defer release()
	if errors.Is(err, errDirectoryExpansion) {
		return p.cache.load(ctx, "history-page/"+ref.Hash, func() ([]byte, error) {
			packed, _, err := p.store.Get(ctx, ref.Pack, ref.Offset, ref.Length)
			if err != nil {
				return nil, err
			}
			if fmt.Sprintf("%x", sha256.Sum256(packed)) != ref.Hash {
				return nil, fmt.Errorf("history checksum")
			}
			return decodeFrame(packed, 64<<10)
		})
	}
	if err != nil {
		return nil, err
	}
	// Scan protobuf frame envelopes in the mmap without copying unrelated pages.
	// Copy only the selected frame before releasing the cache mapping.
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if num != 1 || typ != protowire.BytesType {
			return nil, fmt.Errorf("invalid cached history container")
		}
		frame, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		var offset, length int64
		var hash, payload []byte
		for len(frame) > 0 {
			num, typ, n = protowire.ConsumeTag(frame)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			frame = frame[n:]
			switch {
			case (num == 1 || num == 2) && typ == protowire.VarintType:
				value, n := protowire.ConsumeVarint(frame)
				if n < 0 {
					return nil, protowire.ParseError(n)
				}
				frame = frame[n:]
				if num == 1 {
					offset = int64(value)
				} else {
					length = int64(value)
				}
			case (num == 3 || num == 4) && typ == protowire.BytesType:
				value, n := protowire.ConsumeBytes(frame)
				if n < 0 {
					return nil, protowire.ParseError(n)
				}
				frame = frame[n:]
				if num == 3 {
					hash = value
				} else {
					payload = value
				}
			default:
				return nil, fmt.Errorf("invalid cached history frame")
			}
		}
		if offset == ref.Offset {
			if length != ref.Length || string(hash) != ref.Hash {
				return nil, fmt.Errorf("history checksum")
			}
			return append([]byte(nil), payload...), nil
		}
	}
	return nil, fmt.Errorf("missing history frame")
}
