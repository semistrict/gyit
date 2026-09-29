package repo

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

var ErrHistoryIndexPending = errors.New("file history coverage is incomplete")

func (p *Progressive) HasHistoryIndex(ctx context.Context) (bool, error) {
	items, err := p.index().scan(ctx, historyBatchKey, "", 1)
	return len(items) != 0, err
}

func (p *Progressive) readHistoryPage(ctx context.Context, ref *pb.PageReference, out proto.Message) error {
	if ref == nil {
		return fmt.Errorf("missing history page")
	}
	raw, err := p.historyBytes(ctx, decodePageRef(ref))
	if err != nil {
		return err
	}
	return proto.Unmarshal(raw, out)
}
