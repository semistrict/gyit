package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	pb "gyit/internal/gen/gyit/storage/v1"
)

type historyIngestCommit struct {
	*pb.HistoryBatchCommit
	metadata *pb.CommitRecord
}

func (p *Progressive) historyBatchCommit(ctx context.Context, sha string) (*historyIngestCommit, string, error) {
	raw, release, err := p.borrowObject(ctx, sha)
	defer release()
	if err != nil {
		return nil, "", err
	}
	if len(raw) == 0 || raw[0] != 1 {
		return nil, "", fmt.Errorf("history expected commit")
	}
	var parents []string
	// The parser owns its returned fields. Borrowing avoids copying the entire
	// decoded commit, and a small read buffer suffices for bounded header lines.
	tree, info, err := parseBufferedCommitParents(bufio.NewReaderSize(bytes.NewReader(raw[1:]), 256), &parents, 20)
	if err != nil {
		return nil, "", err
	}
	if len(parents) > maxLogParents {
		return nil, "", fmt.Errorf("too many history parents")
	}
	record := encodeCommitInfo(info)
	oid, _ := hex.DecodeString(sha)
	c := &historyIngestCommit{HistoryBatchCommit: &pb.HistoryBatchCommit{Oid: oid}, metadata: record}
	for _, id := range parents {
		b, err := hex.DecodeString(id)
		if err != nil {
			return nil, "", err
		}
		c.Parents = append(c.Parents, b)
	}
	return c, tree, nil
}
