package repo

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"sort"
	"strings"
)

var ErrHistoryIndexPending = errors.New("file history index is not ready; history ingestion is still running")

const historyRootPrefix = "history/root/"

func capHistoryState(state *pb.FileHistoryState, gate int64) *pb.FileHistoryState {
	if state == nil {
		return &pb.FileHistoryState{}
	}
	s := proto.Clone(state).(*pb.FileHistoryState)
	if s.Normal != nil {
		s.Normal.Gate = min(s.Normal.Gate, gate)
	}
	if s.FirstParent != nil {
		s.FirstParent.Gate = min(s.FirstParent.Gate, gate)
	}
	if s.Directory != nil {
		s.DirectoryGate = min(s.DirectoryGate, gate)
	}
	return s
}
func (p *Progressive) HasHistoryIndex(ctx context.Context) (bool, error) {
	var state pb.ProgressiveState
	err := p.get(ctx, "history/ready", &state)
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Root and directory records are COW and include tombstones, so historical
// paths remain queryable after deletion or a directory-to-file replacement.
func (p *Progressive) fileHistoryState(ctx context.Context, sha, path string) (*pb.FileHistoryRoot, *pb.FileHistoryState, error) {
	var root pb.FileHistoryRoot
	if err := p.get(ctx, historyRootPrefix+sha, &root); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, ErrHistoryIndexPending
		}
		return nil, nil, err
	}
	state := root.State
	if state == nil || root.Commit == nil || root.Commit.Metadata == nil {
		return nil, nil, fmt.Errorf("invalid history root")
	}
	if path == "." || path == "" {
		return &root, state, nil
	}
	for _, name := range strings.Split(path, "/") {
		if state.Directory == nil {
			return &root, &pb.FileHistoryState{}, nil
		}
		gate := state.DirectoryGate
		ref := state.Directory
		for depth := 0; ; depth++ {
			if depth > 32 {
				return nil, nil, fmt.Errorf("history directory depth")
			}
			var page pb.FileHistoryDirectory
			if err := p.readHistoryPage(ctx, ref, &page); err != nil {
				return nil, nil, err
			}
			if len(page.Children) > 0 {
				i := sort.Search(len(page.Children), func(i int) bool { return string(page.Children[i].MaxName) >= name })
				if i == len(page.Children) {
					return &root, &pb.FileHistoryState{}, nil
				}
				ref = page.Children[i].Page
				continue
			}
			i := sort.Search(len(page.Entries), func(i int) bool { return string(page.Entries[i].Name) >= name })
			if i == len(page.Entries) || string(page.Entries[i].Name) != name {
				return &root, &pb.FileHistoryState{}, nil
			}
			state = capHistoryState(page.Entries[i].State, gate)
			break
		}
	}
	return &root, state, nil
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
