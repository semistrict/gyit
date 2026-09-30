//go:build !js

package repo

import (
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/storage/v1"
)

// Ingestion drains its queue from the front within one staging transaction,
// leaving empty bbolt leaves until commit. The frontier is written by reverse
// iteration, which must neither stop early at an emptied leaf nor spin on an
// emptied multi-page bucket.
func TestHistoryFrontierAfterDrainingQueue(t *testing.T) {
	const queued = 4000 // Many leaves, even with 16 KiB pages.
	for name, keep := range map[string][2]int{
		"middle-drained": {200, queued - 200},
		"fully-drained":  {queued, queued},
	} {
		t.Run(name, func(t *testing.T) {
			p := &Progressive{temp: t.TempDir()}
			var want []string
			var got []string
			err := p.stageHistory(func(stage *historyStage) error {
				queue := stage.buckets["queue"]
				for i := range queued {
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], uint64(i))
					if err := queue.Put(key[:], []byte(fmt.Sprintf("%040x", i))); err != nil {
						return err
					}
				}
				if err := stage.checkpoint(); err != nil {
					return err
				}
				queue = stage.buckets["queue"]
				for i := range queued {
					if i < keep[0] || i >= keep[1] {
						want = append(want, fmt.Sprintf("%040x", i))
						continue
					}
					var key [8]byte
					binary.BigEndian.PutUint64(key[:], uint64(i))
					if err := queue.Delete(key[:]); err != nil {
						return err
					}
				}
				pages := map[string]*pb.HistoryFrontierPage{}
				save := func(m proto.Message) (*pb.PageReference, error) {
					hash := fmt.Sprint(len(pages))
					pages[hash] = proto.Clone(m).(*pb.HistoryFrontierPage)
					return &pb.PageReference{Hash: hash}, nil
				}
				done := make(chan struct{})
				var head *pb.PageReference
				var err error
				go func() {
					defer close(done)
					head, err = writeHistoryFrontier(stage, save)
				}()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					return fmt.Errorf("frontier write did not finish")
				}
				if err != nil {
					return err
				}
				for ref := head; ref != nil; ref = pages[ref.Hash].Next {
					for _, oid := range pages[ref.Hash].Commits {
						got = append(got, fmt.Sprintf("%x", oid))
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Fatalf("frontier has %d commits, want %d", len(got), len(want))
			}
		})
	}
}
