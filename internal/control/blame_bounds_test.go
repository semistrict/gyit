package control

import (
	"bytes"
	"strings"
	"testing"

	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
	"google.golang.org/protobuf/proto"
)

func TestBlameResponsePermittedMaximaFitFrame(t *testing.T) {
	path := strings.Repeat("p", 4096)
	line := repo.BlameLine{
		SHA: strings.Repeat("a", 64), PreviousSHA: strings.Repeat("b", 64),
		Path: path, PreviousPath: path,
		Author: bytes.Repeat([]byte("a"), 4<<10), Committer: bytes.Repeat([]byte("c"), 4<<10),
		Message: bytes.Repeat([]byte("s"), 24<<10), Content: bytes.Repeat([]byte("l"), 24<<10),
		AuthorTime: -1, CommitterTime: -1, AuthorOffset: -1439, CommitterOffset: -1439,
		OriginalLine: 8 << 20, FinalLine: 8 << 20, GroupLines: 8 << 20,
		HasCommitter: true, Boundary: true, AuthorTruncated: true, CommitterTruncated: true, MessageTruncated: true,
	}
	response := blameResponse(line)
	encoded, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxFrame {
		t.Fatalf("permitted blame fields exceed frame: %d > %d", len(encoded), maxFrame)
	}
	var stream bytes.Buffer
	if err := writeFrame(&stream, response); err != nil {
		t.Fatal(err)
	}
	var decoded pb.Response
	if err := readFrame(&stream, &decoded); err != nil {
		t.Fatal(err)
	}
	got := decoded.GetBlameLine()
	if got == nil || len(got.PreviousPath) != 0 || string(got.Path) != path || !bytes.Equal(got.Committer, line.Committer) || !bytes.Equal(got.Content, line.Content) || !bytes.Equal(got.Summary, line.Message) || got.PreviousSha != line.PreviousSHA {
		t.Fatal("bounded encoding dropped actual blame data")
	}
	t.Logf("all permitted maxima encode to %d bytes below %d-byte cap", len(encoded), maxFrame)
}
