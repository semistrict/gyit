package control

import (
	"bytes"
	"strings"
	"testing"

	pb "gat/internal/gen/gat/control/v1"
)

func TestWideLogResponseFitsFrame(t *testing.T) {
	parents := make([]string, 512)
	for i := range parents {
		parents[i] = strings.Repeat("f", 64)
	}
	response := &pb.Response{Version: Version, Result: &pb.Response_LogEntry{LogEntry: &pb.LogEntry{
		Sha: parents[0], ShortSha: parents[0], Parents: parents,
		ParentAbbrevLengths: logParentLengths(parents),
		Author:              bytes.Repeat([]byte{'a'}, 4096), Message: bytes.Repeat([]byte{'m'}, 24576),
		AuthorTime: -1, AuthorOffsetMinutes: -1, MessageTruncated: true, AuthorTruncated: true,
	}}}
	var wire bytes.Buffer
	if err := writeFrame(&wire, response); err != nil {
		t.Fatal("maximum supported merge must fit a control frame", err)
	}
	var decoded pb.Response
	if err := readFrame(&wire, &decoded); err != nil {
		t.Fatal(err)
	}
	entry := decoded.GetLogEntry()
	if len(entry.Parents) != 512 || len(entry.ParentAbbrevLengths) != 512 || entry.ParentAbbrevLengths[511] != 64 {
		t.Fatal("wide merge lost parent identities or prefix lengths")
	}
}
