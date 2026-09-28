package repo

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCommitMetadataBounds(t *testing.T) {
	for _, size := range []int{maxLogMessage, maxLogMessage + 1, 2 << 20} {
		author := strings.Repeat("a", maxLogAuthor+2000)
		raw := fmt.Sprintf("tree %s\nauthor %s <a@example.test> 1000000000 -0730\ncommitter Test <a@example.test> 1000000010 +0530\ngpgsig %s\n continuation\n\n%s", strings.Repeat("a", 40), author, strings.Repeat("s", 100000), strings.Repeat("m", size))
		reader := bytes.NewReader([]byte(raw))
		_, info, err := parseCommit(reader)
		if err != nil {
			t.Fatal(err)
		}
		if len(info.Author) != maxLogAuthor || !info.AuthorTruncated || info.AuthorTime != 1000000000 || info.AuthorOffset != -450 || info.CommitTime != 1000000010 || string(info.Committer) != "Test <a@example.test>" || info.CommitterOffset != 330 || !info.HasCommitter {
			t.Fatal("invalid bounded identity", info.AuthorTime, info.AuthorOffset, len(info.Author))
		}
		if len(info.Message) != maxLogMessage || info.MessageTruncated != (size > maxLogMessage) || reader.Len() != 0 {
			t.Fatal("message truncation/draining", len(info.Message), info.MessageTruncated, reader.Len())
		}
		data, err := marshal(info)
		if err != nil {
			t.Fatal(err)
		}
		var decoded commitInfo
		if err := unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decoded.Message, info.Message) || !decoded.AuthorTruncated || !bytes.Equal(decoded.Committer, info.Committer) || decoded.CommitterOffset != info.CommitterOffset || !decoded.HasCommitter {
			t.Fatal("metadata encoding lost bounds")
		}
	}
}
