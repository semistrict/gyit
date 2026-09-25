package repo

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCorrectnessReaderOracleParser(t *testing.T) {
	row := fmt.Sprintf("100755 blob %040x 17\tfile\twith\nbytes\x00040000 tree %040x -\tdir\x00160000 commit %040x -\tsubmodule\x00", 1, 2, 3)
	got, err := correctnessParseTree([]byte(row))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Name != "dir" || got[0].Mode != 0040000 || got[0].Size != 0 || got[1].Name != "file\twith\nbytes" || got[1].Mode != 0100755 || got[1].Size != 17 || got[2].Mode != 0160000 {
		t.Fatalf("unexpected oracle entries: %+v", got)
	}
	for _, bad := range [][]byte{[]byte("unterminated"), []byte("100644 blob nope 1\tx\x00"), []byte(fmt.Sprintf("100644 blob %040x -1\tx\x00", 1)), append([]byte(row), []byte(row)...)} {
		if _, err := correctnessParseTree(bad); err == nil {
			t.Fatal("invalid oracle output accepted")
		}
	}
}
func TestCorrectnessReaderOracleOutputBound(t *testing.T) {
	var b bytes.Buffer
	w := &correctnessLimitedWriter{out: &b, remaining: 3}
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("d")); err == nil {
		t.Fatal("oracle output bound ignored")
	}
	if b.String() != "abc" {
		t.Fatal("out-of-bound data escaped")
	}
}
