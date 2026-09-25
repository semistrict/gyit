package repo

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gat/internal/store"
)

// This helper reads only retained records from an unpublished, captured manifest.
// It deliberately cannot establish directory or mount correctness.
func ceilingCheckRetained(t *testing.T, backend store.Store, proposed manifest, source, tip string, paths []string) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if proposed.Root == (pageRef{}) || proposed.History == (pageRef{}) || proposed.HistoryCount == 0 {
		t.Fatal("retained readback requires completed catalog and history")
	}
	if manifestHasGlobalSizes(proposed.Version) && proposed.Blobs == (pageRef{}) {
		t.Fatal("archive readback requires a completed direct blob index")
	}
	if len(paths) == 0 || len(paths) > 16 {
		t.Fatal("retained readback requires 1..16 selected blob paths")
	}
	shared := newCache(DefaultCacheBytes)
	idx := &index{store: backend, cache: shared, root: proposed.Root, blobRoot: proposed.Blobs}
	history := &index{store: backend, cache: shared, root: proposed.History}
	cursor := &historyCursor{idx: history}
	var rows []map[string]any
	record := func(operation, sha, path string, oracleSeconds float64, started time.Time) {
		seconds := time.Since(started).Seconds()
		row := map[string]any{"operation": operation, "sha": sha, "seconds": seconds, "over_1s": seconds > 1, "oracle_seconds": oracleSeconds, "oracle_over_1s": oracleSeconds > 1, "ok": true}
		if path != "" {
			row["path"] = path
		}
		rows = append(rows, row)
	}
	oracle := func(input string, args ...string) []byte {
		t.Helper()
		commandCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		cmd := git(commandCtx, source, args...)
		cmd.Stdin = strings.NewReader(input)
		var output ceilingRetainedOutput
		var stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &output, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("retained oracle %v: %v: %s", args, err, stderr.Bytes())
		}
		return output.Bytes()
	}
	var tipObject object
	commits := []string{tip}
	for i := 0; i < len(commits); i++ {
		sha := commits[i]
		oracleStart := time.Now()
		raw := oracle("", "cat-file", "commit", sha)
		wantTree, wantInfo := ceilingRetainedCommit(t, raw)
		parentFields := strings.Fields(string(oracle("", "rev-list", "--parents", "--max-count=1", sha)))
		if len(parentFields) == 0 || parentFields[0] != sha {
			t.Fatalf("invalid retained parent oracle for %s", sha)
		}
		wantParents := parentFields[1:]
		oracleSeconds := time.Since(oracleStart).Seconds()
		started := time.Now()
		var o object
		if err := idx.get(ctx, "o/"+sha, &o); err != nil {
			t.Fatal(err)
		}
		if o.Kind != "commit" || o.Size != int64(len(raw)) || o.Tree != wantTree {
			t.Fatalf("retained commit identity differs for %s: %+v", sha, o)
		}
		var info commitInfo
		if err := idx.get(ctx, "c/"+sha, &info); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(info, wantInfo) {
			t.Fatalf("retained commit display metadata differs for %s", sha)
		}
		var p parents
		if err := idx.get(ctx, "p/"+sha, &p); err != nil {
			t.Fatal(err)
		}
		if strings.Join(p.Parents, " ") != strings.Join(wantParents, " ") {
			t.Fatalf("retained parents differ for %s", sha)
		}
		var position historyPosition
		if err := history.get(ctx, "g/"+sha, &position); err != nil {
			t.Fatal(err)
		}
		if position == 0 || uint64(position) > proposed.HistoryCount {
			t.Fatalf("retained history position outside manifest: %d", position)
		}
		node, err := cursor.get(ctx, uint64(position))
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(node.Oid) != sha || hex.EncodeToString(node.Tree) != wantTree || len(node.Parents) != len(wantParents) {
			t.Fatalf("retained history identity differs for %s", sha)
		}
		// Copy positions: fetching another block replaces the cursor's working set.
		for j, parentPosition := range append([]uint64(nil), node.Parents...) {
			parent, err := cursor.get(ctx, parentPosition)
			if err != nil || hex.EncodeToString(parent.Oid) != wantParents[j] {
				t.Fatalf("retained history parent differs for %s: %v", sha, err)
			}
		}
		if i == 0 {
			tipObject = o
			if len(wantParents) > 0 {
				commits = append(commits, wantParents[0])
			}
		}
		record("retained_commit_and_history", sha, "", oracleSeconds, started)
	}

	oracleStart := time.Now()
	if len(proposed.Tips) == 0 {
		t.Fatal("retained manifest has no captured tips")
	}
	countText := oracle(strings.Join(proposed.Tips, "\n")+"\n", "rev-list", "--count", "--stdin")
	wantCount, err := strconv.ParseUint(strings.TrimSpace(string(countText)), 10, 64)
	if err != nil || wantCount != proposed.HistoryCount {
		t.Fatalf("fresh retained history count: got %d, Git %q (%v)", proposed.HistoryCount, countText, err)
	}
	oracleSeconds := time.Since(oracleStart).Seconds()
	started := time.Now()
	for _, position := range []uint64{1, proposed.HistoryCount} {
		node, err := cursor.get(ctx, position)
		if err != nil {
			t.Fatal(err)
		}
		var stored historyPosition
		if err := history.get(ctx, "g/"+hex.EncodeToString(node.Oid), &stored); err != nil || uint64(stored) != position {
			t.Fatalf("retained history boundary disagrees with OID index: %v", err)
		}
		if position == 1 && len(node.Parents) != 0 {
			t.Fatal("first retained history node has a parent")
		}
	}
	record("retained_history_boundaries", tip, "", oracleSeconds, started)

	snapshot := &Snapshot{idx: idx, history: history, SHA: tip, Tree: tipObject.Tree}
	for _, path := range paths {
		oracleStart := time.Now()
		entry := oracle("", "--literal-pathspecs", "ls-tree", "-z", tip, "--", path)
		if len(entry) == 0 || entry[len(entry)-1] != 0 || bytes.Count(entry, []byte{0}) != 1 {
			t.Fatalf("retained blob path must resolve exactly once: %q", path)
		}
		header, name, ok := bytes.Cut(entry[:len(entry)-1], []byte{'\t'})
		fields := strings.Fields(string(header))
		if !ok || string(name) != path || len(fields) != 3 || fields[1] != "blob" {
			t.Fatalf("retained blob oracle is not a blob: %q", entry)
		}
		oid := fields[2]
		want := oracle("", "cat-file", "blob", oid)
		oracleSeconds := time.Since(oracleStart).Seconds()
		started := time.Now()
		var identity object
		if err := idx.get(ctx, "o/"+oid, &identity); err != nil || identity.Kind != "blob" || identity.Size != int64(len(want)) {
			t.Fatalf("retained blob identity differs for %q: %+v: %v", path, identity, err)
		}
		got := make([]byte, len(want)+1)
		n, err := snapshot.ReadAt(ctx, oid, got, 0)
		if err != io.EOF || n != len(want) || !bytes.Equal(got[:n], want) {
			t.Fatalf("retained blob bytes differ for %q: n=%d err=%v", path, n, err)
		}
		if n, err := snapshot.ReadAt(ctx, oid, got[:1], int64(len(want))); n != 0 || err != io.EOF {
			t.Fatalf("retained blob EOF differs for %q: n=%d err=%v", path, n, err)
		}
		record("retained_blob_by_oid", tip, path, oracleSeconds, started)
	}
	return rows
}

// Keep native-oracle output bounded even if a future fixture changes paths.
type ceilingRetainedOutput struct{ data bytes.Buffer }

func (b *ceilingRetainedOutput) Write(p []byte) (int, error) {
	if len(p) > (8<<20)-b.data.Len() {
		return 0, fmt.Errorf("retained oracle exceeds 8 MiB")
	}
	return b.data.Write(p)
}

func (b *ceilingRetainedOutput) Bytes() []byte { return b.data.Bytes() }

// Parse the selected raw Git oracle independently of the import parser.
func ceilingRetainedCommit(t *testing.T, raw []byte) (string, commitInfo) {
	t.Helper()
	headers, message, ok := bytes.Cut(raw, []byte("\n\n"))
	if !ok {
		t.Fatal("retained commit oracle has no message separator")
	}
	var tree string
	var info commitInfo
	var authorFound, committerFound bool
	for _, line := range bytes.Split(headers, []byte{'\n'}) {
		key, value, ok := bytes.Cut(line, []byte{' '})
		if !ok {
			continue
		}
		if string(key) == "tree" {
			tree = string(value)
		}
		if string(key) != "author" && string(key) != "committer" {
			continue
		}
		zoneStart := bytes.LastIndexByte(value, ' ')
		if zoneStart < 0 {
			t.Fatal("invalid retained identity oracle")
		}
		stampStart := bytes.LastIndexByte(value[:zoneStart], ' ')
		if stampStart < 0 {
			t.Fatal("invalid retained timestamp oracle")
		}
		stamp, err := strconv.ParseInt(string(value[stampStart+1:zoneStart]), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		zone := string(value[zoneStart+1:])
		if len(zone) != 5 || (zone[0] != '+' && zone[0] != '-') || stampStart > maxLogAuthor {
			t.Fatal("selected retained identity oracle exceeds supported fixture bounds")
		}
		hours, e1 := strconv.Atoi(zone[1:3])
		minutes, e2 := strconv.Atoi(zone[3:])
		if e1 != nil || e2 != nil || hours > 23 || minutes > 59 {
			t.Fatal("invalid retained identity timezone")
		}
		offset := int32(hours*60 + minutes)
		if zone[0] == '-' {
			offset = -offset
		}
		if string(key) == "committer" {
			info.Committer = bytes.Clone(value[:stampStart])
			info.CommitTime, info.CommitterOffset = stamp, offset
			info.HasCommitter, committerFound = true, true
		} else {
			info.Author = bytes.Clone(value[:stampStart])
			info.AuthorTime, info.AuthorOffset, authorFound = stamp, offset, true
		}
	}
	if tree == "" || !authorFound || !committerFound {
		t.Fatal("retained commit oracle missing required headers")
	}
	info.Message = append([]byte(nil), message[:min(len(message), maxLogMessage)]...)
	info.MessageTruncated = len(message) > maxLogMessage
	return tree, info
}
