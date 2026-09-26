package repo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"gyit/internal/store"
	bolt "go.etcd.io/bbolt"
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

func TestLogUpgradeAndBoundedReads(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q", "-b", "main")
	for i := 0; i < 3; i++ {
		write(t, source, "file", []byte(fmt.Sprint(i)))
		commit(t, source)
	}
	local, _ := store.NewLocal(t.TempDir())
	storage := &countedStore{Store: local}
	if _, err := Import(ctx, storage, ImportOptions{Repo: source}); err != nil {
		t.Fatal(err)
	}
	m, token, err := readHead(ctx, storage)
	if err != nil {
		t.Fatal(err)
	}
	old := &index{store: storage, cache: newCache(1 << 20), root: m.Root}
	db, err := bolt.Open(filepath.Join(t.TempDir(), "old.db"), 0600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("records"))
		if err != nil {
			return err
		}
		after := ""
		for {
			items, err := old.scan(ctx, "", after, 128)
			if err != nil {
				return err
			}
			for _, item := range items {
				after = item.Key
				if !strings.HasPrefix(item.Key, "c/") {
					if err := b.Put([]byte(item.Key), item.Value); err != nil {
						return err
					}
				}
			}
			if len(items) < 128 {
				break
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = db.View(func(tx *bolt.Tx) error {
		var err error
		idx := &index{store: storage, cache: newCache(1 << 20)}
		m.Root, err = idx.update(ctx, tx.Bucket([]byte("records")))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	m.CommitMetadata = false
	data, _ := marshal(m)
	if err := storage.Put(ctx, "HEAD", data, token); err != nil {
		t.Fatal(err)
	}
	repository, _ := New(storage, 0)
	selected, err := repository.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.Log(ctx, 1, false, func(LogEntry) error { return nil }); !errors.Is(err, ErrLogMetadata) {
		t.Fatal("missing metadata must request upgrade", err)
	}
	storage.reset()
	upgraded, err := Import(ctx, storage, ImportOptions{Repo: source})
	if err != nil || upgraded.Objects != 0 || upgraded.UploadedBytes != 0 || storage.packGets != 0 {
		t.Fatal("upgrade touched file data", upgraded, storage.packGets, err)
	}
	selected, err = repository.OpenRevision(ctx, "main", "")
	if err != nil {
		t.Fatal(err)
	}
	storage.reset()
	if err := selected.Log(ctx, 0, false, func(LogEntry) error { t.Fatal("zero count emitted"); return nil }); err != nil || storage.gets != 0 {
		t.Fatal("zero count did I/O", err, storage.gets)
	}
	count := 0
	if err := selected.Log(ctx, 1, false, func(e LogEntry) error {
		count++
		if e.SHA != selected.SHA || string(e.Message) != "fixture\n" {
			t.Fatal(e)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 || storage.packGets != 0 {
		t.Fatal("log count or file reads", count, storage.packGets)
	}
	stop := errors.New("stop")
	if err := selected.Log(ctx, 3, false, func(LogEntry) error { return stop }); !errors.Is(err, stop) {
		t.Fatal("callback failure lost", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := selected.Log(canceled, 3, false, func(LogEntry) error { return nil }); err == nil {
		t.Fatal("cancellation ignored")
	}
	repeated, err := Import(ctx, storage, ImportOptions{Repo: source})
	if err != nil || repeated.Generation != upgraded.Generation {
		t.Fatal("unchanged metadata import", repeated, err)
	}
}
