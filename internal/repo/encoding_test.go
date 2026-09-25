package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/proto"
)

func TestImportPublishesProtobuf(t *testing.T) {
	ctx := context.Background()
	source := t.TempDir()
	command(t, source, "init", "-q")
	write(t, source, "file", []byte("content"))
	commit := commit(t, source)
	s, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stats, err := Import(ctx, s, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	head, _, err := s.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	var manifest storagev1.Manifest
	if err := proto.Unmarshal(head, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != legacyFormatVersion || manifest.Format != "sha1" || manifest.RootPage == nil || manifest.Refs == nil || !manifest.RevisionGraph || !manifest.CommitMetadata || manifest.RefsHash == "" || len(manifest.Tips) != 1 || manifest.Tips[0] != commit {
		t.Fatalf("unexpected protobuf manifest: %v", &manifest)
	}
	generation, _, err := s.Get(ctx, "generations/"+stats.Generation, 0, -1)
	if err != nil || !bytes.Equal(generation, head) {
		t.Fatalf("generation differs from HEAD: %v", err)
	}
	encoded, _, err := s.Get(ctx, manifest.RootPage.Pack, manifest.RootPage.Offset, manifest.RootPage.Length)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(encoded)) != manifest.RootPage.Hash {
		t.Fatal("index ID does not hash the protobuf bytes")
	}
	var page storagev1.IndexPage
	if err := proto.Unmarshal(encoded, &page); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, item := range page.Items {
		switch {
		case strings.HasPrefix(item.Key, "c/"):
			var record storagev1.CommitRecord
			if err := proto.Unmarshal(item.Value, &record); err != nil {
				t.Fatal(err)
			}
			if item.Key != "c/"+commit || string(record.Message) != "fixture\n" || string(record.Author) != "Test <test@example.test>" {
				t.Fatal("invalid commit display metadata", &record)
			}
			counts["commit"]++
		case strings.HasPrefix(item.Key, "p/"):
			var record storagev1.ParentRecord
			if err := proto.Unmarshal(item.Value, &record); err != nil {
				t.Fatal(err)
			}
			if item.Key != "p/"+commit || len(record.Parents) != 0 {
				t.Fatal("invalid root parent record", item.Key, &record)
			}
			counts["parents"]++
		case strings.HasPrefix(item.Key, "o/"):
			var object storagev1.ObjectRecord
			if err := proto.Unmarshal(item.Value, &object); err != nil {
				t.Fatal(err)
			}
			if object.Kind == storagev1.ObjectKind_OBJECT_KIND_UNSPECIFIED {
				t.Fatal("missing object kind")
			}
			if item.Key == "o/"+commit && (object.Kind != storagev1.ObjectKind_OBJECT_KIND_COMMIT || object.Tree == "") {
				t.Fatal("invalid commit record")
			}
			if object.Kind == storagev1.ObjectKind_OBJECT_KIND_TREE {
				ref := object.Directory
				if ref == nil {
					t.Fatal("missing directory root")
				}
				compressed, _, err := s.Get(ctx, ref.Pack, ref.Offset, ref.Length)
				if err != nil {
					t.Fatal(err)
				}
				raw, err := decodeFrame(compressed, directoryPageBytes)
				if err != nil {
					t.Fatal(err)
				}
				var directory storagev1.DirectoryPage
				if err := proto.Unmarshal(raw, &directory); err != nil {
					t.Fatal(err)
				}
				if len(directory.Entries) != 1 {
					t.Fatal("missing directory entry")
				}
				entry := directory.Entries[0]
				if string(entry.Name) != "file" || entry.Mode != 0100644 || entry.Size != 7 || len(entry.Oid) != 20 {
					t.Fatalf("invalid directory entry: %v", entry)
				}
				counts["entry"]++
			}
			counts["object"]++
		case strings.HasPrefix(item.Key, "b/"):
			var chunk storagev1.ChunkRecord
			if err := proto.Unmarshal(item.Value, &chunk); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(chunk.Pack, "packs/") || chunk.Length == 0 || len(chunk.Hash) != 64 {
				t.Fatalf("invalid chunk: %v", &chunk)
			}
			counts["chunk"]++
		default:
			t.Fatalf("unexpected index key %q", item.Key)
		}
	}
	if counts["commit"] != 1 || counts["parents"] != 1 || counts["object"] != 3 || counts["entry"] != 1 || counts["chunk"] != 1 {
		t.Fatalf("missing records: %v", counts)
	}
}

func TestRejectLegacyAndUnsupportedFormats(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	legacy := []byte(`{"version":1,"format":"sha1","root":"legacy","tips":[]}`)
	if err := s.Put(ctx, "HEAD", legacy, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readHead(ctx, s); err == nil {
		t.Fatal("legacy JSON manifest accepted")
	}
	unsupported, err := proto.Marshal(&storagev1.Manifest{Version: 99, Format: "sha1", RootPage: &storagev1.PageReference{Pack: "future"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "HEAD", unsupported, ""); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readHead(ctx, s); err == nil {
		t.Fatal("unknown format version accepted")
	}
	unknown, err := proto.Marshal(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind(99)})
	if err != nil {
		t.Fatal(err)
	}
	var object object
	if err := unmarshal(unknown, &object); err == nil {
		t.Fatal("unknown object kind accepted")
	}
}
