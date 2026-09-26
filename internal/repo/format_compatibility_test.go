package repo

import (
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"slices"
	"testing"

	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"gyit/internal/store"
	"google.golang.org/protobuf/proto"
)

// Build old layouts directly from their wire fields, without using the current
// importer. Changing the writer cannot silently turn this into a new-format test.
func TestReadLegacyStoreFormats(t *testing.T) {
	for _, version := range []uint32{4, 5, 6} {
		t.Run(fmt.Sprintf("version-%d", version), func(t *testing.T) {
			ctx := t.Context()
			backend, err := store.NewLocal(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			encode := func(message proto.Message) []byte {
				t.Helper()
				data, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
				if err != nil {
					t.Fatal(err)
				}
				return data
			}
			put := func(key string, data []byte) *storagev1.PageReference {
				t.Helper()
				if err := backend.Put(ctx, key, data, "*"); err != nil {
					t.Fatal(err)
				}
				return &storagev1.PageReference{Pack: key, Length: int64(len(data)), Hash: fmt.Sprintf("%x", sha256.Sum256(data))}
			}
			oid := func(kind string, body []byte) string {
				hash := sha1.New()
				fmt.Fprintf(hash, "%s %d\x00", kind, len(body))
				hash.Write(body)
				return hex.EncodeToString(hash.Sum(nil))
			}
			content := []byte("stored before the archive importer\n")
			blobID := oid("blob", content)
			blobBytes, _ := hex.DecodeString(blobID)
			treeBody := append([]byte("100644 file\x00"), blobBytes...)
			treeID := oid("tree", treeBody)
			commitBody := []byte(fmt.Sprintf("tree %s\n\nlegacy fixture\n", treeID))
			commitID := oid("commit", commitBody)
			compressor, err := newCompressor()
			if err != nil {
				t.Fatal(err)
			}
			defer compressor.Close()
			packed := compressor.EncodeAll(content, nil)
			put("packs/legacy-data", packed)
			blobChunk := &storagev1.ChunkRecord{Pack: "packs/legacy-data", Length: int64(len(packed)), Hash: fmt.Sprintf("%x", sha256.Sum256(content))}
			treeRecord := &storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_TREE, Size: int64(len(treeBody))}
			page := &storagev1.IndexPage{Items: []*storagev1.IndexItem{
				{Key: "b/" + blobID + "/0000000000000000", Value: encode(blobChunk)},
				{Key: "o/" + blobID, Value: encode(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_BLOB, Size: int64(len(content))})},
				{Key: "o/" + commitID, Value: encode(&storagev1.ObjectRecord{Kind: storagev1.ObjectKind_OBJECT_KIND_COMMIT, Size: int64(len(commitBody)), Tree: treeID})},
			}}
			if version == 6 {
				directory := &storagev1.DirectoryPage{Entries: []*storagev1.NamedEntry{{Name: []byte("file"), Oid: blobBytes, Mode: 0100644, Size: int64(len(content))}}}
				treeRecord.Directory = put("index/legacy-directory", compressor.EncodeAll(encode(directory), nil))
			} else {
				page.Items = append(page.Items, &storagev1.IndexItem{Key: "t/" + treeID + "/66696c65", Value: encode(&storagev1.DirectoryEntry{Oid: blobID, Mode: 0100644, Size: int64(len(content))})})
			}
			page.Items = append(page.Items, &storagev1.IndexItem{Key: "o/" + treeID, Value: encode(treeRecord)})
			slices.SortFunc(page.Items, func(a, b *storagev1.IndexItem) int { return bytes.Compare([]byte(a.Key), []byte(b.Key)) })
			root := put("index/legacy-root", encode(page))
			put("HEAD", encode(&storagev1.Manifest{Version: version, Format: "sha1", RootPage: root, Tips: []string{commitID}}))

			repository, err := New(backend, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := repository.Open(ctx, commitID)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := snapshot.ReadDir(ctx, snapshot.Tree, "", 10)
			if err != nil || len(entries) != 1 || entries[0].Name != "file" || entries[0].OID != blobID || entries[0].Mode != 0100644 || entries[0].Size != int64(len(content)) {
				t.Fatalf("legacy directory: entries=%+v err=%v", entries, err)
			}
			entry, err := snapshot.Resolve(ctx, "file")
			if err != nil || entry.OID != blobID {
				t.Fatalf("legacy lookup: entry=%+v err=%v", entry, err)
			}
			got := make([]byte, len(content)+1)
			n, err := snapshot.ReadAt(ctx, blobID, got, 0)
			if err != io.EOF || n != len(content) || !bytes.Equal(got[:n], content) {
				t.Fatalf("legacy contents: n=%d err=%v got=%q", n, err, got[:n])
			}
		})
	}
}
