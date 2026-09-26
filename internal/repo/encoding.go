package repo

import (
	"fmt"

	storagev1 "gyit/internal/gen/gyit/storage/v1"
	"google.golang.org/protobuf/proto"
)

const (
	legacyFormatVersion = 6
	directBlobFormat    = 7
	formatVersion       = 8
)

// Stable formats 4–6 use the original object index. Format 7 adds a direct
// blob index; format 8 also uses archived Git frames and a global size table.
// The 9000-series values were published by earlier development builds.
func supportedFormat(version int) bool {
	switch version {
	case 4, 5, legacyFormatVersion, directBlobFormat, formatVersion,
		9004, 9010, 9011, 9012, 9013, 9014, 9015:
		return true
	}
	return false
}

func manifestHasGlobalSizes(version int) bool {
	return version == formatVersion || version == 9013 || version == 9014 || version == 9015
}

// Keep wire messages at the encoding seam. The repository's small value types
// can be copied safely; generated protobuf messages contain runtime state.
// Deterministic encoding makes repeated publication of unchanged pages stable
// with the pinned schema/runtime. Hashes always cover the actual stored bytes.
func marshal(value any) ([]byte, error) {
	var message proto.Message
	switch v := value.(type) {
	case manifest:
		message = &storagev1.Manifest{Version: uint32(v.Version), Format: v.Format, RootPage: encodePageRef(v.Root), Tips: v.Tips, Refs: encodePageRef(v.Refs), RevisionGraph: v.RevisionGraph, RefsHash: v.RefsHash, CommitMetadata: v.CommitMetadata, History: encodePageRef(v.History), HistoryCount: v.HistoryCount, Blobs: encodePageRef(v.Blobs)}
	case historyPosition:
		message = &storagev1.HistoryPosition{Position: uint64(v)}
	case pageRef:
		message = encodePageRef(v)
	case reference:
		message = &storagev1.ReferenceRecord{Commit: v.Commit, ObjectId: v.ObjectID, SymbolicTarget: v.SymbolicTarget}
	case commitInfo:
		message = &storagev1.CommitRecord{Author: v.Author, AuthorTime: v.AuthorTime, AuthorOffsetMinutes: v.AuthorOffset, CommitTime: v.CommitTime, Message: v.Message, MessageTruncated: v.MessageTruncated, AuthorTruncated: v.AuthorTruncated, Committer: v.Committer, CommitterOffsetMinutes: v.CommitterOffset, CommitterTruncated: v.CommitterTruncated, HasCommitter: v.HasCommitter}
	case parents:
		message = &storagev1.ParentRecord{Parents: v.Parents}
	case page:
		p := &storagev1.IndexPage{}
		for _, v := range v.Items {
			p.Items = append(p.Items, &storagev1.IndexItem{Key: v.Key, Value: v.Value})
		}
		for _, v := range v.Children {
			p.Children = append(p.Children, &storagev1.IndexChild{MaxKey: v.Max, Page: encodePageRef(v.ID)})
		}
		message = p
	case object:
		kind, err := encodeKind(v.Kind)
		if err != nil {
			return nil, err
		}
		message = &storagev1.ObjectRecord{Kind: kind, Size: v.Size, Tree: v.Tree, Directory: encodePageRef(v.Directory)}
	case Entry:
		message = &storagev1.DirectoryEntry{Oid: v.OID, Mode: v.Mode, Size: v.Size, RawMode: v.RawMode}
	case chunk:
		p := &storagev1.ChunkRecord{Pack: v.Pack, Offset: v.Offset, Length: v.Length, Hash: v.Hash, ArchiveRecipe: []byte(v.ArchiveRecipe)}
		if v.Base != nil {
			p.Base = encodeChunkBase(v.Base)
		}
		message = p
	case anchorRecord:
		p := &storagev1.AnchorRecord{}
		for i := range v.Candidates {
			p.Candidates = append(p.Candidates, encodeChunkBase(&v.Candidates[i]))
		}
		message = p
	default:
		return nil, fmt.Errorf("unsupported storage record type %T", value)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func unmarshal(data []byte, value any) error {
	switch out := value.(type) {
	case *manifest:
		var p storagev1.Manifest
		if err := proto.Unmarshal(data, &p); err != nil {
			return fmt.Errorf("decode protobuf manifest (legacy stores must be reimported): %w", err)
		}
		*out = manifest{Version: int(p.Version), Format: p.Format, Root: decodePageRef(p.RootPage), Tips: p.Tips, Refs: decodePageRef(p.Refs), RevisionGraph: p.RevisionGraph, RefsHash: p.RefsHash, CommitMetadata: p.CommitMetadata, History: decodePageRef(p.History), HistoryCount: p.HistoryCount, Blobs: decodePageRef(p.Blobs)}
	case *historyPosition:
		var p storagev1.HistoryPosition
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = historyPosition(p.Position)
	case *pageRef:
		var p storagev1.PageReference
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = decodePageRef(&p)
	case *reference:
		var p storagev1.ReferenceRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = reference{Commit: p.Commit, ObjectID: p.ObjectId, SymbolicTarget: p.SymbolicTarget}
	case *commitInfo:
		var p storagev1.CommitRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		if len(p.Author) > maxLogAuthor || len(p.Committer) > maxLogAuthor || len(p.Message) > maxLogMessage {
			return fmt.Errorf("oversized commit display metadata")
		}
		*out = commitInfo{Author: p.Author, AuthorTime: p.AuthorTime, AuthorOffset: p.AuthorOffsetMinutes, CommitTime: p.CommitTime, Message: p.Message, MessageTruncated: p.MessageTruncated, AuthorTruncated: p.AuthorTruncated, Committer: p.Committer, CommitterOffset: p.CommitterOffsetMinutes, CommitterTruncated: p.CommitterTruncated, HasCommitter: p.HasCommitter}
	case *parents:
		var p storagev1.ParentRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = parents{Parents: p.Parents}
	case *page:
		var p storagev1.IndexPage
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = page{}
		for _, v := range p.Items {
			out.Items = append(out.Items, item{Key: v.Key, Value: v.Value})
		}
		for _, v := range p.Children {
			out.Children = append(out.Children, edge{Max: v.MaxKey, ID: decodePageRef(v.Page)})
		}
	case *object:
		var p storagev1.ObjectRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		kind, err := decodeKind(p.Kind)
		if err != nil {
			return err
		}
		*out = object{Kind: kind, Size: p.Size, Tree: p.Tree, Directory: decodePageRef(p.Directory)}
	case *Entry:
		var p storagev1.DirectoryEntry
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = Entry{OID: p.Oid, Mode: p.Mode, Size: p.Size, RawMode: p.RawMode}
	case *chunk:
		var p storagev1.ChunkRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = chunk{Pack: p.Pack, Offset: p.Offset, Length: p.Length, Hash: p.Hash, ArchiveRecipe: string(p.ArchiveRecipe)}
		if len(p.ArchiveRecipe) > 8192 {
			return fmt.Errorf("archive recipe limit")
		}
		if p.Base != nil {
			var err error
			out.Base, err = decodeChunkBase(p.Base, 0)
			if err != nil {
				return err
			}
		}
	case *anchorRecord:
		var p storagev1.AnchorRecord
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		if len(p.Candidates) > maxDeltaCandidates {
			return fmt.Errorf("too many anchor candidates")
		}
		*out = anchorRecord{}
		for _, candidate := range p.Candidates {
			b, err := decodeChunkBase(candidate, 0)
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("empty anchor candidate")
			}
			out.Candidates = append(out.Candidates, *b)
		}
	default:
		return fmt.Errorf("unsupported storage record destination %T", value)
	}
	return nil
}

func encodeKind(kind string) (storagev1.ObjectKind, error) {
	switch kind {
	case "commit":
		return storagev1.ObjectKind_OBJECT_KIND_COMMIT, nil
	case "tree":
		return storagev1.ObjectKind_OBJECT_KIND_TREE, nil
	case "blob":
		return storagev1.ObjectKind_OBJECT_KIND_BLOB, nil
	default:
		return 0, fmt.Errorf("unsupported object kind %q", kind)
	}
}

func decodeKind(kind storagev1.ObjectKind) (string, error) {
	switch kind {
	case storagev1.ObjectKind_OBJECT_KIND_COMMIT:
		return "commit", nil
	case storagev1.ObjectKind_OBJECT_KIND_TREE:
		return "tree", nil
	case storagev1.ObjectKind_OBJECT_KIND_BLOB:
		return "blob", nil
	default:
		return "", fmt.Errorf("unsupported object kind %d", kind)
	}
}

func encodePageRef(r pageRef) *storagev1.PageReference {
	if r == (pageRef{}) {
		return nil
	}
	return &storagev1.PageReference{Pack: r.Pack, Offset: r.Offset, Length: r.Length, Hash: r.Hash}
}

func decodePageRef(r *storagev1.PageReference) pageRef {
	if r == nil {
		return pageRef{}
	}
	return pageRef{Pack: r.Pack, Offset: r.Offset, Length: r.Length, Hash: r.Hash}
}

func encodeChunkBase(c *chunkBase) *storagev1.ChunkBase {
	if c == nil {
		return nil
	}
	return &storagev1.ChunkBase{Pack: c.Pack, Offset: c.Offset, Length: c.Length, Hash: c.Hash, Base: encodeChunkBase(c.Base)}
}
func decodeChunkBase(c *storagev1.ChunkBase, level int) (*chunkBase, error) {
	if c == nil {
		return nil, nil
	}
	if level >= MaxDeltaDepth {
		return nil, fmt.Errorf("delta chain exceeds supported depth")
	}
	parent, err := decodeChunkBase(c.Base, level+1)
	if err != nil {
		return nil, err
	}
	return &chunkBase{Pack: c.Pack, Offset: c.Offset, Length: c.Length, Hash: c.Hash, Base: parent}, nil
}
