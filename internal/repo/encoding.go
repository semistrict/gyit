package repo

import (
	"fmt"

	storagev1 "gyit/internal/gen/gyit/storage/v1"

	"google.golang.org/protobuf/proto"
)

// Keep wire messages at the encoding seam. The repository's small value types
// can be copied safely; generated protobuf messages contain runtime state.
// Deterministic encoding makes repeated publication of unchanged pages stable
// with the pinned schema/runtime. Hashes always cover the actual stored bytes.
func marshal(value any) ([]byte, error) {
	var message proto.Message
	switch v := value.(type) {
	case proto.Message:
		message = v
	case pageRef:
		message = encodePageRef(v)
	case reference:
		message = &storagev1.ReferenceRecord{Commit: v.Commit, ObjectId: v.ObjectID, SymbolicTarget: v.SymbolicTarget}
	case commitInfo:
		message = encodeCommitInfo(v)
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
		message = &storagev1.ObjectRecord{Kind: kind, Size: v.Size, Tree: v.Tree}
	case Entry:
		message = &storagev1.DirectoryEntry{Oid: v.OID, Mode: v.Mode, Size: v.Size, RawMode: v.RawMode}
	default:
		return nil, fmt.Errorf("unsupported storage record type %T", value)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

func encodeCommitInfo(v commitInfo) *storagev1.CommitRecord {
	return &storagev1.CommitRecord{Author: v.Author, AuthorTime: v.AuthorTime, AuthorOffsetMinutes: v.AuthorOffset, CommitTime: v.CommitTime, Message: v.Message, MessageTruncated: v.MessageTruncated, AuthorTruncated: v.AuthorTruncated, Committer: v.Committer, CommitterOffsetMinutes: v.CommitterOffset, CommitterTruncated: v.CommitterTruncated, HasCommitter: v.HasCommitter}
}

func unmarshal(data []byte, value any) error {
	switch out := value.(type) {
	case proto.Message:
		return proto.Unmarshal(data, out)
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
		*out = object{Kind: kind, Size: p.Size, Tree: p.Tree}
	case *Entry:
		var p storagev1.DirectoryEntry
		if err := proto.Unmarshal(data, &p); err != nil {
			return err
		}
		*out = Entry{OID: p.Oid, Mode: p.Mode, Size: p.Size, RawMode: p.RawMode}
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
