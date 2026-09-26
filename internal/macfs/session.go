// Package macfs adapts immutable repository snapshots to native filesystem calls.
package macfs

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"syscall"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

type Session struct {
	snapshot   *repo.Snapshot
	controller *control.Controller
}

func New(ctx context.Context, r *repo.Repository, revision string) (*Session, error) {
	s, err := r.OpenRevision(ctx, revision, "")
	if err != nil {
		return nil, err
	}
	return &Session{snapshot: s, controller: control.New(r, s)}, nil
}
func validPath(path string) error {
	if path != "" && (!fs.ValidPath(path) || path == ".") {
		return syscall.EINVAL
	}
	return nil
}
func (s *Session) Lookup(ctx context.Context, path string) (repo.Entry, error) {
	if err := validPath(path); err != nil {
		return repo.Entry{}, err
	}
	return s.snapshot.Resolve(ctx, path)
}
func (s *Session) ReadDir(ctx context.Context, path, after string, limit int) ([]repo.Entry, error) {
	if limit < 1 || limit > 128 {
		return nil, syscall.EINVAL
	}
	e, err := s.Lookup(ctx, path)
	if err != nil {
		return nil, err
	}
	if e.Mode == 0160000 {
		return nil, nil
	}
	if e.Mode != 0040000 {
		return nil, syscall.ENOTDIR
	}
	return s.snapshot.ReadDir(ctx, e.OID, after, limit)
}
func (s *Session) Read(ctx context.Context, path string, b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	e, err := s.Lookup(ctx, path)
	if err != nil {
		return 0, err
	}
	if e.Mode == 0040000 || e.Mode == 0160000 {
		return 0, syscall.EISDIR
	}
	n, err := s.snapshot.ReadAt(ctx, e.OID, b, off)
	if err == io.EOF {
		err = nil
	}
	return n, err
}
func (s *Session) Readlink(ctx context.Context, path string) (string, error) {
	e, err := s.Lookup(ctx, path)
	if err != nil {
		return "", err
	}
	if e.Mode != 0120000 {
		return "", syscall.EINVAL
	}
	if e.Size < 0 || e.Size > 1<<20 {
		return "", fmt.Errorf("symlink exceeds bound")
	}
	b := make([]byte, e.Size)
	n, err := s.Read(ctx, path, b, 0)
	return string(b[:n]), err
}
func Inode(path string) uint64 {
	if path == "" {
		return 1
	}
	h := sha256.Sum256([]byte(path))
	n := binary.LittleEndian.Uint64(h[:8])
	if n < 3 {
		n += 3
	}
	return n
}

// Serve exposes existing read-only commands. Native checkout is deliberately
// rejected until item-cache invalidation and open-handle semantics are proven.
func (s *Session) Serve(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
	if req.Version == control.Version && req.GetSwitch() != nil {
		return send(&pb.Response{Version: control.Version, Result: &pb.Response_Error{Error: &pb.Error{Code: pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, Message: "native FSKit mounts currently pin one revision; remount to change versions"}}})
	}
	return s.controller.Serve(ctx, req, send)
}
