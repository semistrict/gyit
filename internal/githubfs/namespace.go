package githubfs

import (
	"context"
	"gyit/internal/repo"
	"strings"
	"syscall"
)

// Namespace is the gyit volume: github.com is a directory beneath its root.
type Namespace struct{ *FS }

func namespacePath(path string) (string, error) {
	if path == "github.com" {
		return "", nil
	}
	if strings.HasPrefix(path, "github.com/") {
		return strings.TrimPrefix(path, "github.com/"), nil
	}
	return "", syscall.ENOENT
}
func (n Namespace) Lookup(ctx context.Context, path string) (repo.Entry, error) {
	if path == "" {
		return directory(""), nil
	}
	p, err := namespacePath(path)
	if err != nil {
		return repo.Entry{}, err
	}
	e, err := n.FS.Lookup(ctx, p)
	if path == "github.com" {
		e.Name = "github.com"
	}
	return e, err
}
func (n Namespace) ReadDir(ctx context.Context, path, after string, limit int) ([]repo.Entry, error) {
	if limit < 1 || limit > 128 {
		return nil, syscall.EINVAL
	}
	if path == "" {
		if after < "github.com" {
			return []repo.Entry{directory("github.com")}, nil
		}
		return nil, nil
	}
	p, err := namespacePath(path)
	if err != nil {
		return nil, err
	}
	return n.FS.ReadDir(ctx, p, after, limit)
}
func (n Namespace) Read(ctx context.Context, path string, b []byte, off int64) (int, error) {
	p, err := namespacePath(path)
	if err != nil {
		return 0, err
	}
	return n.FS.Read(ctx, p, b, off)
}
func (n Namespace) Retry(path string) error {
	p, err := namespacePath(path)
	if err != nil {
		return err
	}
	return n.FS.Retry(p)
}
func (n Namespace) Generation(path string) uint64 {
	p, err := namespacePath(path)
	if err != nil {
		return 1
	}
	return n.FS.Generation(p)
}
