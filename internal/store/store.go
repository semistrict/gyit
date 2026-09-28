// Package store implements the small set of object operations needed by a repository.
package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

var ErrNotFound = errors.New("object not found")
var ErrConflict = errors.New("object changed: another writer published an update")

// Store must provide atomic whole-object writes and strong read-after-write consistency.
// Methods may be called concurrently, including writes to independent immutable keys.
// Get with length=-1 reads the whole object; otherwise it must return exactly length bytes.
// Successful whole-object reads return an opaque version token, neither empty nor "*".
// Put condition is empty for unconditional, "*" for create-only, or an opaque Get token.
// Conditional Put must atomically compare and replace across all clients, returning
// ErrConflict on mismatch. A read followed by an unconditional write is not CAS.
// Keys are relative slash-separated paths. Published objects other than HEAD are immutable.
type Store interface {
	Get(context.Context, string, int64, int64) ([]byte, string, error)
	Put(context.Context, string, []byte, string) error
}

// VersionedWriter optionally returns the version assigned by an atomic write.
// The token must identify that write, never a subsequent GET of a newer value.
// This avoids a read-after-write round trip when the provider returns a version.
type VersionedWriter interface {
	PutVersion(context.Context, string, []byte, string) (string, error)
}

func validKey(key string) error {
	if !fs.ValidPath(key) || key == "." {
		return fmt.Errorf("invalid object key %q", key)
	}
	return nil
}
