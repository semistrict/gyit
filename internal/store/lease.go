package store

import (
	"context"
)

// Acquire borrows immutable range bytes until release. The returned slice must
// not be modified or retained after release. Stores without leases use Get.
func Acquire(ctx context.Context, s Store, key string, off, n int64) ([]byte, func(), error) {
	if s, ok := s.(interface {
		Acquire(context.Context, string, int64, int64) ([]byte, func(), error)
	}); ok {
		return s.Acquire(ctx, key, off, n)
	}
	b, _, err := s.Get(ctx, key, off, n)
	return b, func() {}, err
}
