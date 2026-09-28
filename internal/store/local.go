//go:build !js

package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

type Local struct{ root string }

func NewLocal(root string) (*Local, error) {
	if root == "" {
		return nil, fmt.Errorf("empty store path")
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

func (s *Local) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := validKey(key); err != nil {
		return nil, "", err
	}
	if off < 0 || length < -1 {
		return nil, "", fmt.Errorf("invalid range")
	}
	f, err := os.Open(filepath.Join(s.root, key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	if length == -1 {
		if off != 0 {
			return nil, "", fmt.Errorf("whole-object read requires offset zero")
		}
		b, err := io.ReadAll(f)
		return b, fmt.Sprintf("%x", sha256.Sum256(b)), err
	}
	b := make([]byte, length)
	_, err = f.ReadAt(b, off)
	return b, "", err
}

func (s *Local) Put(ctx context.Context, key string, data []byte, condition string) error {
	if err := validKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	name := filepath.Join(s.root, key)
	if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
		return err
	}
	// flock also protects CAS across separate importer processes.
	lock, err := os.OpenFile(name+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	for {
		err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if condition != "" {
		_, token, err := s.Get(ctx, key, 0, -1)
		if condition == "*" {
			if err == nil {
				return ErrConflict
			}
			if !errors.Is(err, ErrNotFound) {
				return err
			}
		} else {
			if errors.Is(err, ErrNotFound) || (err == nil && token != condition) {
				return ErrConflict
			}
			if err != nil {
				return err
			}
		}
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".upload-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
