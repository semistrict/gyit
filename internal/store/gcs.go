//go:build !js

package store

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// gcsStore uses the native Cloud Storage API and Application Default Credentials.
// Object generations, not ETags, are the compare-and-swap publication tokens.
type gcsStore struct {
	client         *storage.Client
	bucket, prefix string
}

func newGCS(ctx context.Context, u *url.URL) (Store, error) {
	if u.Host == "" || u.User != nil || u.Port() != "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("expected gs://bucket/prefix")
	}
	prefix := strings.Trim(u.Path, "/")
	if prefix != "" {
		if err := validKey(prefix); err != nil {
			return nil, err
		}
	}
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return &gcsStore{client: client, bucket: u.Host, prefix: prefix}, nil
}

func (s *gcsStore) Close() error { return s.client.Close() }
func (s *gcsStore) object(key string) *storage.ObjectHandle {
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	return s.client.Bucket(s.bucket).Object(key)
}
func gcsError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	var api *googleapi.Error
	if errors.As(err, &api) {
		switch api.Code {
		case 404:
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case 412:
			return fmt.Errorf("%w: %v", ErrConflict, err)
		}
	}
	return err
}
func (s *gcsStore) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if err := validKey(key); err != nil {
		return nil, "", err
	}
	if off < 0 || length < -1 || (length == -1 && off != 0) || (length > 0 && (length == math.MaxInt64 || off > math.MaxInt64-length)) {
		return nil, "", fmt.Errorf("invalid range")
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if length == 0 {
		return []byte{}, "", nil
	}
	// Preserve stored bytes even if external object metadata requests gzip
	// transcoding. Pack offsets always address the original immutable bytes.
	r, err := s.object(key).ReadCompressed(true).NewRangeReader(ctx, off, length)
	if err != nil {
		return nil, "", gcsError(err)
	}
	defer r.Close()
	if r.Attrs.Generation <= 0 || r.Attrs.StartOffset != off {
		return nil, "", fmt.Errorf("GCS returned invalid generation or range offset")
	}
	var reader io.Reader = r
	if length >= 0 {
		reader = io.LimitReader(r, length+1)
	}
	b, err := io.ReadAll(reader)
	if err == nil && length >= 0 && int64(len(b)) != length {
		err = io.ErrUnexpectedEOF
	}
	return b, strconv.FormatInt(r.Attrs.Generation, 10), gcsError(err)
}
func (s *gcsStore) Put(ctx context.Context, key string, data []byte, condition string) error {
	_, err := s.PutVersion(ctx, key, data, condition)
	return err
}
func (s *gcsStore) PutVersion(ctx context.Context, key string, data []byte, condition string) (string, error) {
	if err := validKey(key); err != nil {
		return "", err
	}
	object := s.object(key)
	switch condition {
	case "":
	case "*":
		object = object.If(storage.Conditions{DoesNotExist: true})
	default:
		generation, err := strconv.ParseInt(condition, 10, 64)
		if err != nil || generation <= 0 || strconv.FormatInt(generation, 10) != condition {
			return "", fmt.Errorf("invalid GCS generation token")
		}
		object = object.If(storage.Conditions{GenerationMatch: generation})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := object.NewWriter(ctx)
	// Cap retry buffers at 8 MiB per concurrent upload. Tiny metadata uses one
	// 256 KiB quantum; large packs avoid one network round trip per 256 KiB.
	const quantum, maxChunk = 256 << 10, 8 << 20
	w.ChunkSize = maxChunk
	if len(data) < maxChunk {
		w.ChunkSize = max(quantum, (len(data)+quantum-1)/quantum*quantum)
	}
	w.ContentType = "application/octet-stream"
	w.CRC32C = crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	w.SendCRC32C = true
	if _, err := w.Write(data); err != nil {
		cancel() // Never finalize a partial object after a failed write.
		_ = w.Close()
		return "", gcsError(err)
	}
	if err := w.Close(); err != nil {
		return "", gcsError(err)
	}
	if w.Attrs() == nil || w.Attrs().Generation <= 0 {
		return "", fmt.Errorf("GCS returned invalid write generation")
	}
	return strconv.FormatInt(w.Attrs().Generation, 10), nil
}
