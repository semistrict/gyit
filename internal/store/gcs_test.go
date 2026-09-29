package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

func TestGCSRangeAndPublicationWire(t *testing.T) {
	var generation string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			generation = r.URL.Query().Get("ifGenerationMatch")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusPreconditionFailed)
			fmt.Fprint(w, `{"error":{"code":412,"message":"generation conflict"}}`)
			return
		}
		if r.Method != "GET" {
			t.Errorf("unexpected method %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "prefix/HEAD") {
			t.Errorf("missing object prefix: %s", r.URL.Path)
		}
		w.Header().Set("X-Goog-Generation", "42")
		if r.Header.Get("Range") == "bytes=2-4" {
			w.Header().Set("Content-Range", "bytes 2-4/6")
			w.WriteHeader(http.StatusPartialContent)
			fmt.Fprint(w, "cde")
		} else if r.Header.Get("Range") == "bytes=0-9" {
			fmt.Fprint(w, "abcdef")
		} else {
			fmt.Fprint(w, "abcdef")
		}
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &gcsStore{client: client, bucket: "test-bucket", prefix: "prefix"}
	for _, condition := range []string{"*", "42"} {
		if err := s.Put(t.Context(), "HEAD", []byte("next"), condition); !errors.Is(err, ErrConflict) {
			t.Fatalf("CAS %q: %v", condition, err)
		}
		want := condition
		if want == "*" {
			want = "0"
		}
		if generation != want {
			t.Fatalf("generation condition %q; want %q", generation, want)
		}
	}
	b, token, err := s.Get(t.Context(), "HEAD", 2, 3)
	if err != nil || string(b) != "cde" || token != "42" {
		t.Fatalf("range %q %q %v", b, token, err)
	}
	if _, _, err := s.Get(t.Context(), "HEAD", 0, 10); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short range: %v", err)
	}
	for _, condition := range []string{"0", "-1", "etag", "01"} {
		if err := s.Put(t.Context(), "HEAD", nil, condition); err == nil {
			t.Fatalf("invalid token %q accepted", condition)
		}
	}
	for _, key := range []string{"../escape", "/absolute", "."} {
		if _, _, err := s.Get(t.Context(), key, 0, -1); err == nil {
			t.Fatal("unsafe key accepted")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := s.Get(ctx, "HEAD", 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for code, want := range map[int]error{404: ErrNotFound, 412: ErrConflict} {
		if !errors.Is(gcsError(&googleapi.Error{Code: code}), want) {
			t.Fatal("error mapping", code)
		}
	}
	for _, raw := range []string{"gs:///prefix", "gs://user:pass@bucket", "gs://bucket/p?query=yes", "gs://bucket/a/../b"} {
		u, _ := url.Parse(raw)
		if _, err := newGCS(t.Context(), u); err == nil {
			t.Fatalf("invalid location %q accepted", raw)
		}
	}
}

// This opt-in test uses a real bucket and two independent native GCS clients.
// It deletes only its own temporary objects; bucket lifecycle belongs to caller.
func TestGCSLiveRangeAndAtomicCAS(t *testing.T) {
	testGCSLiveRangeAndAtomicCAS(t, storage.NewClient)
}
func TestGCSLiveGRPCRangeAndAtomicCAS(t *testing.T) {
	testGCSLiveRangeAndAtomicCAS(t, storage.NewGRPCClient)
}
func testGCSLiveRangeAndAtomicCAS(t *testing.T, newClient func(context.Context, ...option.ClientOption) (*storage.Client, error)) {
	bucket := os.Getenv("GYIT_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set GYIT_GCS_TEST_BUCKET to a disposable GCS bucket")
	}
	var options []option.ClientOption
	if token := os.Getenv("GYIT_GCS_TEST_ACCESS_TOKEN"); token != "" {
		options = append(options, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})))
	}
	prefix := "adapter-test/" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var stores []*gcsStore
	for range 2 {
		client, err := newClient(t.Context(), options...)
		if err != nil {
			t.Fatal(err)
		}
		s := &gcsStore{client: client, bucket: bucket, prefix: prefix}
		stores = append(stores, s)
		t.Cleanup(func() { s.Close() })
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, key := range []string{"HEAD", "empty", "large", "compressed"} {
			err := stores[0].object(key).Delete(ctx)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				t.Errorf("cleanup temporary GCS object %s: %v", key, err)
			}
		}
	})
	s := stores[0]
	if err := s.Put(t.Context(), "HEAD", []byte("abcdef"), "*"); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(t.Context(), "HEAD", []byte("wrong"), "*"); !errors.Is(err, ErrConflict) {
		t.Fatalf("create-only: %v", err)
	}
	body, token, err := s.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || string(body) != "abcdef" || token == "" {
		t.Fatalf("whole object %q %q %v", body, token, err)
	}
	body, _, err = s.Get(t.Context(), "HEAD", 2, 3)
	if err != nil || string(body) != "cde" {
		t.Fatalf("range %q %v", body, err)
	}
	if _, _, err := s.Get(t.Context(), "missing", 0, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("not-found: %v", err)
	}
	var won atomic.Int32
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			err := stores[i%2].Put(t.Context(), "HEAD", []byte("winner"), token)
			if err == nil {
				won.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("CAS admitted %d winners", won.Load())
	}
	body, newToken, err := s.Get(t.Context(), "HEAD", 0, -1)
	if err != nil || string(body) != "winner" || token == newToken {
		t.Fatalf("publication %q %q %v", body, newToken, err)
	}
	// Use separate objects for independent size scenarios so they do not
	// compete with the CAS test for GCS's per-object mutation rate.
	if err := s.Put(t.Context(), "empty", nil, ""); err != nil {
		t.Fatal(err)
	}
	if b, _, err := s.Get(t.Context(), "empty", 0, -1); err != nil || len(b) != 0 {
		t.Fatalf("empty object %q %v", b, err)
	}
	// Foreign content-encoding metadata must never change our byte offsets.
	var compressed bytes.Buffer
	zipper := gzip.NewWriter(&compressed)
	if _, err := zipper.Write(bytes.Repeat([]byte("stored bytes\n"), 100)); err != nil {
		t.Fatal(err)
	}
	if err := zipper.Close(); err != nil {
		t.Fatal(err)
	}
	writer := s.object("compressed").NewWriter(t.Context())
	writer.ContentEncoding = "gzip"
	writer.ContentType = "text/plain"
	if _, err := writer.Write(compressed.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, bounds := range [][2]int64{{0, -1}, {5, 13}} {
		got, _, err := stores[1].Get(t.Context(), "compressed", bounds[0], bounds[1])
		want := compressed.Bytes()
		if bounds[1] >= 0 {
			want = want[bounds[0] : bounds[0]+bounds[1]]
		}
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("stored gzip range %v: got=%d bytes want=%d bytes: %v", bounds, len(got), len(want), err)
		}
	}
	// Cross the resumable-upload chunk boundary and read across it using the
	// second client, as archive pack range reads do in a mounted repository.
	large := bytes.Repeat([]byte("native-gcs-range\x00"), 600000)
	if err := s.Put(t.Context(), "large", large, ""); err != nil {
		t.Fatal(err)
	}
	const start, length = (8 << 20) - 17, 500000
	b, _, err := stores[1].Get(t.Context(), "large", start, length)
	if err != nil || !bytes.Equal(b, large[start:start+length]) {
		t.Fatalf("resumable upload/range mismatch: length=%d err=%v", len(b), err)
	}
}

func TestGCSPutReturnsAssignedGeneration(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.URL.Query().Get("ifGenerationMatch") != "41" {
			t.Errorf("unexpected publication: %s %s", r.Method, r.URL)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"generation":"42","size":"4"}`)
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &gcsStore{client: client, bucket: "test-bucket"}
	token, err := s.PutVersion(t.Context(), "HEAD", []byte("next"), "41")
	if err != nil || token != "42" {
		t.Fatalf("write token %q: %v", token, err)
	}
	if requests.Load() != 1 {
		t.Fatalf("publication made %d requests", requests.Load())
	}
}

// Range addressing and cached object hashes require original stored bytes.
// Some foreign metadata combinations make GCS transcode even when compressed
// reads were requested. Returning those bytes as a successful Get is invalid.
func TestGCSRejectsTranscodedRead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Goog-Generation", "42")
		w.Header().Set("X-Goog-Stored-Content-Encoding", "gzip")
		w.Header().Set("X-Goog-Stored-Content-Length", "30")
		fmt.Fprint(w, "decompressed payload")
	}))
	defer server.Close()
	client, err := storage.NewClient(t.Context(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s := &gcsStore{client: client, bucket: "test-bucket"}
	if data, _, err := s.Get(t.Context(), "object", 0, -1); err == nil || len(data) != 0 {
		t.Fatalf("accepted transcoded object: %q %v", data, err)
	}
}
