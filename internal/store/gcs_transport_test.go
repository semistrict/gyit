//go:build !js

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGCSStatusErrors(t *testing.T) {
	for _, test := range []struct {
		code codes.Code
		want error
	}{
		{codes.NotFound, ErrNotFound},
		{codes.FailedPrecondition, ErrConflict},
	} {
		if err := gcsError(status.Error(test.code, "test")); !errors.Is(err, test.want) {
			t.Fatalf("%s: got %v, want %v", test.code, err, test.want)
		}
	}
	original := status.Error(codes.PermissionDenied, "test")
	if err := gcsError(original); err != original {
		t.Fatalf("changed unrelated error: %v", err)
	}
}

// Compare transports on the same VM/bucket using the same payloads. Every run
// owns a unique prefix and deletes only its own objects, even after failure.
func TestGCSLiveTransportLatency(t *testing.T) {
	bucket := os.Getenv("GYIT_GCS_TEST_BUCKET")
	if bucket == "" {
		t.Skip("set GYIT_GCS_TEST_BUCKET")
	}
	checkCtx, cancelCheck := context.WithTimeout(t.Context(), 15*time.Second)
	t.Logf("direct connectivity check: %v", storage.CheckDirectConnectivitySupported(checkCtx, bucket))
	cancelCheck()
	for _, transport := range []struct {
		name string
		open func(context.Context, ...option.ClientOption) (*storage.Client, error)
	}{{"HTTP", storage.NewClient}, {"gRPC", storage.NewGRPCClient}} {
		t.Run(transport.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
			defer cancel()
			client, err := transport.open(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			s := &gcsStore{client: client, bucket: bucket, prefix: "adapter-bench/" + strconv.FormatInt(time.Now().UnixNano(), 10)}
			var keys []string
			defer func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				for _, key := range keys {
					if err := s.object(key).Delete(cleanup); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
						t.Errorf("cleanup %s: %v", key, err)
					}
				}
			}()
			body := bytes.Repeat([]byte("payload"), 32768)
			keys = append(keys, "body")
			if err := s.Put(ctx, "body", body, "*"); err != nil {
				t.Fatal(err)
			}
			timings := map[string][]time.Duration{}
			for i := 0; i < 10; i++ {
				start := time.Now()
				got, _, err := s.Get(ctx, "body", 0, -1)
				timings["get"] = append(timings["get"], time.Since(start))
				if err != nil || !bytes.Equal(got, body) {
					t.Fatalf("GET: %v", err)
				}
				key := fmt.Sprintf("manifest-%d", i)
				keys = append(keys, key)
				start = time.Now()
				_, err = s.PutVersion(ctx, key, body[:256], "*")
				timings["put"] = append(timings["put"], time.Since(start))
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, operation := range []string{"get", "put"} {
				samples := timings[operation]
				slices.Sort(samples)
				t.Logf("%s n=10 median=%s maximum=%s", operation, samples[len(samples)/2], samples[len(samples)-1])
			}
		})
	}
}
