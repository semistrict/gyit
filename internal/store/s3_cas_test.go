package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// Verify the SDK's wire contract without requiring credentials or an emulator.
// Actual atomicity is supplied by the S3 service, never emulated by this client.
func TestS3PublicationConditions(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "gyit-test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	for _, tc := range []struct {
		name, condition, match, none, code string
		status                             int
	}{
		{"create", "*", "", "*", "", 200},
		{"update", `"opaque-version"`, `"opaque-version"`, "", "", 200},
		{"stale", `"opaque-version"`, `"opaque-version"`, "", "PreconditionFailed", 412},
		{"race", `"opaque-version"`, `"opaque-version"`, "", "ConditionalRequestConflict", 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != "PUT" || r.URL.Path != "/bucket/prefix/HEAD" || r.Header.Get("If-Match") != tc.match || r.Header.Get("If-None-Match") != tc.none {
					t.Errorf("incorrect conditional request: %s %s match=%q none=%q", r.Method, r.URL.Path, r.Header.Get("If-Match"), r.Header.Get("If-None-Match"))
				}
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(tc.status)
				if tc.code != "" {
					fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", tc.code)
				}
			}))
			defer server.Close()
			location, _ := url.Parse("s3://bucket/prefix")
			s, err := newS3(context.Background(), location, server.URL, "us-east-1")
			if err != nil {
				t.Fatal(err)
			}
			err = s.Put(context.Background(), "HEAD", []byte("manifest"), tc.condition)
			if tc.status == 200 && err != nil {
				t.Fatal(err)
			}
			if tc.status != 200 && !errors.Is(err, ErrConflict) {
				t.Fatalf("want conflict, got %v", err)
			}
			if calls != 1 {
				t.Fatalf("unexpected retry after publication response: %d requests", calls)
			}
		})
	}
}
