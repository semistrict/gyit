package store

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// This opt-in test creates and deletes a dedicated bucket on a local emulator.
func TestS3RangeAndConditionalPublication(t *testing.T) {
	endpoint := os.Getenv("GAT_S3_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set GAT_S3_TEST_ENDPOINT to a local S3-compatible test server")
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		t.Fatal("test endpoint must be loopback")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "gat-test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	ctx := context.Background()
	bucket := fmt.Sprintf("gat-test-%d", time.Now().UnixNano())
	location, _ := url.Parse("s3://" + bucket + "/prefix")
	s, err := newS3(ctx, location, endpoint, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	client := s.(*s3Store).client
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if err != nil {
			t.Error(err)
			return
		}
		for _, object := range out.Contents {
			if _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key}); err != nil {
				t.Error(err)
			}
		}
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	})
	if err := s.Put(ctx, "HEAD", []byte("0123456789"), "*"); err != nil {
		t.Fatal(err)
	}
	_, token, err := s.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := s.Get(ctx, "HEAD", 3, 4)
	if err != nil || string(b) != "3456" {
		t.Fatal("range", string(b), err)
	}
	if err := s.Put(ctx, "HEAD", []byte("bad"), "*"); !errors.Is(err, ErrConflict) {
		t.Fatal("create-only", err)
	}
	if err := s.Put(ctx, "HEAD", []byte("new"), token); err != nil {
		t.Fatal("CAS", err)
	}
	if err := s.Put(ctx, "HEAD", []byte("stale"), token); !errors.Is(err, ErrConflict) {
		t.Fatal("stale CAS", err)
	}
	if _, _, err := s.Get(ctx, "missing", 0, -1); !errors.Is(err, ErrNotFound) {
		t.Fatal("missing", err)
	}
}
