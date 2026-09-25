package repo

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"gat/internal/store"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// TestExistingRepositoryS3 is a reproducible scale probe, not a default fixture.
// It never fetches or changes the supplied source repository.
func TestExistingRepositoryS3(t *testing.T) {
	endpoint, source := os.Getenv("GAT_S3_TEST_ENDPOINT"), os.Getenv("GAT_SCALE_SOURCE")
	if endpoint == "" || source == "" {
		t.Skip("set GAT_S3_TEST_ENDPOINT and GAT_SCALE_SOURCE for a local S3 scale probe")
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		t.Fatal("test endpoint must be loopback")
	}
	t.Setenv("AWS_ACCESS_KEY_ID", "gat-test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "local-test-secret")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion("us-east-1"))
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(endpoint); o.UsePathStyle = true })
	bucket := fmt.Sprintf("gat-scale-%d", time.Now().UnixNano())
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Always list from the beginning while deleting, avoiding continuation
		// tokens whose position might change when objects are removed.
		for {
			out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
			if err != nil {
				t.Error(err)
				return
			}
			if len(out.Contents) == 0 {
				break
			}
			objects := make([]types.ObjectIdentifier, 0, len(out.Contents))
			for _, o := range out.Contents {
				objects = append(objects, types.ObjectIdentifier{Key: o.Key})
			}
			deleted, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: objects}})
			if err != nil {
				t.Error(err)
				return
			}
			if len(deleted.Errors) > 0 {
				t.Error(deleted.Errors)
				return
			}
		}
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	})
	backend, err := store.Open(ctx, "s3://"+bucket+"/repo", endpoint, "us-east-1")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	stats, err := Import(ctx, backend, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("import: %s; objects=%d blobs=%d raw_bytes=%d compressed_pack_bytes=%d", time.Since(started), stats.Objects, stats.Blobs, stats.Bytes, stats.UploadedBytes)
	sha := command(t, source, "rev-parse", "HEAD")
	s := &countedStore{Store: backend}
	r, _ := New(s, DefaultCacheBytes)
	started = time.Now()
	snapshot, err := r.Open(ctx, sha)
	if err != nil {
		t.Fatal(err)
	}
	var entries []Entry
	after := ""
	for {
		batch, err := snapshot.ReadDir(ctx, snapshot.Tree, after, 128)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, batch...)
		if len(batch) < 128 {
			break
		}
		after = batch[len(batch)-1].Name
	}
	t.Logf("cold open + root listing: %s; entries=%d GETs=%d bytes=%d pack_GETs=%d", time.Since(started), len(entries), s.gets, s.bytes, s.packGets)
	if s.packGets != 0 || s.gets > 12 || s.bytes > 1<<20 {
		t.Fatalf("root listing fetched too much: %+v", s)
	}
	for _, entry := range entries {
		if entry.Mode == 0100644 && strings.HasPrefix(entry.Name, "README") {
			b := make([]byte, 128)
			s.reset()
			started = time.Now()
			n, err := snapshot.ReadAt(ctx, entry.OID, b, 0)
			elapsed := time.Since(started)
			if err != nil {
				t.Fatal(err)
			}
			expected := command(t, source, "show", sha+":"+entry.Name)
			if string(b[:n]) != expected[:n] {
				t.Fatal("file bytes differ from git")
			}
			t.Logf("first file range: %s; GETs=%d bytes=%d pack_GETs=%d", elapsed, s.gets, s.bytes, s.packGets)
			if s.packGets != 1 {
				t.Fatal("single file range should fetch one compressed chunk")
			}
			break
		}
	}
	s.reset()
	started = time.Now()
	if _, err := r.Open(ctx, sha); err != nil {
		t.Fatal(err)
	}
	t.Logf("warm checkout open: %s; GETs=%d bytes=%d", time.Since(started), s.gets, s.bytes)
	started = time.Now()
	stats, err = Import(ctx, backend, ImportOptions{Repo: source})
	if err != nil {
		t.Fatal(err)
	}
	if stats.Objects != 0 {
		t.Fatal("no-op import reuploaded objects")
	}
	t.Logf("no-op incremental import: %s", time.Since(started))
}
