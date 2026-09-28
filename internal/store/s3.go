//go:build !js

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

type s3Store struct {
	client         *s3.Client
	bucket, prefix string
}

func newS3(ctx context.Context, u *url.URL, endpoint, region string) (Store, error) {
	if u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("expected s3://bucket/prefix")
	}
	options := []func(*config.LoadOptions) error{}
	if region != "" {
		options = append(options, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	})
	return &s3Store{client: client, bucket: u.Host, prefix: strings.Trim(u.Path, "/")}, nil
}

func (s *s3Store) key(key string) string {
	if s.prefix == "" {
		return key
	}
	return s.prefix + "/" + key
}

func mapError(err error) error {
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return fmt.Errorf("%w: %v", ErrNotFound, err)
		case "PreconditionFailed", "ConditionalRequestConflict":
			return fmt.Errorf("%w: %v", ErrConflict, err)
		}
	}
	return err
}

func (s *s3Store) Get(ctx context.Context, key string, off, length int64) ([]byte, string, error) {
	if err := validKey(key); err != nil {
		return nil, "", err
	}
	if off < 0 || length < -1 || (length == -1 && off != 0) {
		return nil, "", fmt.Errorf("invalid range")
	}
	if length == 0 {
		return []byte{}, "", nil
	}
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.key(key))}
	if length >= 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		return nil, "", mapError(err)
	}
	defer out.Body.Close()
	if length >= 0 && !strings.HasPrefix(aws.ToString(out.ContentRange), fmt.Sprintf("bytes %d-%d/", off, off+length-1)) {
		return nil, "", fmt.Errorf("object store did not honor requested byte range")
	}
	var r io.Reader = out.Body
	if length >= 0 {
		r = io.LimitReader(r, length+1)
	}
	b, err := io.ReadAll(r)
	if err == nil && length >= 0 && int64(len(b)) != length {
		err = io.ErrUnexpectedEOF
	}
	return b, aws.ToString(out.ETag), err
}

func (s *s3Store) Put(ctx context.Context, key string, data []byte, condition string) error {
	if err := validKey(key); err != nil {
		return err
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(s.key(key)), Body: bytes.NewReader(data)}
	if condition == "*" {
		in.IfNoneMatch = aws.String("*")
	} else if condition != "" {
		in.IfMatch = aws.String(condition)
	}
	_, err := s.client.PutObject(ctx, in)
	return mapError(err)
}
