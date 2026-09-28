//go:build !js

package store

import (
	"context"
	"fmt"
	"net/url"
)

func Open(ctx context.Context, location, endpoint, region string) (Store, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "", "file":
		if u.Scheme == "file" && u.Host != "" {
			return nil, fmt.Errorf("file URL must have no host")
		}
		root := location
		if u.Scheme == "file" {
			root = u.Path
		}
		return NewLocal(root)
	case "s3":
		return newS3(ctx, u, endpoint, region)
	case "gs":
		if endpoint != "" || region != "" {
			return nil, fmt.Errorf("gs:// uses native GCS authentication and endpoint; omit S3 endpoint/region options")
		}
		return newGCS(ctx, u)
	default:
		return nil, fmt.Errorf("unsupported store scheme %q", u.Scheme)
	}
}
