//go:build !linux

package githubmount

import (
	"context"
	"fmt"
	"gyit/internal/githubfs"
)

func Run(context.Context, *githubfs.FS, string) error {
	return fmt.Errorf("use the native gyit app on macOS; FUSE mounts require Linux")
}
