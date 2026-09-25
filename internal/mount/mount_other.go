//go:build !linux

package mount

import (
	"context"
	"fmt"
	"gat/internal/repo"
)

func Run(context.Context, *repo.Repository, string, string, string) error {
	return fmt.Errorf("FUSE mounting requires Linux; importing and reading snapshots work on this platform")
}
