package repo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Raw parent headers are authoritative only without history rewriting. Reject
// overrides even when empty: several Git environment variables distinguish an
// unset value from an explicitly empty one. Ordinary imports retain their old
// qualification and traversable-parent behavior.
func qualifyRawCommitParents(ctx context.Context, source string) (bool, error) {
	for _, name := range []string{
		"GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_SHALLOW_FILE",
		"GIT_GRAFT_FILE", "GIT_REPLACE_REF_BASE", "GIT_NAMESPACE",
		"GIT_DIR", "GIT_COMMON_DIR", "GIT_WORK_TREE", "GIT_CONFIG", "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT",
	} {
		if _, set := os.LookupEnv(name); set {
			return false, nil
		}
	}
	b, err := git(ctx, source, "rev-parse", "--git-path", "info/grafts").Output()
	if err != nil {
		return false, err
	}
	grafts := strings.TrimSpace(string(b))
	if !filepath.IsAbs(grafts) {
		grafts = filepath.Join(source, grafts)
	}
	if _, err = os.Stat(grafts); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	// Git commands already force GIT_NO_REPLACE_OBJECTS=1. Still reject the
	// presence of replacements, rather than silently adopting a different view
	// from other tools inspecting this private source.
	b, err = git(ctx, source, "for-each-ref", "--count=1", "--format=%(refname)", "refs/replace/").Output()
	if err != nil {
		return false, fmt.Errorf("inspect replacement refs: %w", err)
	}
	return len(strings.TrimSpace(string(b))) == 0, nil
}
