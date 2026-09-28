package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gyit/internal/store"

	bolt "go.etcd.io/bbolt"
)

type ImportOptions struct{ Repo, Revision, TempDir string }
type Stats struct{ Generation string }

func git(ctx context.Context, dir string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, "git", append([]string{"-C", dir}, args...)...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "GIT_NO_LAZY_FETCH=1")
	return cmd
}

// Import publishes a local repository using the same immutable pack pool as
// on-demand GitHub acquisition. A temporary bare clone packs loose source
// objects without modifying the source repository.
func Import(ctx context.Context, backend store.Store, opt ImportOptions) (Stats, error) {
	temp, err := os.MkdirTemp(opt.TempDir, "gyit-import-")
	if err != nil {
		return Stats{}, err
	}
	defer os.RemoveAll(temp)
	source, err := filepath.Abs(opt.Repo)
	if err != nil {
		return Stats{}, err
	}
	format, err := git(ctx, source, "rev-parse", "--show-object-format").Output()
	if err != nil {
		return Stats{}, err
	}
	if strings.TrimSpace(string(format)) != "sha1" {
		return Stats{}, fmt.Errorf("only SHA-1 repositories are supported")
	}
	clone := filepath.Join(temp, "source.git")
	if out, err := git(ctx, temp, "clone", "--mirror", "--no-local", "--quiet", "--", source, clone).CombinedOutput(); err != nil {
		return Stats{}, fmt.Errorf("pack source: %w: %s", err, out)
	}
	p, err := NewProgressive(ctx, backend, nil, temp)
	if err != nil {
		return Stats{}, err
	}
	if err = p.ImportPacks(ctx, clone); err != nil {
		return Stats{}, err
	}
	revision := opt.Revision
	if revision == "" {
		revision = "HEAD"
	}
	out, err := git(ctx, clone, "rev-parse", "--verify", "--end-of-options", revision+"^{commit}").Output()
	if err != nil {
		return Stats{}, err
	}
	sha := strings.TrimSpace(string(out))
	tips := []string{sha}
	refs, err := git(ctx, clone, "for-each-ref", "--format=%(objecttype):%(objectname) %(*objecttype):%(*objectname)").Output()
	if err != nil {
		return Stats{}, err
	}
	for _, field := range strings.Fields(string(refs)) {
		if strings.HasPrefix(field, "commit:") {
			tips = append(tips, strings.TrimPrefix(field, "commit:"))
		}
	}
	if err = p.ingestHistoryTips(ctx, tips, clone); err != nil {
		return Stats{}, err
	}
	if err = p.PrepareSnapshot(ctx, sha); err != nil {
		return Stats{}, err
	}
	if err = p.importReferences(ctx, source); err != nil {
		return Stats{}, err
	}
	return Stats{Generation: sha}, nil
}

// Replace the small reference index as one publication so deleted refs vanish.
func (p *Progressive) importReferences(ctx context.Context, source string) error {
	raw, err := git(ctx, source, "for-each-ref", "--format=%(refname) %(objectname) %(symref)").Output()
	if err != nil {
		return err
	}
	head, err := git(ctx, source, "rev-parse", "HEAD").Output()
	if err != nil {
		return err
	}
	symbolic, _ := git(ctx, source, "symbolic-ref", "-q", "HEAD").Output()
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	lines = append(lines, "HEAD "+strings.TrimSpace(string(head))+" "+strings.TrimSpace(string(symbolic)))
	p.writer.Lock()
	defer p.writer.Unlock()
	return p.stage(func(changes *bolt.Bucket) error {
		for _, line := range lines {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}
			peeled, err := git(ctx, source, "rev-parse", "--verify", fields[1]+"^{}").Output()
			if err != nil {
				return err
			}
			ref := reference{Commit: strings.TrimSpace(string(peeled)), ObjectID: fields[1]}
			if len(fields) > 2 {
				ref.SymbolicTarget = fields[2]
			}
			b, err := marshal(ref)
			if err != nil {
				return err
			}
			if err = changes.Put([]byte("r/"+fields[0]), b); err != nil {
				return err
			}
			if ref.ObjectID != ref.Commit {
				if err = changes.Put([]byte("a/"+ref.ObjectID), b); err != nil {
					return err
				}
			}
		}
		idx := &index{store: p.store, cache: p.cache, containers: true}
		root, err := idx.update(ctx, changes)
		if err != nil {
			return err
		}
		// The refs index is separate from the repository object index.
		cursor := changes.Cursor()
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if err := cursor.Delete(); err != nil {
				return err
			}
		}
		data, err := marshal(root)
		if err != nil {
			return err
		}
		if err = changes.Put([]byte("refs-root"), data); err != nil {
			return err
		}
		return p.publish(ctx, changes)
	})
}
