package repo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"gyit/internal/store"
)

var ErrInvalidRevision = errors.New("invalid revision")

func invalidRevision(message string) error { return fmt.Errorf("%w: %s", ErrInvalidRevision, message) }

// OpenRevision resolves against one published metadata root. HEAD is local to
// the mount when current is supplied; refs always use the latest publication.
func (r *Repository) OpenRevision(ctx context.Context, revision, current string) (*Snapshot, error) {
	snapshots, err := r.OpenRevisions(ctx, []string{revision}, current)
	if err != nil {
		return nil, err
	}
	return snapshots[0], nil
}

// OpenRevisions pins all endpoints to one publication, even during an import.
func (r *Repository) OpenRevisions(ctx context.Context, revisions []string, current string) ([]*Snapshot, error) {
	m, err := r.queryManifest(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*Snapshot, 0, len(revisions))
	for _, revision := range revisions {
		s, err := r.openRevision(ctx, m, revision, current)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (r *Repository) openRevision(ctx context.Context, m manifest, revision, current string) (*Snapshot, error) {
	if revision == "" || len(revision) > 4096 {
		return nil, invalidRevision("expected a revision")
	}
	if strings.ContainsAny(revision, ": \t\n") || strings.Contains(revision, "..") || strings.Contains(revision, "@{") {
		return nil, invalidRevision("reflogs, ranges, message searches and path selectors are not supported")
	}
	var err error
	objects := r.progressive.historyIndex()
	refs := &index{store: r.store, cache: r.cache, root: m.Refs}
	base, suffix := revision, ""
	if pos := strings.IndexAny(revision, "^~"); pos >= 0 {
		base, suffix = revision[:pos], revision[pos:]
	}
	if base == "" {
		base = "HEAD"
	}
	target, err := resolveName(ctx, objects, refs, base, current, m.Format)
	if err != nil && r.progressive.ResolveRevision != nil && (!isHexRevision(base) || len(base) < 40) {
		target.sha, err = r.progressive.ResolveRevision(ctx, base)
	}
	if err != nil {
		return nil, err
	}
	sha := target.sha
	// Ref metadata also preserves tree/blob targets. Reject those before any
	// ancestor traversal, rather than interpreting a missing p/ row as history.
	var initial object
	if err := objects.get(ctx, "o/"+sha, &initial); err != nil {
		return nil, err
	}
	if initial.Kind != "commit" {
		return nil, invalidRevision("revision does not name a commit")
	}
	originalSuffix := suffix
	for suffix != "" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if strings.HasPrefix(suffix, "^{}") {
			suffix = suffix[3:]
			continue
		}
		if strings.HasPrefix(suffix, "^{commit}") {
			suffix = suffix[9:]
			continue
		}
		op := suffix[0]
		if op != '^' && op != '~' {
			return nil, invalidRevision("unsupported revision suffix")
		}
		suffix = suffix[1:]
		end := 0
		for end < len(suffix) && suffix[end] >= '0' && suffix[end] <= '9' {
			end++
		}
		count := uint64(1)
		if end > 0 {
			count, err = strconv.ParseUint(suffix[:end], 10, 64)
			if err != nil {
				return nil, invalidRevision("invalid ancestor count")
			}
		}
		suffix = suffix[end:]
		if count == 0 {
			continue
		}
		if !m.RevisionGraph {
			return nil, invalidRevision("parent metadata is missing; re-run import to upgrade this store")
		}
		steps := count
		if op == '^' {
			steps = 1
		}
		for step := uint64(0); step < steps; step++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			var p parents
			if err := objects.get(ctx, "p/"+sha, &p); err != nil {
				return nil, err
			}
			parent := uint64(1)
			if op == '^' {
				parent = count
			}
			if parent > uint64(len(p.Parents)) {
				return nil, fmt.Errorf("revision %q has no requested parent: %w", revision, store.ErrNotFound)
			}
			sha = p.Parents[parent-1]
		}
	}
	var object object
	if err := objects.get(ctx, "o/"+sha, &object); err != nil {
		return nil, err
	}
	if object.Kind != "commit" {
		return nil, invalidRevision("revision does not name a commit")
	}
	branch, label := target.branch, target.label
	if originalSuffix != "" {
		branch, label = "", ""
	}
	if branch == "" && label == "" {
		label, err = abbreviate(ctx, objects, refs, sha)
		if err != nil {
			return nil, err
		}
	}
	return &Snapshot{progressive: r.progressive, idx: objects, SHA: sha, Tree: object.Tree, Branch: branch, DetachedAt: label}, nil
}

type revisionTarget struct{ sha, branch, label string }

func resolveName(ctx context.Context, objects, refs *index, name, current, format string) (revisionTarget, error) {
	if name == "@" {
		name = "HEAD"
	}
	if name == "HEAD" && current != "" {
		return revisionTarget{sha: current}, nil
	}
	width := 40
	if format == "sha256" {
		width = 64
	}
	hash := strings.ToLower(name)
	isHex := true
	for _, c := range hash {
		if !strings.ContainsRune("0123456789abcdef", c) {
			isHex = false
			break
		}
	}
	if len(hash) == width && isHex {
		var o object
		if err := objects.get(ctx, "o/"+hash, &o); err == nil && o.Kind != "tag" {
			return revisionTarget{sha: hash}, nil
		} else if err != nil && !errors.Is(err, store.ErrNotFound) {
			return revisionTarget{}, err
		}
		var ref reference
		if err := refs.get(ctx, "a/"+hash, &ref); err != nil {
			return revisionTarget{}, err
		}
		return revisionTarget{sha: ref.Commit}, nil
	}
	// Checkout prefers a local branch over a same-named tag.
	names := []string{"refs/heads/" + name, name, "refs/" + name, "refs/tags/" + name, "refs/remotes/" + name, "refs/remotes/" + name + "/HEAD"}
	for _, candidate := range names {
		var ref reference
		if err := refs.get(ctx, "r/"+candidate, &ref); err == nil {
			if candidate == "refs/heads/"+name {
				return revisionTarget{sha: ref.Commit, branch: name}, nil
			}
			return revisionTarget{sha: ref.Commit, label: name}, nil
		} else if !errors.Is(err, store.ErrNotFound) {
			return revisionTarget{}, err
		}
	}
	// git describe output ends in -g<abbreviated object ID>.
	if pos := strings.LastIndex(name, "-g"); pos >= 0 {
		hash = strings.ToLower(name[pos+2:])
		_, err := hex.DecodeString(hash + strings.Repeat("0", len(hash)%2))
		isHex = err == nil
	}
	if isHex && len(hash) >= 4 && len(hash) < width {
		matches, err := objects.scan(ctx, "o/"+hash, "", 2)
		if err != nil {
			return revisionTarget{}, err
		}
		tags, err := refs.scan(ctx, "a/"+hash, "", 2)
		if err != nil {
			return revisionTarget{}, err
		}
		if distinctMatches(matches, tags) > 1 {
			return revisionTarget{}, invalidRevision("ambiguous abbreviated object ID")
		}
		if len(matches) == 1 && len(tags) == 0 {
			return revisionTarget{sha: strings.TrimPrefix(matches[0].Key, "o/")}, nil
		}
		if len(tags) == 1 {
			var ref reference
			if err := unmarshal(tags[0].Value, &ref); err != nil {
				return revisionTarget{}, err
			}
			return revisionTarget{sha: ref.Commit}, nil
		}
	}
	// Like checkout's remote guessing, a unique remote-tracking branch can be
	// selected by its branch name. Only bounded pages of ref metadata are read.
	after, found := "", ""
	for {
		batch, err := refs.scan(ctx, "r/refs/remotes/", after, 128)
		if err != nil {
			return revisionTarget{}, err
		}
		for _, item := range batch {
			rest := strings.TrimPrefix(item.Key, "r/refs/remotes/")
			if slash := strings.IndexByte(rest, '/'); slash >= 0 && rest[slash+1:] == name {
				if found != "" {
					return revisionTarget{}, invalidRevision("ambiguous remote branch; specify remote/branch")
				}
				var ref reference
				if err := unmarshal(item.Value, &ref); err != nil {
					return revisionTarget{}, err
				}
				found = ref.Commit
			}
			after = item.Key
		}
		if len(batch) < 128 {
			break
		}
	}
	if found != "" {
		return revisionTarget{sha: found, branch: name}, nil
	}
	return revisionTarget{}, fmt.Errorf("revision %q is not imported (re-run import to refresh refs): %w", name, store.ErrNotFound)
}

// Compute a unique display ID at switch time; status never walks the catalog.
func abbreviate(ctx context.Context, objects, refs *index, sha string) (string, error) {
	for length := min(7, len(sha)); length < len(sha); length++ {
		prefix := sha[:length]
		matches, err := objects.scan(ctx, "o/"+prefix, "", 2)
		if err != nil {
			return "", err
		}
		tags, err := refs.scan(ctx, "a/"+prefix, "", 2)
		if err != nil {
			return "", err
		}
		collision := false
		for _, match := range matches {
			if match.Key != "o/"+sha {
				collision = true
				break
			}
		}
		for _, tag := range tags {
			if tag.Key != "a/"+sha {
				collision = true
				break
			}
		}
		if !collision {
			return prefix, nil
		}
	}
	return sha, nil
}

func distinctMatches(groups ...[]item) int {
	seen := map[string]bool{}
	for _, group := range groups {
		for _, v := range group {
			_, oid, _ := strings.Cut(v.Key, "/")
			seen[oid] = true
		}
	}
	return len(seen)
}
