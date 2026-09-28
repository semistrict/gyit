package repo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"

	"gyit/internal/store"
)

type viewRef struct {
	name string
	reference
}

// ViewRefs reads every reference from one publication; HEAD remains the mounted
// selection even when an importer publishes a newer branch tip.
func (r *Repository) ViewRefs(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	m, err := r.queryManifest(ctx)
	if err != nil {
		return err
	}
	idx := &index{store: r.store, cache: r.cache, root: m.Refs}
	refs, err := readViewRefs(ctx, idx)
	if err != nil {
		return err
	}
	switch opt.Command {
	case "branch", "tag":
		return r.viewRefList(ctx, current, opt, out, m, refs)
	case "show-ref":
		return r.viewShowRef(ctx, current, opt, out, m, refs)
	case "rev-parse":
		return r.viewRevParse(ctx, current, opt, out, m, refs)
	default:
		return fmt.Errorf("unknown reference command %q", opt.Command)
	}
}

func readViewRefs(ctx context.Context, idx *index) ([]viewRef, error) {
	var refs []viewRef
	retained := 0
	after := ""
	for {
		batch, err := idx.scan(ctx, "r/", after, fanout)
		if err != nil {
			return nil, err
		}
		for _, item := range batch {
			var ref reference
			if err := unmarshal(item.Value, &ref); err != nil {
				return nil, err
			}
			retained += 256 + len(item.Key) + len(ref.Commit) + len(ref.ObjectID) + len(ref.SymbolicTarget)
			if retained > 16<<20 {
				return nil, fmt.Errorf("reference listing exceeds the 16 MiB command memory limit")
			}
			refs = append(refs, viewRef{strings.TrimPrefix(item.Key, "r/"), ref})
			after = item.Key
		}
		if len(batch) < fanout {
			return refs, nil
		}
	}
}

func matchViewRef(name string, patterns []string) (bool, error) {
	if len(patterns) == 0 {
		return true, nil
	}
	for _, pattern := range patterns {
		// Git's reference wildcards cross slash boundaries.
		ok, err := path.Match(strings.ReplaceAll(pattern, "/", "\x00"), strings.ReplaceAll(name, "/", "\x00"))
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func (r *Repository) viewRefList(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer, m manifest, refs []viewRef) error {
	all, remote, verbose, list, showCurrent, noAbbrev := false, false, false, false, false, false
	format := ""
	abbrev := 7
	var patterns []string
	for i := 0; i < len(opt.Args); i++ {
		a := opt.Args[i]
		switch {
		case a == "--":
			patterns = append(patterns, opt.Args[i+1:]...)
			i = len(opt.Args)
		case a == "-av" || a == "-va":
			all, verbose = true, true
		case a == "-rv" || a == "-vr":
			remote, verbose = true, true
		case a == "-a" || a == "--all":
			all = true
		case a == "-r" || a == "--remotes":
			remote = true
		case a == "-v" || a == "--verbose":
			verbose = true
		case a == "-l" || a == "--list":
			list = true
		case a == "--show-current":
			showCurrent = true
		case a == "--no-color" || a == "--color=never":
		case a == "--no-abbrev":
			noAbbrev = true
		case strings.HasPrefix(a, "--abbrev="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--abbrev="))
			if err != nil || n < 4 {
				return fmt.Errorf("invalid abbreviation")
			}
			abbrev = n
		case strings.HasPrefix(a, "--format="):
			format = strings.TrimPrefix(a, "--format=")
		case a == "--format":
			if i+1 == len(opt.Args) {
				return fmt.Errorf("--format requires a value")
			}
			i++
			format = opt.Args[i]
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("%s: unsupported or mutating option %q; only reference listing is supported", opt.Command, a)
		default:
			patterns = append(patterns, a)
		}
	}
	if all || remote || verbose || format != "" {
		list = true
	}
	if len(patterns) > 0 && !list {
		return fmt.Errorf("%s is read-only; use --list before patterns", opt.Command)
	}
	if opt.Command == "tag" && (all || remote || verbose || showCurrent || noAbbrev) {
		return fmt.Errorf("unsupported tag listing option")
	}
	if showCurrent {
		if current.Branch == "" {
			return nil
		}
		_, err := fmt.Fprintln(out, current.Branch)
		return err
	}
	type row struct{ name, ref, sha, mark, original, target string }
	var rows []row
	for _, ref := range refs {
		name := ""
		if opt.Command == "tag" {
			if strings.HasPrefix(ref.name, "refs/tags/") {
				name = strings.TrimPrefix(ref.name, "refs/tags/")
			}
		} else {
			if strings.HasPrefix(ref.name, "refs/heads/") && !remote {
				name = strings.TrimPrefix(ref.name, "refs/heads/")
			}
			if strings.HasPrefix(ref.name, "refs/remotes/") && (remote || all) {
				name = strings.TrimPrefix(ref.name, "refs/remotes/")
				if all {
					name = "remotes/" + name
				}
			}
		}
		if name == "" {
			continue
		}
		ok, err := matchViewRef(name, patterns)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		mark := " "
		if ref.name == "refs/heads/"+current.Branch && current.Branch != "" {
			mark = "*"
		}
		rows = append(rows, row{name, ref.name, ref.Commit, mark, ref.ObjectID, ref.SymbolicTarget})
	}
	if opt.Command == "branch" && !remote && current.Branch == "" {
		label := current.DetachedAt
		if label == "" {
			label = current.SHA[:min(7, len(current.SHA))]
		}
		name := "(HEAD detached at " + label + ")"
		ok, err := matchViewRef(name, patterns)
		if err != nil {
			return err
		}
		if ok {
			rows = append(rows, row{name, "HEAD", current.SHA, "*", current.SHA, ""})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].ref == "HEAD" {
			return true
		}
		if rows[j].ref == "HEAD" {
			return false
		}
		return rows[i].ref < rows[j].ref
	})
	width := 0
	for _, row := range rows {
		width = max(width, len(row.name))
	}
	objects := newViewRefIndex(r.progressive.historyIndex())
	refidx := newViewRefIndex(&index{store: r.store, cache: r.cache, root: m.Refs})
	var subjects map[string]string
	if verbose || strings.Contains(format, "%(subject)") {
		if opt.Command == "tag" {
			for _, row := range rows {
				if row.original != "" && row.original != row.sha {
					return fmt.Errorf("annotated tag subject metadata is not imported")
				}
			}
		}
		ids := make([]string, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.sha)
		}
		var err error
		subjects, err = objects.subjects(ctx, ids)
		if err != nil {
			return err
		}
	}
	for _, row := range rows {
		line := row.name
		if opt.Command == "branch" && row.target != "" && format == "" {
			line = row.mark + " " + line + " -> " + strings.TrimPrefix(row.target, "refs/remotes/")
		} else if verbose || format != "" {
			subject, foundSubject := subjects[row.sha]
			if (verbose || strings.Contains(format, "%(subject)")) && !foundSubject {
				var c commitInfo
				if opt.Command == "tag" && row.original != "" && row.original != row.sha {
					return fmt.Errorf("annotated tag subject metadata is not imported")
				}
				if err := objects.get(ctx, "c/"+row.sha, &c); err != nil {
					return err
				}
				subject = viewRefSubject(c)
			}
			oid := row.sha
			if row.original != "" {
				oid = row.original
			}
			short := oid
			if verbose || strings.Contains(format, "%(objectname:short)") {
				var err error
				short, err = viewAbbreviate(ctx, objects, refidx, oid, abbrev)
				if err != nil {
					return err
				}
			}
			if noAbbrev {
				short = row.sha
			}
			if format != "" {
				line = strings.NewReplacer("%(refname:short)", strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(row.ref, "refs/heads/"), "refs/tags/"), "refs/remotes/"), "%(refname)", row.ref, "%(objectname)", oid, "%(symref)", row.target, "%(objectname:short)", short, "%(HEAD)", row.mark, "%(subject)", subject).Replace(format)
				if strings.Contains(line, "%(") {
					return fmt.Errorf("unsupported reference format atom")
				}
			} else {
				line = fmt.Sprintf("%s %-*s %s %s", row.mark, width, row.name, short, subject)
			}
		} else if opt.Command == "branch" {
			line = row.mark + " " + line
		}
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	return nil
}

type viewRefScanner interface {
	scan(context.Context, string, string, int) ([]item, error)
}

func viewAbbreviate(ctx context.Context, objects, refs viewRefScanner, oid string, length int) (string, error) {
	for n := min(max(4, length), len(oid)); n < len(oid); n++ {
		a, err := objects.scan(ctx, "o/"+oid[:n], "", 2)
		if err != nil {
			return "", err
		}
		b, err := refs.scan(ctx, "a/"+oid[:n], "", 2)
		if err != nil {
			return "", err
		}
		if distinctMatches(a, b) <= 1 {
			return oid[:n], nil
		}
	}
	return oid, nil
}

func (r *Repository) viewShowRef(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer, m manifest, refs []viewRef) error {
	heads, tags, head, verify, hash, quiet, deref := false, false, false, false, false, false, false
	length := 0
	var patterns []string
	for _, a := range opt.Args {
		switch {
		case a == "--heads" || a == "--branches":
			heads = true
		case a == "--tags":
			tags = true
		case a == "--head":
			head = true
		case a == "--verify":
			verify = true
		case a == "-q" || a == "--quiet":
			quiet = true
		case a == "-s" || a == "--hash":
			hash = true
		case a == "-d" || a == "--dereference":
			deref = true
		case a == "--abbrev":
			length = 7
		case strings.HasPrefix(a, "--hash=") || strings.HasPrefix(a, "--abbrev="):
			_, v, _ := strings.Cut(a, "=")
			n, err := strconv.Atoi(v)
			if err != nil || n < 4 {
				return fmt.Errorf("invalid abbreviation")
			}
			length = n
			if strings.HasPrefix(a, "--hash=") {
				hash = true
			}
		case a == "--":
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("show-ref: unsupported option %q", a)
		default:
			patterns = append(patterns, a)
		}
	}
	if verify && len(patterns) == 0 {
		return fmt.Errorf("show-ref --verify requires a reference")
	}
	objects := r.progressive.historyIndex()
	refidx := &index{store: r.store, cache: r.cache, root: m.Refs}
	var selected []viewRef
	if head {
		selected = append(selected, viewRef{"HEAD", reference{Commit: current.SHA}})
	}
	if verify {
		selected = nil
		for _, name := range patterns {
			found := false
			if name == "HEAD" {
				selected = append(selected, viewRef{"HEAD", reference{Commit: current.SHA}})
				continue
			}
			for _, ref := range refs {
				if ref.name == name && strings.HasPrefix(name, "refs/") {
					selected = append(selected, ref)
					found = true
					break
				}
			}
			if !found {
				if quiet {
					return ErrViewNoMatch
				}
				return fmt.Errorf("%s: %w", name, store.ErrNotFound)
			}
		}
	} else {
		for _, ref := range refs {
			if !strings.HasPrefix(ref.name, "refs/") {
				continue
			}
			if heads || tags {
				if !(heads && strings.HasPrefix(ref.name, "refs/heads/")) && !(tags && strings.HasPrefix(ref.name, "refs/tags/")) {
					continue
				}
			}
			ok := len(patterns) == 0
			for _, p := range patterns {
				if ref.name == p || strings.HasSuffix(ref.name, "/"+p) {
					ok = true
				}
			}
			if ok {
				selected = append(selected, ref)
			}
		}
	}
	if len(selected) == 0 {
		return ErrViewNoMatch
	}
	for _, ref := range selected {
		if quiet {
			continue
		}
		if ref.ObjectID == "" && strings.HasPrefix(ref.name, "refs/tags/") {
			return fmt.Errorf("tag object identity is missing; re-import to upgrade this store")
		}
		oid := ref.Commit
		// Original tag IDs are retained separately from peeled commit IDs.
		if ref.ObjectID != "" {
			oid = ref.ObjectID
		}
		ids := []string{oid}
		names := []string{ref.name}
		if deref && oid != ref.Commit {
			ids = append(ids, ref.Commit)
			names = append(names, ref.name+"^{}")
		}
		for i, id := range ids {
			if length > 0 {
				var err error
				id, err = viewAbbreviate(ctx, objects, refidx, id, length)
				if err != nil {
					return err
				}
			}
			var err error
			if hash {
				_, err = fmt.Fprintln(out, id)
			} else {
				_, err = fmt.Fprintf(out, "%s %s\n", id, names[i])
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Repository) viewRevParse(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer, m manifest, refs []viewRef) (result error) {
	quiet, endOptions := false, false
	verify, symbolic, full, abbrevRef := false, false, false, false
	length := 0
	defer func() {
		if quiet && verify && (errors.Is(result, ErrInvalidRevision) || errors.Is(result, store.ErrNotFound)) {
			result = ErrViewNoMatch
		}
	}()
	var revisions []string
	for _, a := range opt.Args {
		if endOptions {
			revisions = append(revisions, a)
			continue
		}
		switch {
		case a == "--verify":
			verify = true
		case a == "--quiet" || a == "-q":
			quiet = true
		case a == "--end-of-options":
			endOptions = true
		case a == "--short":
			length = 7
			verify = true
		case strings.HasPrefix(a, "--short="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--short="))
			if err != nil || n < 1 {
				return fmt.Errorf("invalid abbreviation")
			}
			length = n
			verify = true
		case a == "--symbolic":
			symbolic = true
		case a == "--symbolic-full-name":
			symbolic = true
			full = true
		case a == "--abbrev-ref" || a == "--abbrev-ref=strict" || a == "--abbrev-ref=loose":
			symbolic = true
			full = true
			abbrevRef = true
		case a == "--show-toplevel":
			if opt.MountRoot == "" {
				return fmt.Errorf("mount root unavailable")
			}
			if _, err := fmt.Fprintln(out, opt.MountRoot); err != nil {
				return err
			}
		case a == "--show-prefix":
			if _, err := fmt.Fprintln(out, strings.TrimSuffix(opt.Prefix, "/")+func() string {
				if opt.Prefix != "" {
					return "/"
				}
				return ""
			}()); err != nil {
				return err
			}
		case a == "--show-cdup":
			n := 0
			if opt.Prefix != "" {
				n = len(strings.Split(strings.Trim(opt.Prefix, "/"), "/"))
			}
			if _, err := fmt.Fprintln(out, strings.Repeat("../", n)); err != nil {
				return err
			}
		case a == "--is-inside-work-tree":
			if _, err := fmt.Fprintln(out, "true"); err != nil {
				return err
			}
		case a == "--is-bare-repository" || a == "--is-inside-git-dir":
			if _, err := fmt.Fprintln(out, "false"); err != nil {
				return err
			}
		case a == "--show-object-format":
			if _, err := fmt.Fprintln(out, m.Format); err != nil {
				return err
			}
		case a == "--all" || a == "--branches" || a == "--tags" || a == "--remotes":
			prefix := "refs/"
			if a == "--branches" {
				prefix = "refs/heads/"
			}
			if a == "--tags" {
				prefix = "refs/tags/"
			}
			if a == "--remotes" {
				prefix = "refs/remotes/"
			}
			for _, ref := range refs {
				if strings.HasPrefix(ref.name, prefix) {
					revisions = append(revisions, ref.name)
				}
			}
		case strings.HasPrefix(a, "-"):
			return fmt.Errorf("rev-parse: unsupported option %q", a)
		default:
			revisions = append(revisions, a)
		}
	}
	if verify && len(revisions) != 1 {
		if quiet {
			return ErrViewNoMatch
		}
		return fmt.Errorf("rev-parse --verify requires exactly one revision")
	}
	objects := r.progressive.historyIndex()
	refidx := &index{store: r.store, cache: r.cache, root: m.Refs}
	for _, rev := range revisions {
		refName := ""
		if rev == "HEAD" || rev == "@" {
			refName = "HEAD"
			if current.Branch != "" {
				refName = "refs/heads/" + current.Branch
			}
		}
		if rev != "HEAD" && rev != "@" {
			for _, candidate := range []string{rev, "refs/" + rev, "refs/tags/" + rev, "refs/heads/" + rev, "refs/remotes/" + rev, "refs/remotes/" + rev + "/HEAD"} {
				for _, ref := range refs {
					if ref.name == candidate {
						if refName != "" && refName != candidate && rev != "HEAD" && rev != "@" {
							return invalidRevision("ambiguous reference")
						}
						refName = candidate
					}
				}
			}
		}
		if symbolic {
			if refName == "" {
				var err error
				if strings.ContainsAny(rev, "^~:") {
					_, err = r.openRevision(ctx, m, rev, current.SHA)
				} else {
					_, err = resolveName(ctx, objects, refidx, rev, current.SHA, m.Format)
				}
				if err != nil {
					return err
				}
			}
			for steps := 0; steps <= len(refs); steps++ {
				target := ""
				for _, ref := range refs {
					if ref.name == refName {
						target = ref.SymbolicTarget
						break
					}
				}
				if target == "" {
					break
				}
				if steps == len(refs) {
					return fmt.Errorf("symbolic reference cycle")
				}
				refName = target
			}
			value := rev
			if full {
				if refName == "" {
					continue
				}
				value = refName
			}
			if abbrevRef {
				value = strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(value, "refs/heads/"), "refs/tags/"), "refs/remotes/")
			}
			if _, err := fmt.Fprintln(out, value); err != nil {
				return err
			}
			continue
		}
		oid := ""
		if rev == "HEAD" || rev == "@" {
			oid = current.SHA
		} else if refName != "" {
			for _, ref := range refs {
				if ref.name == refName {
					if ref.ObjectID == "" && strings.HasPrefix(ref.name, "refs/tags/") {
						return fmt.Errorf("tag object identity is missing; re-import to upgrade this store")
					}
					oid = ref.Commit
					if ref.ObjectID != "" {
						oid = ref.ObjectID
					}
					break
				}
			}
		}
		if oid == "" && !strings.ContainsAny(rev, "^~:") {
			target, err := resolveName(ctx, objects, refidx, rev, current.SHA, m.Format)
			if err != nil {
				return err
			}
			oid = target.sha
			// resolveName peels tags for checkout. Recover an original tag ID
			// when a caller names that object, including an unambiguous prefix.
			tags, err := refidx.scan(ctx, "a/"+strings.ToLower(rev), "", 2)
			if err != nil {
				return err
			}
			if len(tags) == 1 {
				oid = strings.TrimPrefix(tags[0].Key, "a/")
			}
		}
		if oid == "" {
			// Object selectors can peel to trees/blobs without being mountable.
			// Keep this identical to cat-file/ls-tree's object resolution.
			_, resolved, _, err := r.resolveViewObject(ctx, current, opt.Prefix, rev)
			if err != nil {
				return err
			}
			oid = resolved
		}
		if length > 0 {
			var err error
			oid, err = viewAbbreviate(ctx, objects, refidx, oid, length)
			if err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(out, oid); err != nil {
			return err
		}
	}
	return nil
}
