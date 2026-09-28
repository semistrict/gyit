package repo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gyit/internal/pathspec"
	"gyit/internal/store"
)

// ViewObjects implements queries against immutable committed objects.
func (r *Repository) ViewObjects(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	switch opt.Command {
	case "ls-tree":
		return r.viewLSTree(ctx, current, opt, out)
	case "ls-files":
		return r.viewLSFiles(ctx, current, opt, out)
	case "cat-file":
		return r.viewCatFile(ctx, current, opt, out)
	case "grep":
		return r.viewGrep(ctx, current, opt, out)
	default:
		return fmt.Errorf("unknown object view command %q", opt.Command)
	}
}

// resolveViewObject returns a snapshot whose catalog contains the selected
// object. REV:path is rooted at the repository except for ./ and ../ selectors.
func (r *Repository) resolveViewObject(ctx context.Context, current *Snapshot, prefix, selector string) (*Snapshot, string, object, error) {
	if rev, name, ok := strings.Cut(selector, ":"); ok {
		if rev == "" {
			return nil, "", object{}, fmt.Errorf("index-stage selectors are unsupported")
		}
		// Git's REV:path accepts any tree-ish, including a lightweight or
		// annotated tag whose final target is a tree rather than a commit.
		s, _, _, err := r.resolveViewObject(ctx, current, prefix, rev+"^{tree}")
		if err != nil {
			return nil, "", object{}, err
		}
		if strings.HasPrefix(name, "./") || strings.HasPrefix(name, "../") {
			name = path.Join(prefix, name)
		}
		name = path.Clean(name)
		if name == "." {
			name = ""
		}
		if strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
			return nil, "", object{}, fmt.Errorf("object path is outside repository")
		}
		e, err := s.Resolve(ctx, name)
		if err != nil {
			return nil, "", object{}, err
		}
		if e.Mode == 0100644 || e.Mode == 0100755 || e.Mode == 0120000 {
			return s, e.OID, object{Kind: "blob", Size: e.Size}, nil
		}
		var o object
		err = s.idx.get(ctx, "o/"+e.OID, &o)
		return s, e.OID, o, err
	}
	tree := strings.HasSuffix(selector, "^{tree}")
	if tree {
		selector = strings.TrimSuffix(selector, "^{tree}")
	}
	blob := strings.HasSuffix(selector, "^{blob}")
	if blob {
		selector = strings.TrimSuffix(selector, "^{blob}")
	}
	peel := strings.HasSuffix(selector, "^{}")
	for strings.HasSuffix(selector, "^{}") {
		selector = strings.TrimSuffix(selector, "^{}")
	}
	m, err := r.queryManifest(ctx)
	if err != nil {
		return nil, "", object{}, err
	}
	idx := r.progressive.historyIndex()
	refs := &index{store: r.store, cache: r.cache, root: m.Refs}
	var oid string
	// Preserve annotated tag identity for object inspection; the revision
	// resolver deliberately peels tags for checkout and history traversal.
	if !tree && !blob && !peel && !strings.ContainsAny(selector, "~^") && selector != "HEAD" && selector != "@" {
		keys := []string{"a/" + selector}
		for _, name := range []string{"refs/heads/" + selector, selector, "refs/" + selector, "refs/tags/" + selector, "refs/remotes/" + selector} {
			keys = append(keys, "r/"+name)
		}
		for _, key := range keys {
			var ref reference
			err := refs.get(ctx, key, &ref)
			if errors.Is(err, store.ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, "", object{}, err
			}
			original := ref.ObjectID
			if strings.HasPrefix(key, "a/") {
				original = strings.TrimPrefix(key, "a/")
			}
			if original != "" && original != ref.Commit {
				return &Snapshot{progressive: r.progressive, idx: idx, SHA: ref.Commit, Tree: current.Tree}, original, object{Kind: "tag", Size: -1}, nil
			}
			if original == "" && strings.HasPrefix(key, "r/refs/tags/") {
				return nil, "", object{}, fmt.Errorf("tag object identity is missing; re-import to upgrade this store")
			}
			break
		}
	}
	// Resolve ancestor syntax through the commit walker; direct object IDs may
	// identify blobs and trees and therefore cannot use OpenRevision.
	if strings.ContainsAny(selector, "~^") {
		s, e := r.openRevision(ctx, m, selector, current.SHA)
		if e != nil {
			return nil, "", object{}, e
		}
		oid = s.SHA
	} else {
		target, e := resolveName(ctx, idx, refs, selector, current.SHA, m.Format)
		if e != nil {
			return nil, "", object{}, e
		}
		oid = target.sha
		if !tree && !blob && !peel && len(selector) >= 4 && len(selector) < len(oid) {
			hexName := strings.ToLower(selector)
			if _, e := hex.DecodeString(hexName + strings.Repeat("0", len(hexName)%2)); e == nil {
				tags, e := refs.scan(ctx, "a/"+hexName, "", 2)
				if e != nil {
					return nil, "", object{}, e
				}
				if len(tags) == 1 {
					return &Snapshot{progressive: r.progressive, idx: idx, SHA: oid, Tree: current.Tree}, strings.TrimPrefix(tags[0].Key, "a/"), object{Kind: "tag", Size: -1}, nil
				}
			}
		}
	}
	var o object
	if err = idx.get(ctx, "o/"+oid, &o); err != nil {
		return nil, "", object{}, err
	}
	if tree && o.Kind == "commit" {
		oid = o.Tree
		err = idx.get(ctx, "o/"+oid, &o)
	}
	if tree && o.Kind != "tree" {
		return nil, "", object{}, fmt.Errorf("object is not a tree")
	}
	if blob && o.Kind != "blob" {
		return nil, "", object{}, fmt.Errorf("object is not a blob")
	}
	s := &Snapshot{progressive: r.progressive, idx: idx, SHA: current.SHA, Tree: current.Tree}
	if o.Kind == "commit" {
		s.SHA, s.Tree = oid, o.Tree
	}
	if o.Kind == "tree" {
		s.Tree = oid
	}
	return s, oid, o, err
}

func viewFlags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}
func viewQuoted(name string, nul bool) string {
	if nul {
		return name
	}
	return quoteDiffPath(name)
}
func viewRelative(name, prefix string) string {
	if prefix == "" {
		return name
	}
	v, err := filepath.Rel(prefix, name)
	if err != nil {
		return name
	}
	return filepath.ToSlash(v)
}
func viewKind(e Entry) string {
	switch e.Mode {
	case 0040000:
		return "tree"
	case 0160000:
		return "commit"
	default:
		return "blob"
	}
}

// A single directory is bounded independently of repository size. Git's tree
// order compares directories as though their name ends in '/'.
func viewTreeEntries(ctx context.Context, s *Snapshot, tree string) ([]Entry, error) {
	var entries []Entry
	after := ""
	budget := 0
	for {
		batch, err := s.ReadDir(ctx, tree, after, 128)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			break
		}
		for _, e := range batch {
			budget += len(e.Name) + len(e.OID) + 64
			if budget > 16<<20 {
				return nil, fmt.Errorf("directory listing exceeds 16 MiB budget")
			}
			entries = append(entries, e)
		}
		after = batch[len(batch)-1].Name
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i].Name, entries[j].Name
		if entries[i].Mode == 0040000 {
			a += "/"
		}
		if entries[j].Mode == 0040000 {
			b += "/"
		}
		return a < b
	})
	return entries, nil
}
func viewWalk(ctx context.Context, s *Snapshot, tree, base string, matcher *pathspec.Matcher, attrs *Snapshot, visit func(string, Entry) error) error {
	retained := 0
	return viewWalkBudget(ctx, s, tree, base, matcher, attrs, visit, &retained)
}
func viewEntriesBytes(entries []Entry) int {
	n := 0
	for _, e := range entries {
		n += len(e.Name) + len(e.OID) + 64
	}
	return n
}
func viewWalkBudget(ctx context.Context, s *Snapshot, tree, base string, matcher *pathspec.Matcher, attrs *Snapshot, visit func(string, Entry) error, retained *int) error {
	if strings.Count(base, "/") > 512 {
		return fmt.Errorf("tree depth exceeds 512")
	}
	entries, err := viewTreeEntries(ctx, s, tree)
	if err != nil {
		return err
	}
	used := viewEntriesBytes(entries)
	*retained += used
	defer func() { *retained -= used }()
	if *retained > 16<<20 {
		return fmt.Errorf("tree traversal exceeds 16 MiB directory budget")
	}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := path.Join(base, e.Name)
		if e.Mode == 0040000 {
			if matcher.MayDescend(name) {
				if err := viewWalkBudget(ctx, s, e.OID, name, matcher, attrs, visit, retained); err != nil {
					return err
				}
			}
			continue
		}
		ok, err := matcher.Match(name, func(n string, req []pathspec.Requirement) (bool, error) { return attrs.matchAttributes(ctx, n, req) })
		if err != nil {
			return err
		}
		if ok {
			if err := visit(name, e); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *Repository) viewLSTree(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := viewFlags("ls-tree")
	recursive := f.Bool("r", false, "")
	trees := f.Bool("t", false, "")
	dirs := f.Bool("d", false, "")
	names := f.Bool("name-only", false, "")
	f.BoolVar(names, "name-status", false, "")
	long := f.Bool("l", false, "")
	f.BoolVar(long, "long", false, "")
	nul := f.Bool("z", false, "")
	fullTree := f.Bool("full-tree", false, "")
	fullName := f.Bool("full-name", false, "")
	abbrev := f.Int("abbrev", 0, "")
	if err := parseViewFlags(f, opt.Args); err != nil {
		return err
	}
	args := f.Args()
	if len(args) < 1 {
		return fmt.Errorf("ls-tree requires a tree-ish")
	}
	s, oid, o, err := r.resolveViewObject(ctx, current, opt.Prefix, args[0])
	if err != nil {
		return err
	}
	if o.Kind == "tag" {
		s, oid, o, err = r.resolveViewObject(ctx, current, opt.Prefix, args[0]+"^{tree}")
		if err != nil {
			return err
		}
	}
	if o.Kind == "commit" {
		oid = o.Tree
	} else if o.Kind != "tree" {
		return fmt.Errorf("not a tree object")
	}
	prefix := opt.Prefix
	if *fullTree {
		prefix = ""
	}
	paths := args[1:]
	if len(paths) == 0 && prefix != "" {
		paths = []string{"."}
	}
	matcher, err := pathspec.Compile(paths, prefix)
	if err != nil {
		return err
	}
	end := "\n"
	if *nul {
		end = "\x00"
	}
	if *abbrev < 0 || *abbrev > 64 {
		return fmt.Errorf("invalid abbreviation length")
	}
	retained := 0
	var walk func(string, string) error
	walk = func(tree, base string) error {
		if strings.Count(base, "/") > 512 {
			return fmt.Errorf("tree depth exceeds 512")
		}
		entries, err := viewTreeEntries(ctx, s, tree)
		if err != nil {
			return err
		}
		used := viewEntriesBytes(entries)
		retained += used
		defer func() { retained -= used }()
		if retained > 16<<20 {
			return fmt.Errorf("tree traversal exceeds 16 MiB directory budget")
		}
		for _, e := range entries {
			name := path.Join(base, e.Name)
			isdir := e.Mode == 0040000
			match, err := matcher.Match(name, func(n string, req []pathspec.Requirement) (bool, error) { return current.matchAttributes(ctx, n, req) })
			if err != nil {
				return err
			}
			// Literal paths descending into a directory implicitly recurse into it.
			descend := isdir && matcher.MayDescend(name) && (*recursive || !match || name == prefix)
			emit := match && name != prefix && (!*dirs || isdir) && (!isdir || !*recursive || *trees || *dirs)
			if emit {
				display := name
				if !*fullName && !*fullTree {
					display = viewRelative(name, prefix)
				}
				display = viewQuoted(display, *nul)
				if *names {
					_, err = io.WriteString(out, display+end)
				} else {
					id := e.OID
					if *abbrev > 0 && *abbrev < len(id) {
						id = id[:*abbrev]
					}
					if *long {
						size := "-"
						if viewKind(e) == "blob" {
							size = strconv.FormatInt(e.Size, 10)
						}
						_, err = fmt.Fprintf(out, "%06o %s %s %7s\t%s%s", e.Mode, viewKind(e), id, size, display, end)
					} else {
						_, err = fmt.Fprintf(out, "%06o %s %s\t%s%s", e.Mode, viewKind(e), id, display, end)
					}
				}
				if err != nil {
					return err
				}
			}
			if descend {
				if err := walk(e.OID, name); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(oid, "")
}
func (r *Repository) viewLSFiles(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := viewFlags("ls-files")
	nul := f.Bool("z", false, "")
	stage := f.Bool("stage", false, "")
	f.BoolVar(stage, "s", false, "")
	full := f.Bool("full-name", false, "")
	f.Bool("cached", false, "")
	f.Bool("c", false, "")
	if err := parseViewFlags(f, opt.Args); err != nil {
		return err
	}
	paths := f.Args()
	if len(paths) == 0 && opt.Prefix != "" {
		paths = []string{"."}
	}
	matcher, err := pathspec.Compile(paths, opt.Prefix)
	if err != nil {
		return err
	}
	end := "\n"
	if *nul {
		end = "\x00"
	}
	return viewWalk(ctx, current, current.Tree, "", matcher, current, func(name string, e Entry) error {
		if !*full {
			name = viewRelative(name, opt.Prefix)
		}
		name = viewQuoted(name, *nul)
		var err error
		if *stage {
			_, err = fmt.Fprintf(out, "%06o %s 0\t%s%s", e.Mode, e.OID, name, end)
		} else {
			_, err = io.WriteString(out, name+end)
		}
		return err
	})
}

func (r *Repository) viewCatFile(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := viewFlags("cat-file")
	pretty := f.Bool("p", false, "")
	kind := f.Bool("t", false, "")
	size := f.Bool("s", false, "")
	exists := f.Bool("e", false, "")
	if err := parseViewFlags(f, opt.Args); err != nil {
		return err
	}
	n := 0
	for _, v := range []bool{*pretty, *kind, *size, *exists} {
		if v {
			n++
		}
	}
	args := f.Args()
	want := ""
	if n == 0 && len(args) == 2 {
		want, args = args[0], args[1:]
	} else if n != 1 {
		return fmt.Errorf("cat-file requires exactly one of -p, -t, -s, -e or an object type")
	}
	if len(args) != 1 {
		return fmt.Errorf("cat-file requires one object")
	}
	s, oid, o, err := r.resolveViewObject(ctx, current, opt.Prefix, args[0])
	if err != nil {
		if *exists && errors.Is(err, store.ErrNotFound) {
			return ErrViewNoMatch
		}
		return err
	}
	if *exists {
		return nil
	}
	if *kind {
		_, err = fmt.Fprintln(out, o.Kind)
		return err
	}
	if *size {
		if o.Size < 0 {
			return fmt.Errorf("original %s size is unavailable in this store", o.Kind)
		}
		_, err = fmt.Fprintln(out, o.Size)
		return err
	}
	if (want == "tree" || want == "blob") && o.Kind == "tag" {
		s, oid, o, err = r.resolveViewObject(ctx, current, opt.Prefix, args[0]+"^{"+want+"}")
		if err != nil {
			return err
		}
	}
	if want != "" && want != o.Kind {
		if want == "tree" && o.Kind == "commit" {
			oid = o.Tree
			o.Kind = "tree"
		} else {
			return fmt.Errorf("object is %s, not %s", o.Kind, want)
		}
	}
	switch o.Kind {
	case "blob":
		return viewCopyBlob(ctx, s, oid, o.Size, out)
	case "tree":
		if *pretty {
			return r.viewLSTree(ctx, current, ViewOptions{Command: "ls-tree", Args: []string{"--full-tree", oid}}, out)
		}
		entries, err := viewTreeEntries(ctx, s, oid)
		if err != nil {
			return err
		}
		for _, e := range entries {
			raw, err := hex.DecodeString(e.OID)
			if err != nil {
				return err
			}
			mode := e.Mode
			if e.RawMode != 0 {
				mode = e.RawMode
			}
			if _, err = fmt.Fprintf(out, "%o %s\x00", mode, e.Name); err != nil {
				return err
			}
			if _, err = out.Write(raw); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("raw %s contents are unavailable in this store; original headers are not retained", o.Kind)
	}
}
func viewCopyBlob(ctx context.Context, s *Snapshot, oid string, size int64, out io.Writer) error {
	_, err := io.CopyBuffer(out, &viewBlobReader{ctx: ctx, s: s, oid: oid, size: size}, make([]byte, 128<<10))
	return err
}

type viewPatterns []string

func (v *viewPatterns) String() string     { return strings.Join(*v, ",") }
func (v *viewPatterns) Set(s string) error { *v = append(*v, s); return nil }

type viewBlobReader struct {
	ctx       context.Context
	s         *Snapshot
	oid       string
	off, size int64
	data      []byte
	part      int64
}

func (r *viewBlobReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.off >= r.size {
		return 0, io.EOF
	}
	n, err := r.s.ReadAt(r.ctx, r.oid, p[:min(int64(len(p)), r.size-r.off)], r.off)
	r.off += int64(n)
	return n, err
}

// basicViewRegex translates the POSIX basic operators to RE2 syntax. Backrefs
// are rejected instead of silently assigning them different semantics.
func basicViewRegex(p string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == '\\' && i+1 < len(p) {
			i++
			c = p[i]
			if c >= '1' && c <= '9' {
				return "", fmt.Errorf("regular-expression backreferences are unsupported")
			}
			if strings.ContainsRune("()+?|{}", rune(c)) {
				b.WriteByte(c)
			} else {
				b.WriteByte('\\')
				b.WriteByte(c)
			}
			continue
		}
		if strings.ContainsRune("()+?|{}", rune(c)) {
			b.WriteByte('\\')
		}
		b.WriteByte(c)
	}
	return b.String(), nil
}
func (r *Repository) viewGrep(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := viewFlags("grep")
	number := f.Bool("n", false, "")
	f.BoolVar(number, "line-number", false, "")
	names := f.Bool("l", false, "")
	f.BoolVar(names, "files-with-matches", false, "")
	ignore := f.Bool("i", false, "")
	f.BoolVar(ignore, "ignore-case", false, "")
	fixed := f.Bool("F", false, "")
	f.BoolVar(fixed, "fixed-strings", false, "")
	extended := f.Bool("E", false, "")
	f.BoolVar(extended, "extended-regexp", false, "")
	basic := f.Bool("G", false, "")
	f.BoolVar(basic, "basic-regexp", false, "")
	invert := f.Bool("v", false, "")
	count := f.Bool("c", false, "")
	quiet := f.Bool("q", false, "")
	nul := f.Bool("z", false, "")
	text := f.Bool("a", false, "")
	binarySkip := f.Bool("I", false, "")
	noName := f.Bool("h", false, "")
	withName := f.Bool("H", false, "")
	f.Bool("cached", false, "")
	full := f.Bool("full-name", false, "")
	var patterns viewPatterns
	f.Var(&patterns, "e", "")
	f.Var(&patterns, "regexp", "")
	if err := parseViewFlags(f, opt.Args); err != nil {
		return err
	}
	args := f.Args()
	if len(patterns) == 0 {
		if len(args) == 0 {
			return fmt.Errorf("grep requires a pattern")
		}
		patterns = append(patterns, args[0])
		args = args[1:]
	}
	if (*fixed && *extended) || (*basic && (*fixed || *extended)) {
		return fmt.Errorf("choose one pattern syntax: -G, -E, or -F")
	}
	if *noName && *withName {
		return fmt.Errorf("-h and -H cannot be combined")
	}
	var expr []string
	for _, p := range patterns {
		for _, p := range strings.Split(p, "\n") {
			if *fixed {
				p = regexp.QuoteMeta(p)
			} else if !*extended {
				var err error
				p, err = basicViewRegex(p)
				if err != nil {
					return err
				}
			}
			expr = append(expr, "(?:"+p+")")
		}
	}
	pattern := strings.Join(expr, "|")
	if *ignore {
		pattern = "(?i)" + pattern
	}
	rx, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	// Arguments before -- can be any number of commit, tree, or blob
	// selectors. Resolve all selectors before emitting results so a malformed
	// later selector cannot silently become a path filter or partial success.
	type grepTarget struct {
		snapshot         *Snapshot
		oid, label, kind string
		size             int64
	}
	var targets []grepTarget
	paths := args
	before, delimited := len(args), false
	for i := 0; i < len(opt.Args); i++ {
		arg := opt.Args[i]
		// A pattern value can itself be "--"; only an unconsumed token ends options.
		if arg == "-e" || arg == "--e" || arg == "--regexp" || arg == "-regexp" {
			i++
			continue
		}
		if arg == "--" {
			before = max(0, len(args)-(len(opt.Args)-i-1))
			delimited = true
			break
		}
	}
	for consumed := 0; consumed < before; consumed++ {
		selector := args[consumed]
		if !delimited && strings.HasPrefix(selector, ":") {
			break
		}
		candidate, oid, obj, e := r.resolveViewObject(ctx, current, opt.Prefix, selector)
		if e != nil {
			if delimited || (strings.Contains(selector, ":") && !strings.HasPrefix(selector, ":")) || strings.ContainsAny(selector, "~^") || (!errors.Is(e, store.ErrNotFound) && !errors.Is(e, ErrInvalidRevision)) {
				return e
			}
			// Unseparated paths are allowed only after the object list ends.
			// In particular, an invalid later revision is never swallowed.
			for _, arg := range args[consumed:before] {
				if strings.Contains(arg, ":") && !strings.HasPrefix(arg, ":") || strings.ContainsAny(arg, "~^") {
					return fmt.Errorf("invalid object selector %q; use -- before path filters", arg)
				}
				if len(targets) > 0 && !strings.HasPrefix(arg, ":") && !strings.ContainsAny(arg, "*?[") {
					if _, pathErr := current.Resolve(ctx, path.Join(opt.Prefix, arg)); pathErr != nil {
						return fmt.Errorf("cannot resolve %q as an object or path; use -- before path filters: %w", arg, pathErr)
					}
				}
			}
			break
		}
		if obj.Kind == "tag" {
			candidate, oid, obj, e = r.resolveViewObject(ctx, current, opt.Prefix, selector+"^{}")
			if e != nil {
				return e
			}
		}
		if obj.Kind == "commit" {
			oid, obj.Kind = obj.Tree, "tree"
		}
		if obj.Kind != "tree" && obj.Kind != "blob" {
			return fmt.Errorf("grep cannot search %s objects", obj.Kind)
		}
		targets = append(targets, grepTarget{candidate, oid, selector, obj.Kind, obj.Size})
		paths = args[consumed+1:]
	}
	if len(targets) == 0 {
		targets = []grepTarget{{current, current.Tree, "", "tree", 0}}
	}
	if len(paths) == 0 && opt.Prefix != "" {
		paths = []string{"."}
	}
	matcher, err := pathspec.Compile(paths, opt.Prefix)
	if err != nil {
		return err
	}
	matched := false
	stop := errors.New("grep stop")
	reader := bufio.NewReaderSize(nil, 32<<10)
	scanBuffer := make([]byte, 32<<10)
	searchFile := func(s *Snapshot, name string, e Entry, revPrefix string) error {
		if e.Mode&0170000 != 0100000 {
			return nil
		}
		display := name
		if !*full {
			display = viewRelative(display, opt.Prefix)
		}
		quoted := revPrefix + viewQuoted(display, *nul)
		display = revPrefix + display
		reader.Reset(&viewBlobReader{ctx: ctx, s: s, oid: e.OID, size: e.Size})
		peek, er := reader.Peek(int(min(e.Size, 8000)))
		if er != nil && er != io.EOF {
			return er
		}
		binary := bytes.IndexByte(peek, 0) >= 0
		if binary && *binarySkip {
			return nil
		}
		scanner := bufio.NewScanner(reader)
		scanner.Split(viewGrepLines)
		scanner.Buffer(scanBuffer, 4<<20)
		line, total := 0, 0
		for scanner.Scan() {
			line++
			hit := rx.Match(scanner.Bytes())
			if *invert {
				hit = !hit
			}
			if !hit {
				continue
			}
			matched = true
			total++
			if *quiet {
				return stop
			}
			if *names {
				end := "\n"
				if *nul {
					end = "\x00"
				}
				_, er = io.WriteString(out, quoted+end)
				return er
			}
			if *count {
				continue
			}
			if binary && !*text {
				_, er = fmt.Fprintf(out, "Binary file %s matches\n", display)
				return er
			}
			if !*noName {
				sep := ":"
				if *nul {
					sep = "\x00"
				}
				if _, er = io.WriteString(out, quoted+sep); er != nil {
					return er
				}
			}
			if *number {
				if _, er = fmt.Fprintf(out, "%d%s", line, func() string {
					if *nul {
						return "\x00"
					}
					return ":"
				}()); er != nil {
					return er
				}
			}
			if _, er = out.Write(scanner.Bytes()); er != nil {
				return er
			}
			if _, er = io.WriteString(out, "\n"); er != nil {
				return er
			}
		}
		if er = scanner.Err(); er != nil {
			return fmt.Errorf("grep %s: line exceeds 4 MiB budget or read failed: %w", name, er)
		}
		if *count && total > 0 {
			if !*noName {
				sep := ":"
				if *nul {
					sep = "\x00"
				}
				if _, er = io.WriteString(out, quoted+sep); er != nil {
					return er
				}
			}
			_, er = fmt.Fprintln(out, total)
			return er
		}
		return nil
	}
	for _, target := range targets {
		if target.kind == "blob" {
			// Git labels an explicit blob by its selector, without a trailing
			// colon, and ignores path filters because no tree is being walked.
			err = searchFile(target.snapshot, target.label, Entry{OID: target.oid, Mode: 0100644, Size: target.size}, "")
		} else {
			label := ""
			if target.label != "" {
				label = target.label + ":"
			}
			err = viewWalk(ctx, target.snapshot, target.oid, "", matcher, current, func(name string, e Entry) error { return searchFile(target.snapshot, name, e, label) })
		}
		if errors.Is(err, stop) {
			break
		}
		if err != nil {
			return err
		}
	}
	if !matched {
		return ErrViewNoMatch
	}
	return nil
}

// Git grep preserves carriage returns in both matching and output.
func viewGrepLines(data []byte, atEOF bool) (int, []byte, error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}
