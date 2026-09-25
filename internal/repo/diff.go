package repo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strconv"
	"strings"
)

// DiffOptions paths are literal, repository-relative files or directory prefixes.
type DiffOptions struct {
	Paths                []string
	NameOnly, NameStatus bool
	Context              int
}

func validHistoryPath(p string) error {
	if len(p) > 4096 || strings.IndexByte(p, 0) >= 0 || strings.HasPrefix(p, "/") || p == ".." || strings.HasPrefix(p, "../") || path.Clean(p) != p {
		return invalidRevision("expected a clean repository-relative path")
	}
	return nil
}

func (s *Snapshot) historyBlob(ctx context.Context, e Entry) ([]byte, error) {
	if e.OID == "" {
		return nil, nil
	}
	if e.Mode == 0160000 {
		return []byte("Subproject commit " + e.OID + "\n"), nil
	}
	if e.Size > maxHistoryFile {
		return nil, fmt.Errorf("file exceeds the 8 MiB history limit; use diff --name-only")
	}
	b := make([]byte, int(e.Size))
	n, err := s.ReadAt(ctx, e.OID, b, 0)
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}

// Diff prunes identical trees and collects a bounded list of changed paths.
// Paths are sorted like Git; file contents are loaded and emitted one at a time.
func (s *Snapshot) Diff(ctx context.Context, to *Snapshot, opts DiffOptions, out io.Writer) error {
	if opts.Context < 0 || opts.Context > 100 {
		return invalidRevision("diff context must be 0..100")
	}
	if len(opts.Paths) > 128 {
		return invalidRevision("too many path filters")
	}
	for _, p := range opts.Paths {
		if err := validHistoryPath(p); err != nil {
			return err
		}
	}
	selected := func(p string) bool {
		if len(opts.Paths) == 0 {
			return true
		}
		for _, filter := range opts.Paths {
			if filter == "." || p == filter || strings.HasPrefix(p, filter+"/") || strings.HasPrefix(filter, p+"/") {
				return true
			}
		}
		return false
	}
	type change struct {
		path string
		a, b Entry
	}
	var changes []change
	retained := 0
	collect := func(p string, a, b Entry) error {
		retained += len(p) + len(a.OID) + len(b.OID) + len(a.Name) + len(b.Name) + 256
		if retained > 32<<20 || len(changes) >= 100000 {
			return fmt.Errorf("diff exceeds bounded changed-path buffer; narrow path filters")
		}
		changes = append(changes, change{p, a, b})
		return nil
	}
	var walk func(string, Entry, Entry, int) error
	walk = func(p string, a, b Entry, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if a.OID == b.OID && a.Mode == b.Mode {
			return nil
		}
		if depth > 256 {
			return fmt.Errorf("directory depth exceeds history limit")
		}
		if p != "" && !selected(p) {
			return nil
		}
		ad, bd := a.Mode == 0040000, b.Mode == 0040000
		if ad || bd {
			// File/directory replacements are a removal and an addition.
			if a.OID != "" && !ad {
				if err := collect(p, a, Entry{}); err != nil {
					return err
				}
			}
			if b.OID != "" && !bd {
				if err := collect(p, Entry{}, b); err != nil {
					return err
				}
			}
			ai := dirIterator{s: s, tree: a.OID}
			bi := dirIterator{s: to, tree: b.OID}
			if !ad {
				ai.tree = ""
			}
			if !bd {
				bi.tree = ""
			}
			ae, err := ai.next(ctx)
			if err != nil {
				return err
			}
			be, err := bi.next(ctx)
			if err != nil {
				return err
			}
			for ae.OID != "" || be.OID != "" {
				var x, y Entry
				name := ""
				switch {
				case be.OID == "" || (ae.OID != "" && ae.Name < be.Name):
					x = ae
					name = ae.Name
					ae, err = ai.next(ctx)
				case ae.OID == "" || be.Name < ae.Name:
					y = be
					name = be.Name
					be, err = bi.next(ctx)
				default:
					x, y = ae, be
					name = ae.Name
					ae, err = ai.next(ctx)
					if err == nil {
						be, err = bi.next(ctx)
					}
				}
				if err != nil {
					return err
				}
				if err := walk(path.Join(p, name), x, y, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		return collect(p, a, b)
	}
	if err := walk("", Entry{OID: s.Tree, Mode: 0040000}, Entry{OID: to.Tree, Mode: 0040000}, 0); err != nil {
		return err
	}
	sort.SliceStable(changes, func(i, j int) bool { return changes[i].path < changes[j].path })
	for _, c := range changes {
		if err := s.diffFile(ctx, to, c.path, c.a, c.b, opts, out); err != nil {
			return err
		}
	}
	return nil
}

type dirIterator struct {
	s           *Snapshot
	tree, after string
	batch       []Entry
	done        bool
}

func (d *dirIterator) next(ctx context.Context) (Entry, error) {
	if len(d.batch) == 0 && !d.done && d.tree != "" {
		var err error
		d.batch, err = d.s.ReadDir(ctx, d.tree, d.after, 128)
		if err != nil {
			return Entry{}, err
		}
		d.done = len(d.batch) < 128
	}
	if len(d.batch) == 0 {
		return Entry{}, nil
	}
	e := d.batch[0]
	d.batch = d.batch[1:]
	d.after = e.Name
	return e, nil
}

func quoteDiffPath(p string) string {
	var b strings.Builder
	quoted := false
	for _, c := range []byte(p) {
		switch c {
		case '\t':
			b.WriteString(`\t`)
			quoted = true
		case '\n':
			b.WriteString(`\n`)
			quoted = true
		case '\r':
			b.WriteString(`\r`)
			quoted = true
		case '\b':
			b.WriteString(`\b`)
			quoted = true
		case '\f':
			b.WriteString(`\f`)
			quoted = true
		case '\v':
			b.WriteString(`\v`)
			quoted = true
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
			quoted = true
		default:
			if c < 32 || c >= 127 {
				fmt.Fprintf(&b, `\%03o`, c)
				quoted = true
			} else {
				b.WriteByte(c)
			}
		}
	}
	if quoted {
		return `"` + b.String() + `"`
	}
	return p
}
func shortOID(oid string) string {
	if oid == "" {
		return "0000000"
	}
	return oid[:min(7, len(oid))]
}

func (s *Snapshot) diffFile(ctx context.Context, to *Snapshot, p string, a, b Entry, opts DiffOptions, w io.Writer) error {
	if a.OID == b.OID && a.Mode == b.Mode {
		return nil
	}
	if len(opts.Paths) > 0 {
		matched := false
		for _, filter := range opts.Paths {
			if filter == "." || p == filter || strings.HasPrefix(p, filter+"/") {
				matched = true
				break
			}
		}
		if !matched {
			return nil
		}
	}
	status := "M"
	if a.OID == "" {
		status = "A"
	} else if b.OID == "" {
		status = "D"
	} else if a.Mode&0170000 != b.Mode&0170000 {
		status = "T"
	}
	if opts.NameOnly {
		_, err := fmt.Fprintln(w, quoteDiffPath(p))
		return err
	}
	if opts.NameStatus {
		_, err := fmt.Fprintf(w, "%s\t%s\n", status, quoteDiffPath(p))
		return err
	}
	if status == "T" {
		if err := s.diffFile(ctx, to, p, a, Entry{}, opts, w); err != nil {
			return err
		}
		return s.diffFile(ctx, to, p, Entry{}, b, opts, w)
	}
	var header strings.Builder
	ap, bp := quoteDiffPath("a/"+p), quoteDiffPath("b/"+p)
	fmt.Fprintf(&header, "diff --git %s %s\n", ap, bp)
	switch {
	case a.OID == "":
		fmt.Fprintf(&header, "new file mode %06o\n", b.Mode)
	case b.OID == "":
		fmt.Fprintf(&header, "deleted file mode %06o\n", a.Mode)
	case a.Mode != b.Mode:
		fmt.Fprintf(&header, "old mode %06o\nnew mode %06o\n", a.Mode, b.Mode)
	}
	if a.OID == b.OID {
		_, err := io.WriteString(w, header.String())
		return err
	}
	ab, err := s.historyBlob(ctx, a)
	if err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	bb, err := to.historyBlob(ctx, b)
	if err != nil {
		return fmt.Errorf("%s: %w", p, err)
	}
	fmt.Fprintf(&header, "index %s..%s", shortOID(a.OID), shortOID(b.OID))
	if a.Mode == b.Mode {
		fmt.Fprintf(&header, " %06o", a.Mode)
	}
	header.WriteByte('\n')
	if a.OID == "" {
		ap = "/dev/null"
	}
	if b.OID == "" {
		bp = "/dev/null"
	}
	if bytes.IndexByte(ab, 0) >= 0 || bytes.IndexByte(bb, 0) >= 0 {
		fmt.Fprintf(&header, "Binary files %s and %s differ\n", ap, bp)
		_, err = io.WriteString(w, header.String())
		return err
	}
	al, err := textLines(ab)
	if err != nil {
		return err
	}
	bl, err := textLines(bb)
	if err != nil {
		return err
	}
	edits, err := patchLineDiff(ctx, al, bl)
	if err != nil {
		return err
	}
	if len(ab) > 0 || len(bb) > 0 {
		fmt.Fprintf(&header, "--- %s\n+++ %s\n", patchPath(ap), patchPath(bp))
	}
	if _, err = io.WriteString(w, header.String()); err != nil {
		return err
	}
	for i := 0; i < len(edits); {
		for i < len(edits) && edits[i].kind == ' ' {
			i++
		}
		if i == len(edits) {
			break
		}
		start := max(0, i-opts.Context)
		last := i
		for j := i + 1; j < len(edits); j++ {
			if edits[j].kind != ' ' {
				if j-last > 2*opts.Context+1 {
					break
				}
				last = j
			}
		}
		end := min(len(edits), last+opts.Context+1)
		ac, bc := 0, 0
		for _, e := range edits[start:end] {
			if e.kind != '+' {
				ac++
			}
			if e.kind != '-' {
				bc++
			}
		}
		ar, br := edits[start].a+1, edits[start].b+1
		if ac == 0 {
			ar--
		}
		if bc == 0 {
			br--
		}
		if _, err = fmt.Fprintf(w, "@@ -%s +%s @@%s\n", hunkRange(ar, ac), hunkRange(br, bc), hunkLabel(al, edits[start].a)); err != nil {
			return err
		}
		for _, e := range edits[start:end] {
			line := ""
			if e.kind == '+' {
				line = bl[e.b]
			} else {
				line = al[e.a]
			}
			if _, err = fmt.Fprintf(w, "%c%s", e.kind, line); err != nil {
				return err
			}
			if !strings.HasSuffix(line, "\n") {
				if _, err = io.WriteString(w, "\n\\ No newline at end of file\n"); err != nil {
					return err
				}
			}
		}
		i = end
	}
	return nil
}
func hunkRange(start, count int) string {
	if count == 1 {
		return strconv.Itoa(start)
	}
	return fmt.Sprintf("%d,%d", start, count)
}

func patchPath(p string) string {
	if !strings.HasPrefix(p, `"`) && strings.Contains(p, " ") {
		return p + "\t"
	}
	return p
}

// Git's default hunk heading is the closest preceding identifier-like line.
func hunkLabel(lines []string, before int) string {
	for i := min(before, len(lines)) - 1; i >= 0; i-- {
		line := lines[i]
		if line == "" {
			continue
		}
		c := line[0]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c == '$' {
			return " " + strings.TrimRight(line[:min(80, len(line))], " \t\r\n\v\f")
		}
	}
	return ""
}
