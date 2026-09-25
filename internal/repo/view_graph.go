package repo

import (
	"container/heap"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"gat/internal/store"
)

// Graph queries keep ancestry membership in bitsets keyed by the immutable
// history positions. Eight MiB covers 67 million positions, independent of the
// payload cache. Older stores use a bounded SHA set until reimported.
const graphSetBudget = 8 << 20
const graphFrontierLimit = 65536

type viewGraphID struct {
	pos uint64
	sha string
}
type viewGraphSet struct {
	bits   []uint64
	legacy map[string]bool
}

func (s *viewGraphSet) has(id viewGraphID) bool {
	if id.pos != 0 {
		i := id.pos / 64
		return i < uint64(len(s.bits)) && s.bits[i]&(1<<(id.pos%64)) != 0
	}
	return s.legacy[id.sha]
}
func (s *viewGraphSet) add(id viewGraphID) error {
	if id.pos != 0 {
		i := id.pos / 64
		if i >= graphSetBudget/8 {
			return fmt.Errorf("history membership exceeds 8 MiB query budget")
		}
		if i >= uint64(len(s.bits)) {
			s.bits = append(s.bits, make([]uint64, int(i)+1-len(s.bits))...)
		}
		s.bits[i] |= 1 << (id.pos % 64)
		return nil
	}
	if s.legacy == nil {
		s.legacy = map[string]bool{}
	}
	if !s.legacy[id.sha] && len(s.legacy) >= 100000 {
		return fmt.Errorf("legacy history query budget exceeded; reimport to build compact history index")
	}
	s.legacy[id.sha] = true
	return nil
}
func (s *viewGraphSet) remove(id viewGraphID) {
	if id.pos != 0 {
		i := id.pos / 64
		if i < uint64(len(s.bits)) {
			s.bits[i] &^= 1 << (id.pos % 64)
		}
	} else {
		delete(s.legacy, id.sha)
	}
}
func (s *viewGraphSet) each(fn func(viewGraphID) error) error {
	for i, word := range s.bits {
		for bit := uint64(0); word != 0; bit++ {
			if word&(1<<bit) != 0 {
				if err := fn(viewGraphID{pos: uint64(i)*64 + bit}); err != nil {
					return err
				}
				word &^= 1 << bit
			}
		}
	}
	for sha := range s.legacy {
		if err := fn(viewGraphID{sha: sha}); err != nil {
			return err
		}
	}
	return nil
}

type viewGraph struct {
	snapshot *Snapshot
	cursor   historyCursor
}

func newViewGraph(s *Snapshot) *viewGraph {
	return &viewGraph{snapshot: s, cursor: historyCursor{idx: s.history}}
}
func (g *viewGraph) id(ctx context.Context, sha string) (viewGraphID, error) {
	if g.snapshot.history != nil && g.snapshot.history.root != (pageRef{}) {
		var p historyPosition
		if err := g.snapshot.history.get(ctx, "g/"+sha, &p); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return viewGraphID{sha: sha}, nil
			}
			return viewGraphID{}, err
		}
		return viewGraphID{pos: uint64(p), sha: sha}, nil
	}
	return viewGraphID{sha: sha}, nil
}
func (g *viewGraph) node(ctx context.Context, id viewGraphID) (string, []viewGraphID, error) {
	if err := ctx.Err(); err != nil {
		return "", nil, err
	}
	if id.pos != 0 {
		n, err := g.cursor.get(ctx, id.pos)
		if err != nil {
			return "", nil, err
		}
		ps := make([]viewGraphID, len(n.Parents))
		for i, p := range n.Parents {
			ps[i] = viewGraphID{pos: p}
		}
		return hex.EncodeToString(n.Oid), ps, nil
	}
	var p parents
	if err := g.snapshot.idx.get(ctx, "p/"+id.sha, &p); err != nil {
		return "", nil, err
	}
	ps := make([]viewGraphID, len(p.Parents))
	for i, sha := range p.Parents {
		// A locally retained commit can join reachable history. Normalize that
		// boundary so roots, exclusions, and visited sets use the same ID.
		var err error
		ps[i], err = g.id(ctx, sha)
		if err != nil {
			return "", nil, err
		}
	}
	return id.sha, ps, nil
}
func (g *viewGraph) ancestors(ctx context.Context, roots []viewGraphID, first bool) (*viewGraphSet, error) {
	seen := &viewGraphSet{}
	stack := append([]viewGraphID(nil), roots...)
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen.has(id) {
			continue
		}
		if err := seen.add(id); err != nil {
			return nil, err
		}
		_, ps, err := g.node(ctx, id)
		if err != nil {
			return nil, err
		}
		if first && len(ps) > 1 {
			ps = ps[:1]
		}
		if len(stack)+len(ps) > graphFrontierLimit {
			return nil, fmt.Errorf("history frontier exceeds bounded query budget")
		}
		stack = append(stack, ps...)
	}
	return seen, nil
}

type viewGraphCandidate struct {
	id    viewGraphID
	sha   string
	time  int64
	order int
	info  commitInfo
}
type viewGraphQueue []viewGraphCandidate

func (q viewGraphQueue) Len() int { return len(q) }
func (q viewGraphQueue) Less(i, j int) bool {
	if q[i].time == q[j].time {
		return q[i].order < q[j].order
	}
	return q[i].time > q[j].time
}
func (q viewGraphQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *viewGraphQueue) Push(v any)   { *q = append(*q, v.(viewGraphCandidate)) }
func (q *viewGraphQueue) Pop() any {
	i := len(*q) - 1
	v := (*q)[i]
	(*q)[i] = viewGraphCandidate{}
	*q = (*q)[:i]
	return v
}
func (g *viewGraph) walk(ctx context.Context, roots []viewGraphID, exclude *viewGraphSet, first bool, limit int, emit func(string, []string, commitInfo) error) error {
	seen := &viewGraphSet{}
	q := &viewGraphQueue{}
	order := 0
	retained := 0
	add := func(id viewGraphID) error {
		if seen.has(id) || exclude.has(id) {
			return nil
		}
		if len(*q) >= graphFrontierLimit {
			return fmt.Errorf("history frontier exceeds bounded query budget")
		}
		sha := id.sha
		if sha == "" {
			var err error
			sha, _, err = g.node(ctx, id)
			if err != nil {
				return err
			}
		}
		var info commitInfo
		if err := g.snapshot.idx.get(ctx, "c/"+sha, &info); err != nil {
			return err
		}
		if err := seen.add(id); err != nil {
			return err
		}
		retained += len(info.Author) + len(info.Message) + 128
		if retained > 2<<20 {
			return fmt.Errorf("commit frontier metadata exceeds 2 MiB query budget")
		}
		heap.Push(q, viewGraphCandidate{id: id, sha: sha, time: info.CommitTime, order: order, info: info})
		order++
		return nil
	}
	for _, id := range roots {
		if err := add(id); err != nil {
			return err
		}
	}
	for n := 0; q.Len() > 0 && (limit < 0 || n < limit); n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		v := heap.Pop(q).(viewGraphCandidate)
		retained -= len(v.info.Author) + len(v.info.Message) + 128
		_, ps, err := g.node(ctx, v.id)
		if err != nil {
			return err
		}
		parents := make([]string, len(ps))
		for i, p := range ps {
			if p.sha != "" {
				parents[i] = p.sha
			} else {
				parents[i], _, err = g.node(ctx, p)
				if err != nil {
					return err
				}
			}
		}
		if err := emit(v.sha, parents, v.info); err != nil {
			return err
		}
		if limit >= 0 && n+1 >= limit {
			break
		}
		if first && len(ps) > 1 {
			ps = ps[:1]
		}
		for _, p := range ps {
			if err := add(p); err != nil {
				return err
			}
		}
	}
	return nil
}

// ViewGraph implements read-only commit graph commands against a single
// publication. HEAD remains the calling mount's checkout.
func (r *Repository) ViewGraph(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	switch opt.Command {
	case "rev-list":
		return r.viewRevList(ctx, current, opt, out)
	case "merge-base":
		return r.viewMergeBase(ctx, current, opt, out)
	case "shortlog":
		return r.viewShortlog(ctx, current, opt, out)
	default:
		return fmt.Errorf("unsupported graph command %q", opt.Command)
	}
}
func graphFlags(name string) *flag.FlagSet {
	f := flag.NewFlagSet(name, flag.ContinueOnError)
	f.SetOutput(io.Discard)
	return f
}
func graphArgs(args []string) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		if len(arg) > 1 && arg[0] == '-' && arg[1] >= '0' && arg[1] <= '9' {
			if _, err := strconv.Atoi(arg[1:]); err == nil {
				out = append(out, "--max-count="+arg[1:])
				continue
			}
		}
		out = append(out, arg)
	}
	return out
}
func (r *Repository) graphRange(ctx context.Context, current *Snapshot, args []string) (*viewGraph, []viewGraphID, *viewGraphSet, error) {
	revisions := []string{}
	negative := []bool{}
	for _, a := range args {
		if a == "--" {
			return nil, nil, nil, fmt.Errorf("path filtering is not supported by this command")
		}
		if strings.Contains(a, "...") {
			return nil, nil, nil, fmt.Errorf("symmetric ranges are not supported")
		}
		if left, right, ok := strings.Cut(a, ".."); ok {
			if left == "" {
				left = "HEAD"
			}
			if right == "" {
				right = "HEAD"
			}
			revisions = append(revisions, left, right)
			negative = append(negative, true, false)
		} else {
			neg := strings.HasPrefix(a, "^")
			a = strings.TrimPrefix(a, "^")
			revisions = append(revisions, a)
			negative = append(negative, neg)
		}
	}
	if len(revisions) == 0 {
		revisions = []string{"HEAD"}
		negative = []bool{false}
	}
	head := ""
	if current != nil {
		head = current.SHA
	}
	snapshots, err := r.OpenRevisions(ctx, revisions, head)
	if err != nil {
		return nil, nil, nil, err
	}
	g := newViewGraph(snapshots[0])
	roots := []viewGraphID{}
	excluded := []viewGraphID{}
	for i, s := range snapshots {
		id, err := g.id(ctx, s.SHA)
		if err != nil {
			return nil, nil, nil, err
		}
		if negative[i] {
			excluded = append(excluded, id)
		} else {
			roots = append(roots, id)
		}
	}
	set, err := g.ancestors(ctx, excluded, false)
	return g, roots, set, err
}
func (r *Repository) viewRevList(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	for i, arg := range opt.Args {
		if arg == "--" && i+1 < len(opt.Args) {
			return fmt.Errorf("rev-list path filtering is not supported")
		}
	}
	f := graphFlags(opt.Command)
	limit := -1
	count, first, parents, reverse := false, false, false, false
	f.IntVar(&limit, "max-count", -1, "")
	f.IntVar(&limit, "n", -1, "")
	f.BoolVar(&count, "count", false, "")
	f.BoolVar(&first, "first-parent", false, "")
	f.BoolVar(&parents, "parents", false, "")
	f.BoolVar(&reverse, "reverse", false, "")
	if err := parseViewFlags(f, graphArgs(opt.Args)); err != nil {
		return err
	}
	if limit < -1 {
		return fmt.Errorf("max-count must be nonnegative")
	}
	g, roots, excluded, err := r.graphRange(ctx, current, f.Args())
	if err != nil {
		return err
	}
	if count {
		reached, err := g.ancestors(ctx, roots, first)
		if err != nil {
			return err
		}
		n := 0
		if err := reached.each(func(id viewGraphID) error {
			if !excluded.has(id) {
				n++
			}
			return nil
		}); err != nil {
			return err
		}
		if limit >= 0 && n > limit {
			n = limit
		}
		_, err = fmt.Fprintln(out, n)
		return err
	}
	n := 0
	var lines []string
	bytes := 0
	err = g.walk(ctx, roots, excluded, first, limit, func(sha string, ps []string, _ commitInfo) error {
		n++
		if count {
			return nil
		}
		line := sha
		if parents && len(ps) > 0 {
			line += " " + strings.Join(ps, " ")
		}
		if reverse {
			bytes += len(line)
			if bytes > 16<<20 {
				return fmt.Errorf("reverse output exceeds 16 MiB query budget")
			}
			lines = append(lines, line)
			return nil
		}
		_, err := fmt.Fprintln(out, line)
		return err
	})
	if err != nil {
		return err
	}
	if count {
		_, err = fmt.Fprintln(out, n)
	} else if reverse {
		for i := len(lines) - 1; i >= 0; i-- {
			if _, err = fmt.Fprintln(out, lines[i]); err != nil {
				break
			}
		}
	}
	return err
}
func (r *Repository) viewMergeBase(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	f := graphFlags(opt.Command)
	all, ancestor := false, false
	f.BoolVar(&all, "all", false, "")
	f.BoolVar(&all, "a", false, "")
	f.BoolVar(&ancestor, "is-ancestor", false, "")
	if err := parseViewFlags(f, opt.Args); err != nil {
		return err
	}
	if len(f.Args()) != 2 {
		return fmt.Errorf("merge-base requires two revisions")
	}
	if all && ancestor {
		return fmt.Errorf("--all and --is-ancestor cannot be combined")
	}
	head := ""
	if current != nil {
		head = current.SHA
	}
	ss, err := r.OpenRevisions(ctx, f.Args(), head)
	if err != nil {
		return err
	}
	g := newViewGraph(ss[0])
	a, err := g.id(ctx, ss[0].SHA)
	if err != nil {
		return err
	}
	b, err := g.id(ctx, ss[1].SHA)
	if err != nil {
		return err
	}
	right, err := g.ancestors(ctx, []viewGraphID{b}, false)
	if err != nil {
		return err
	}
	if ancestor {
		if right.has(a) {
			return nil
		}
		return graphNegativeResult()
	}
	left, err := g.ancestors(ctx, []viewGraphID{a}, false)
	if err != nil {
		return err
	}
	common := &viewGraphSet{}
	if err := left.each(func(id viewGraphID) error {
		if right.has(id) {
			return common.add(id)
		}
		return nil
	}); err != nil {
		return err
	}
	// Every parent of a common ancestor is common too; removing those parents
	// leaves precisely the lowest common ancestors, including criss-cross merges.
	best := &viewGraphSet{}
	if err := common.each(best.add); err != nil {
		return err
	}
	if err := common.each(func(id viewGraphID) error {
		_, ps, err := g.node(ctx, id)
		if err != nil {
			return err
		}
		for _, p := range ps {
			best.remove(p)
		}
		return nil
	}); err != nil {
		return err
	}
	type base struct {
		sha  string
		time int64
	}
	var bases []base
	if err := best.each(func(id viewGraphID) error {
		sha, _, err := g.node(ctx, id)
		if err != nil {
			return err
		}
		var info commitInfo
		if err := g.snapshot.idx.get(ctx, "c/"+sha, &info); err != nil {
			return err
		}
		bases = append(bases, base{sha, info.CommitTime})
		return nil
	}); err != nil {
		return err
	}
	if len(bases) == 0 {
		return graphNegativeResult()
	}
	sort.Slice(bases, func(i, j int) bool {
		if bases[i].time != bases[j].time {
			return bases[i].time > bases[j].time
		}
		return bases[i].sha < bases[j].sha
	})
	if !all {
		bases = bases[:1]
	}
	for _, b := range bases {
		if _, err := fmt.Fprintln(out, b.sha); err != nil {
			return err
		}
	}
	return nil
}

func graphNegativeResult() error { return ErrViewNoMatch }

type graphAuthor struct{ name, email string }

func graphIdentity(raw string) graphAuthor {
	open := strings.LastIndex(raw, " <")
	if open < 0 || !strings.HasSuffix(raw, ">") {
		return graphAuthor{name: raw}
	}
	return graphAuthor{name: raw[:open], email: raw[open+2 : len(raw)-1]}
}

type graphMailmapRule struct{ old, new graphAuthor }

func graphMailmap(ctx context.Context, s *Snapshot) ([]graphMailmapRule, error) {
	if s == nil {
		return nil, nil
	}
	e, err := s.Resolve(ctx, ".mailmap")
	if IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if e.Mode != 0100644 && e.Mode != 0100755 {
		return nil, nil
	}
	if e.Size > 1<<20 {
		return nil, fmt.Errorf(".mailmap exceeds 1 MiB query budget")
	}
	data := make([]byte, e.Size)
	n, err := s.ReadAt(ctx, e.OID, data, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	var rules []graphMailmapRule
	for _, line := range strings.Split(string(data[:n]), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		end := strings.IndexByte(line, '>')
		start := strings.IndexByte(line, '<')
		if start < 0 || end < start {
			continue
		}
		canonical := graphAuthor{name: strings.TrimSpace(line[:start]), email: line[start+1 : end]}
		rest := strings.TrimSpace(line[end+1:])
		if strings.Contains(rest, "<") {
			old := graphIdentity(" " + rest)
			old.name = strings.TrimSpace(old.name)
			if old.email == "" {
				continue
			}
			rules = append(rules, graphMailmapRule{old: old, new: canonical})
		} else {
			rules = append(rules, graphMailmapRule{old: graphAuthor{email: canonical.email}, new: graphAuthor{name: canonical.name}})
		}
	}
	return rules, nil
}
func graphMappedAuthor(a graphAuthor, rules []graphMailmapRule) graphAuthor {
	original := a
	var specific *graphMailmapRule
	for i := range rules {
		rule := &rules[i]
		if !strings.EqualFold(rule.old.email, original.email) {
			continue
		}
		if rule.old.name != "" {
			if strings.EqualFold(rule.old.name, original.name) {
				specific = rule
			}
			continue
		}
		if rule.new.name != "" {
			a.name = rule.new.name
		}
		if rule.new.email != "" {
			a.email = rule.new.email
		}
	}
	if specific != nil {
		a = original
		if specific.new.name != "" {
			a.name = specific.new.name
		}
		if specific.new.email != "" {
			a.email = specific.new.email
		}
	}
	return a
}
func (r *Repository) viewShortlog(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	for i, arg := range opt.Args {
		if arg == "--" && i+1 < len(opt.Args) {
			return fmt.Errorf("shortlog path filtering is not supported")
		}
	}
	f := graphFlags(opt.Command)
	summary, numeric, email, first, noMerges := false, false, false, false, false
	limit := -1
	f.BoolVar(&summary, "summary", false, "")
	f.BoolVar(&summary, "s", false, "")
	f.BoolVar(&numeric, "numbered", false, "")
	f.BoolVar(&numeric, "n", false, "")
	f.BoolVar(&email, "email", false, "")
	f.BoolVar(&email, "e", false, "")
	f.BoolVar(&first, "first-parent", false, "")
	f.BoolVar(&noMerges, "no-merges", false, "")
	f.IntVar(&limit, "max-count", -1, "")
	args := []string{}
	for _, a := range opt.Args {
		if len(a) > 2 && a[0] == '-' && strings.Trim(a[1:], "sne") == "" {
			for _, c := range a[1:] {
				args = append(args, "-"+string(c))
			}
		} else {
			args = append(args, a)
		}
	}
	if err := parseViewFlags(f, graphArgs(args)); err != nil {
		return err
	}
	if limit < -1 {
		return fmt.Errorf("max-count must be nonnegative")
	}
	g, roots, excluded, err := r.graphRange(ctx, current, f.Args())
	if err != nil {
		return err
	}
	mailmapSource := current
	if mailmapSource == nil {
		mailmapSource = g.snapshot
	}
	rules, err := graphMailmap(ctx, mailmapSource)
	if err != nil {
		return err
	}
	type group struct {
		name     string
		count    int
		subjects []string
	}
	groups := map[string]*group{}
	retained := 0
	if limit == 0 {
		return nil
	}
	displayed := 0
	stop := errors.New("shortlog count satisfied")
	err = g.walk(ctx, roots, excluded, first, -1, func(_ string, ps []string, info commitInfo) error {
		if noMerges && len(ps) > 1 {
			return nil
		}
		if info.AuthorTruncated || (!summary && info.MessageTruncated) {
			return fmt.Errorf("shortlog requires complete commit display metadata")
		}
		a := graphMappedAuthor(graphIdentity(string(info.Author)), rules)
		name := a.name
		if email {
			name += " <" + a.email + ">"
		}
		item := groups[name]
		if item == nil {
			retained += len(name) + 64
			if retained > 16<<20 {
				return fmt.Errorf("shortlog groups exceed 16 MiB query budget")
			}
			item = &group{name: name}
			groups[name] = item
		}
		item.count++
		if !summary {
			subject := strings.TrimLeft(string(info.Message), " \t\r\n")
			subject = strings.SplitN(subject, "\n\n", 2)[0]
			subject = strings.Join(strings.Fields(subject), " ")
			if strings.HasPrefix(subject, "[PATCH") {
				if end := strings.IndexByte(subject, ']'); end >= 0 {
					subject = strings.TrimSpace(subject[end+1:])
				}
			}
			if subject == "" {
				subject = "<none>"
			}
			retained += len(subject) + 16
			if retained > 16<<20 {
				return fmt.Errorf("shortlog subjects exceed 16 MiB query budget; use --summary")
			}
			item.subjects = append(item.subjects, subject)
		}
		displayed++
		if limit >= 0 && displayed >= limit {
			return stop
		}
		return nil
	})
	if errors.Is(err, stop) {
		err = nil
	}
	if err != nil {
		return err
	}
	ordered := make([]*group, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if numeric && ordered[i].count != ordered[j].count {
			return ordered[i].count > ordered[j].count
		}
		return ordered[i].name < ordered[j].name
	})
	for _, g := range ordered {
		if summary {
			if _, err := fmt.Fprintf(out, "%6d\t%s\n", g.count, g.name); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(out, "%s (%d):\n", g.name, g.count); err != nil {
			return err
		}
		for i := len(g.subjects) - 1; i >= 0; i-- {
			if err := graphShortlogSubject(out, g.subjects[i]); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(out); err != nil {
			return err
		}
	}
	return nil
}
func graphShortlogSubject(out io.Writer, subject string) error {
	line := "      "
	for _, word := range strings.Fields(subject) {
		if len(line) > 6 && len(line)+1+len(word) > 76 {
			if _, err := fmt.Fprintln(out, line); err != nil {
				return err
			}
			line = "      "
		}
		if len(line) > 6 {
			line += " "
		}
		line += word
	}
	_, err := fmt.Fprintln(out, line)
	return err
}
