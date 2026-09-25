// Package pathspec implements repository-relative Git path selection.
package pathspec

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

type Requirement struct {
	Name, Value string
	State       byte
}
type Pattern struct {
	Text                               string
	Top, Literal, Glob, ICase, Exclude bool
	directory                          bool
	Attrs                              []Requirement
	re                                 *regexp.Regexp
	prefix                             string
	wildcard                           bool
}
type Matcher struct {
	Patterns []Pattern
	positive int
}

func Parse(raw string) (Pattern, error) {
	var p Pattern
	if raw == "" {
		return p, fmt.Errorf("empty pathspec")
	}
	if len(raw) > 4096 || strings.ContainsRune(raw, 0) {
		return p, fmt.Errorf("pathspec exceeds 4096 bytes or contains NUL")
	}
	p.Text = raw
	if strings.HasPrefix(raw, ":(") {
		end := strings.IndexByte(raw, ')')
		if end < 0 {
			return p, fmt.Errorf("unterminated pathspec magic")
		}
		p.Text = raw[end+1:]
		for _, magic := range strings.Split(raw[2:end], ",") {
			switch magic {
			case "top":
				p.Top = true
			case "literal":
				p.Literal = true
			case "glob":
				p.Glob = true
			case "icase":
				p.ICase = true
			case "exclude":
				p.Exclude = true
			case "":
			default:
				if !strings.HasPrefix(magic, "attr:") {
					return p, fmt.Errorf("unknown pathspec magic %q", magic)
				}
				for _, a := range strings.Fields(strings.TrimPrefix(magic, "attr:")) {
					r := Requirement{State: '+'}
					switch a[0] {
					case '-', '!':
						r.State, a = a[0], a[1:]
					}
					r.Name = a
					if name, value, ok := strings.Cut(a, "="); ok {
						r.Name, r.Value, r.State = name, value, '='
					}
					if r.Name == "" || strings.ContainsAny(r.Name, "! =\t\r\n") {
						return p, fmt.Errorf("invalid attribute requirement")
					}
					p.Attrs = append(p.Attrs, r)
				}
				if len(p.Attrs) == 0 {
					return p, fmt.Errorf("empty attribute requirement")
				}
			}
		}
	} else if strings.HasPrefix(raw, ":") && raw != ":" {
		i := 1
		for i < len(raw) {
			switch raw[i] {
			case '/':
				p.Top = true
			case '!', '^':
				p.Exclude = true
			case ':':
				i++
				p.Text = raw[i:]
				goto parsed
			default:
				p.Text = raw[i:]
				goto parsed
			}
			i++
		}
		p.Text = raw[i:]
	}
parsed:
	p.directory = strings.HasSuffix(p.Text, "/")
	if p.Glob && p.Literal {
		return p, fmt.Errorf("glob and literal pathspec magic are incompatible")
	}
	return p, nil
}

func Compile(raw []string, prefix string) (*Matcher, error) {
	if len(raw) > 128 {
		return nil, fmt.Errorf("at most 128 pathspecs are supported")
	}
	if prefix == "." {
		prefix = ""
	}
	if prefix != "" && (path.Clean(prefix) != prefix || strings.HasPrefix(prefix, "/") || prefix == ".." || strings.HasPrefix(prefix, "../")) {
		return nil, fmt.Errorf("invalid pathspec working-directory prefix")
	}
	m := &Matcher{}
	for _, text := range raw {
		if text == ":" {
			if len(raw) != 1 {
				return nil, fmt.Errorf("a lone ':' pathspec cannot be combined with other paths")
			}
			return &Matcher{}, nil
		}
		p, err := Parse(text)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(p.Text, "/") {
			return nil, fmt.Errorf("pathspec must be repository-relative")
		}
		if p.Top {
			p.Text = path.Clean(p.Text)
		} else {
			p.Text = path.Join(prefix, p.Text)
		}
		if p.Text == ".." || strings.HasPrefix(p.Text, "../") {
			return nil, fmt.Errorf("pathspec is outside the mounted repository")
		}
		if p.Text == "." {
			p.Text = ""
		}
		p.wildcard = !p.Literal && strings.ContainsAny(p.Text, "*?[\\")
		p.prefix = p.Text
		if p.wildcard {
			i := strings.IndexAny(p.Text, "*?[\\")
			p.prefix = p.Text[:i]
			p.re, err = Wildmatch(p.Text, p.Glob, p.ICase)
			if err != nil {
				return nil, err
			}
		}
		if !p.Exclude {
			m.positive++
		}
		m.Patterns = append(m.Patterns, p)
	}
	return m, nil
}

func fold(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// Latin-1 mapping gives regexp byte-oriented behavior even for non-UTF-8 Git names.
func Bytes(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		b.WriteRune(rune(s[i]))
	}
	return b.String()
}
func (p Pattern) Match(name string) bool {
	pattern := p.Text
	if p.ICase {
		name, pattern = fold(name), fold(pattern)
	}
	if p.directory {
		if pattern == "" || strings.HasPrefix(name, pattern+"/") {
			return true
		}
		if p.re != nil {
			for i := 0; i < len(name); i++ {
				if name[i] == '/' && p.re.MatchString(Bytes(name[:i])) {
					return true
				}
			}
		}
		return false
	}
	if pattern == "" || name == pattern || strings.HasPrefix(name, pattern+"/") {
		return true
	}
	return p.re != nil && p.re.MatchString(Bytes(name))
}
func (m *Matcher) Match(name string, attrs func(string, []Requirement) (bool, error)) (bool, error) {
	matched := m.positive == 0
	for _, p := range m.Patterns {
		if p.Exclude || !p.Match(name) {
			continue
		}
		ok, err := matchAttrs(name, p.Attrs, attrs)
		if err != nil {
			return false, err
		}
		if ok {
			matched = true
			break
		}
	}
	if !matched {
		return false, nil
	}
	for _, p := range m.Patterns {
		if !p.Exclude || !p.Match(name) {
			continue
		}
		ok, err := matchAttrs(name, p.Attrs, attrs)
		if err != nil {
			return false, err
		}
		if ok {
			return false, nil
		}
	}
	return true, nil
}
func matchAttrs(name string, req []Requirement, attrs func(string, []Requirement) (bool, error)) (bool, error) {
	if len(req) == 0 {
		return true, nil
	}
	if attrs == nil {
		return false, fmt.Errorf("attribute matching unavailable")
	}
	return attrs(name, req)
}

// MayDescend is conservative: it never prunes a directory containing a match.
func (m *Matcher) MayDescend(dir string) bool {
	if m.positive == 0 {
		return true
	}
	for _, p := range m.Patterns {
		if p.Exclude {
			continue
		}
		pre, d := p.prefix, dir
		if p.ICase {
			pre, d = fold(pre), fold(d)
		}
		if pre == "" || d == "" || strings.HasPrefix(pre, d+"/") || strings.HasPrefix(d, pre) || pre == d {
			return true
		}
	}
	return false
}
func (m *Matcher) LiteralPaths() []string {
	if len(m.Patterns) == 0 {
		return nil
	}
	var out []string
	for _, p := range m.Patterns {
		if p.wildcard || p.ICase || p.Exclude || p.directory || len(p.Attrs) > 0 {
			return nil
		}
		name := p.Text
		if name == "" {
			name = "."
		}
		out = append(out, name)
	}
	return out
}
func (m *Matcher) FollowPath() (string, error) {
	if len(m.Patterns) != 1 {
		return "", fmt.Errorf("--follow requires exactly one file")
	}
	p := m.Patterns[0]
	if p.Glob || p.ICase || p.Exclude || len(p.Attrs) > 0 || p.wildcard {
		return "", fmt.Errorf("--follow requires a literal file path; top and literal magic are supported")
	}
	if p.Text == "" {
		return "", fmt.Errorf("--follow requires a file")
	}
	return p.Text, nil
}

// Wildmatch compiles Git's byte-oriented shell patterns. pathname selects glob
// magic: '*' and '?' cannot cross '/', while '**/' spans zero or more levels.
func Wildmatch(pattern string, pathname, icase bool) (*regexp.Regexp, error) {
	if icase {
		pattern = fold(pattern)
	}
	var out strings.Builder
	out.WriteString("(?s)^")
	for i := 0; i < len(pattern); i++ {
		c := pattern[i]
		switch c {
		case '\\':
			if i+1 < len(pattern) {
				i++
				out.WriteString(regexp.QuoteMeta(Bytes(pattern[i : i+1])))
			} else {
				out.WriteString(`\\`)
			}
		case '*':
			end := i
			for end+1 < len(pattern) && pattern[end+1] == '*' {
				end++
			}
			doublestar := pathname && end > i && (i == 0 || pattern[i-1] == '/') && (end+1 == len(pattern) || pattern[end+1] == '/')
			if doublestar && end+1 < len(pattern) {
				out.WriteString("(?:.*/)?")
				end++
			} else if !pathname || doublestar {
				out.WriteString(".*")
			} else {
				out.WriteString("[^/]*")
			}
			i = end
		case '?':
			if pathname {
				out.WriteString("[^/]")
			} else {
				out.WriteByte('.')
			}
		case '[':
			j := i + 1
			if j < len(pattern) && (pattern[j] == '!' || pattern[j] == '^') {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++
			}
			for j < len(pattern) {
				if pattern[j] == '[' && j+1 < len(pattern) && pattern[j+1] == ':' {
					k := strings.Index(pattern[j+2:], ":]")
					if k >= 0 {
						j += k + 4
						continue
					}
				}
				if pattern[j] == ']' {
					break
				}
				j++
			}
			if j == len(pattern) {
				out.WriteString(`\[`)
				continue
			}
			body := pattern[i+1 : j]
			if strings.HasPrefix(body, "!") {
				body = "^" + body[1:]
			}
			class, err := byteClass(body, pathname)
			if err != nil {
				return nil, err
			}
			out.WriteString(class)
			i = j
		default:
			out.WriteString(regexp.QuoteMeta(Bytes(pattern[i : i+1])))
		}
	}
	out.WriteByte('$')
	re, err := regexp.Compile(out.String())
	if err != nil {
		return nil, fmt.Errorf("invalid pathspec pattern %q: %w", pattern, err)
	}
	return re, nil
}

// A glob bracket cannot match a slash, including through a range such as [.-0].
func byteClass(body string, pathname bool) (string, error) {
	re, err := regexp.Compile("^[" + Bytes(body) + "]$")
	if err != nil {
		return "", err
	}
	var out strings.Builder
	out.WriteByte('[')
	any := false
	for i := 0; i < 256; i++ {
		allowed := func(n int) bool { return (!pathname || byte(n) != '/') && re.MatchString(string(rune(n))) }
		if !allowed(i) {
			continue
		}
		start := i
		for i+1 < 256 && allowed(i+1) {
			i++
		}
		fmt.Fprintf(&out, `\x{%x}`, start)
		if i > start {
			fmt.Fprintf(&out, `-\x{%x}`, i)
		}
		any = true
	}
	if !any {
		return `[^\x{0}-\x{ff}]`, nil
	}
	out.WriteByte(']')
	return out.String(), nil
}

// LooksLike recognizes arguments Git accepts as pathspecs without a live match.
func LooksLike(raw string) bool {
	if strings.HasPrefix(raw, ":(") {
		return true
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' {
			i++
			continue
		}
		if strings.ContainsRune("*?[", rune(raw[i])) {
			return true
		}
	}
	return false
}
