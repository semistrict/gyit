package repo

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"

	"gyit/internal/pathspec"
	"gyit/internal/store"
)

type attributeValue struct {
	state byte
	value string
}

// Attribute pathspecs use the mounted checkout's tracked .gitattributes, as Git
// uses the working tree rather than the historical tree being compared.
func (s *Snapshot) matchAttributes(ctx context.Context, name string, requirements []pathspec.Requirement) (bool, error) {
	values := map[string]attributeValue{}
	macros := map[string][]string{"binary": {"-diff", "-merge", "-text"}}
	var apply func(string, int) error
	apply = func(token string, depth int) error {
		if depth > 32 {
			return fmt.Errorf("attribute macro recursion exceeds 32")
		}
		state := byte('+')
		value := ""
		key := token
		if strings.HasPrefix(key, "-") || strings.HasPrefix(key, "!") {
			state, key = key[0], key[1:]
		}
		if k, v, ok := strings.Cut(key, "="); ok {
			key, value, state = k, v, '='
		}
		if key == "" {
			return nil
		}
		old := values[key]
		values[key] = attributeValue{state, value}
		if state == '+' && old.state != '+' {
			for _, nested := range macros[key] {
				if err := apply(nested, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	dirs := []string{""}
	dir := path.Dir(name)
	if dir != "." {
		parts := strings.Split(dir, "/")
		for i := range parts {
			dirs = append(dirs, strings.Join(parts[:i+1], "/"))
		}
	}
	total := int64(0)
	for _, base := range dirs {
		entry, err := s.Resolve(ctx, path.Join(base, ".gitattributes"))
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if entry.Mode&0170000 != 0100000 {
			continue
		}
		total += entry.Size
		if total > 4<<20 {
			return false, fmt.Errorf("attribute files exceed 4 MiB request budget")
		}
		data, err := s.historyBlob(ctx, entry)
		if err != nil {
			return false, err
		}
		// Macro definitions apply throughout their attribute file, including
		// rules appearing before the definition. Resolve definitions before rules.
		if base == "" {
			definitions := bufio.NewScanner(strings.NewReader(string(data)))
			definitions.Buffer(make([]byte, 4096), 1<<20)
			for definitions.Scan() {
				line := strings.TrimSpace(definitions.Text())
				if !strings.HasPrefix(line, "[attr]") {
					continue
				}
				pattern, rest, err := attributePattern(line)
				if err != nil {
					return false, err
				}
				if len(macros) >= 4096 {
					return false, fmt.Errorf("too many attribute macros")
				}
				macros[strings.TrimPrefix(pattern, "[attr]")] = strings.Fields(rest)
			}
			if err := definitions.Err(); err != nil {
				return false, err
			}
		}
		scan := bufio.NewScanner(strings.NewReader(string(data)))
		scan.Buffer(make([]byte, 4096), 1<<20)
		for scan.Scan() {
			line := strings.TrimSpace(scan.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			pattern, rest, err := attributePattern(line)
			if err != nil {
				return false, err
			}
			settings := strings.Fields(rest)
			if len(settings) == 0 {
				continue
			}
			if strings.HasPrefix(pattern, "[attr]") {
				continue
			}
			if strings.HasPrefix(pattern, "!") || strings.HasSuffix(pattern, "/") {
				continue
			}
			relative := name
			if base != "" {
				relative = strings.TrimPrefix(name, base+"/")
			}
			if !strings.Contains(pattern, "/") {
				relative = path.Base(relative)
			}
			pattern = strings.TrimPrefix(pattern, "/")
			re, err := pathspec.Wildmatch(pattern, true, false)
			if err != nil {
				return false, err
			}
			if re.MatchString(pathspec.Bytes(relative)) {
				for _, setting := range settings {
					if err := apply(setting, 0); err != nil {
						return false, err
					}
				}
			}
		}
		if err := scan.Err(); err != nil {
			return false, err
		}
	}
	for _, r := range requirements {
		value := values[r.Name]
		if r.State == '!' {
			if value.state != 0 && value.state != '!' {
				return false, nil
			}
			continue
		}
		if value.state != r.State || (r.State == '=' && value.value != r.Value) {
			return false, nil
		}
	}
	return true, nil
}
func attributePattern(line string) (string, string, error) {
	if !strings.HasPrefix(line, "\"") {
		p, rest, _ := strings.Cut(line, " ")
		if i := strings.IndexAny(p, "\t\r"); i >= 0 {
			return p[:i], p[i+1:] + " " + rest, nil
		}
		return p, rest, nil
	}
	escaped := false
	for i := 1; i < len(line); i++ {
		if escaped {
			escaped = false
			continue
		}
		if line[i] == '\\' {
			escaped = true
			continue
		}
		if line[i] == '"' {
			p, err := strconv.Unquote(line[:i+1])
			return p, line[i+1:], err
		}
	}
	return "", "", fmt.Errorf("unterminated quoted attribute pattern")
}
