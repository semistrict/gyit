package repo

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"gat/internal/store"
)

var ErrViewNoMatch = errors.New("no matching result")

type ViewOptions struct {
	Command   string
	Args      []string
	Prefix    string
	MountRoot string
}

func (r *Repository) View(ctx context.Context, current *Snapshot, opt ViewOptions, out io.Writer) error {
	if len(opt.Args) > 256 {
		return invalidRevision("too many command arguments")
	}
	// Resolve every argument against one publication, even while an importer
	// advances references between streamed output frames. Immutable object reads
	// still go straight to the original store and share its bounded cache.
	head, token, err := r.store.Get(ctx, "HEAD", 0, -1)
	if err != nil {
		return err
	}
	r = r.withStore(viewStore{Store: r.store, head: head, token: token})
	switch opt.Command {
	case "ls-tree", "ls-files", "cat-file", "grep":
		return r.ViewObjects(ctx, current, opt, out)
	case "branch", "tag", "show-ref", "rev-parse":
		return r.ViewRefs(ctx, current, opt, out)
	case "rev-list", "merge-base", "shortlog":
		return r.ViewGraph(ctx, current, opt, out)
	case "show":
		return r.ViewShow(ctx, current, opt, out)
	default:
		return invalidRevision("unknown read-only command")
	}
}

type viewStore struct {
	store.Store
	head  []byte
	token string
}

func (s viewStore) Get(ctx context.Context, key string, offset, length int64) ([]byte, string, error) {
	if key != "HEAD" {
		return s.Store.Get(ctx, key, offset, length)
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if offset != 0 || length != -1 {
		return nil, "", fmt.Errorf("invalid publication read")
	}
	return s.head, s.token, nil
}

// Reorder recognized options while preserving positional order and the --
// boundary. Option values are consumed before deciding whether a token is a flag.
func parseViewFlags(f *flag.FlagSet, args []string) error {
	var options, positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if arg == "-" || !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		name := strings.TrimLeft(arg, "-")
		key, _, assigned := strings.Cut(name, "=")
		item := f.Lookup(key)
		if item == nil && !strings.HasPrefix(arg, "--") && len(name) > 1 {
			if single := f.Lookup(name[:1]); single != nil {
				boolean, ok := single.Value.(interface{ IsBoolFlag() bool })
				if !ok || !boolean.IsBoolFlag() {
					options = append(options, "-"+name[:1], name[1:])
					continue
				}
			}
			allBool := true
			for _, c := range name {
				flag := f.Lookup(string(c))
				if flag == nil {
					allBool = false
					break
				}
				b, ok := flag.Value.(interface{ IsBoolFlag() bool })
				if !ok || !b.IsBoolFlag() {
					allBool = false
					break
				}
			}
			if allBool {
				for _, c := range name {
					options = append(options, "-"+string(c))
				}
				continue
			}
		}
		options = append(options, arg)
		if item != nil && !assigned {
			boolean, ok := item.Value.(interface{ IsBoolFlag() bool })
			if !ok || !boolean.IsBoolFlag() {
				if i+1 >= len(args) {
					return fmt.Errorf("option %s requires a value", arg)
				}
				i++
				options = append(options, args[i])
			}
		}
	}
	return f.Parse(append(append(options, "--"), positional...))
}

// Keep immutable payload leases available through the pinned-publication view.
func (s viewStore) Acquire(ctx context.Context, key string, off, n int64) ([]byte, func(), error) {
	if key == "HEAD" {
		b, _, err := s.Get(ctx, key, off, n)
		return b, func() {}, err
	}
	return store.Acquire(ctx, s.Store, key, off, n)
}
