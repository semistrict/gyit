package controlcli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gat/internal/control"
	pb "gat/internal/gen/gat/control/v1"
)

func IsViewCommand(name string) bool {
	switch name {
	case "show", "ls-tree", "ls-files", "cat-file", "grep", "branch", "tag", "show-ref", "rev-parse", "rev-list", "merge-base", "shortlog":
		return true
	}
	return false
}

func runView(ctx context.Context, args []string, out io.Writer) error {
	socket := ""
	timeout := 5*time.Minute + 5*time.Second
	var forwarded [][]byte
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			for _, a := range args[i:] {
				forwarded = append(forwarded, []byte(a))
			}
			break
		}
		key, value, assigned := strings.Cut(arg, "=")
		if key != "--socket" && key != "--timeout" {
			forwarded = append(forwarded, []byte(arg))
			// A command option owns its next token even when that value is
			// spelled --socket, --timeout, or -- (notably grep -e patterns).
			if !assigned && viewOptionHasValue(args[0], key) && i+1 < len(args) {
				i++
				forwarded = append(forwarded, []byte(args[i]))
			}
			continue
		}
		if !assigned {
			i++
			if i == len(args) {
				return fmt.Errorf("%s requires a value", key)
			}
			value = args[i]
		}
		if key == "--socket" {
			socket = value
		} else {
			var err error
			timeout, err = time.ParseDuration(value)
			if err != nil {
				return err
			}
		}
	}
	if timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return err
	}
	discovered, root, discoverErr := control.DiscoverMount(cwd)
	prefix := ""
	if socket == "" {
		if discoverErr != nil {
			return discoverErr
		}
		socket = discovered
	}
	if discoverErr == nil && socket == discovered {
		prefix, err = filepath.Rel(root, cwd)
		if err != nil {
			return err
		}
		if prefix == "." {
			prefix = ""
		}
		prefix = filepath.ToSlash(prefix)
	} else {
		root = ""
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return (control.Client{Endpoint: socket}).View(ctx, &pb.ViewRequest{Command: args[0], Arguments: forwarded, PathPrefix: []byte(prefix), MountRoot: []byte(root)}, func(b []byte) error { _, err := out.Write(b); return err })
}

func viewOptionHasValue(command, option string) bool {
	if !strings.HasPrefix(option, "-") {
		return false
	}
	name := strings.TrimLeft(option, "-")
	switch command {
	case "grep":
		return name == "e" || name == "regexp"
	case "show":
		return name == "U" || name == "unified"
	case "rev-list":
		return name == "n" || name == "max-count"
	case "shortlog":
		return name == "max-count"
	case "branch", "tag":
		return name == "format"
	case "ls-tree":
		return name == "abbrev"
	}
	return false
}
