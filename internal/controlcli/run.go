// Package controlcli implements gyit's mount administration commands.
package controlcli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
)

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) > 0 && args[0] == "checkout" {
		args = append([]string(nil), args...)
		args[0] = "switch"
	}
	if len(args) > 0 && IsViewCommand(args[0]) {
		run := func(ctx context.Context, out io.Writer) error { return runView(ctx, args, out) }
		if args[0] == "show" {
			return pageLog(ctx, stdout, stderr, run)
		}
		return run(ctx, stdout)
	}
	if len(args) > 0 && (args[0] == "diff" || args[0] == "blame" || args[0] == "annotate") {
		return runHistory(ctx, args, stdout, stderr)
	}
	if len(args) > 0 && args[0] == "log" {
		return pageLog(ctx, stdout, stderr, func(ctx context.Context, out io.Writer) error {
			return runLog(ctx, args[1:], out, stderr)
		})
	}
	if len(args) == 0 || (args[0] != "status" && args[0] != "switch" && args[0] != "update") {
		return fmt.Errorf("usage: gyit <update|status|switch|checkout|log|diff|blame|annotate> [--socket PATH] [REVISION]")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	f.SetOutput(stderr)
	socket := f.String("socket", "", "mount control socket (default: discover from current directory)")
	defaultTimeout := 35 * time.Second
	if args[0] == "update" {
		defaultTimeout = 5 * time.Minute
	}
	timeout := f.Duration("timeout", defaultTimeout, "maximum time to wait for a response")
	var short, branch, zero bool
	var porcelain string
	if args[0] == "status" {
		f.BoolVar(&short, "short", false, "show short status")
		f.BoolVar(&short, "s", false, "show short status")
		f.BoolVar(&branch, "branch", false, "show branch in short or porcelain status")
		f.BoolVar(&branch, "b", false, "show branch in short or porcelain status")
		f.BoolVar(&zero, "z", false, "NUL-terminate short-format records")
		f.StringVar(&porcelain, "porcelain", "", "machine-readable status (1 or 2)")
		args = append([]string(nil), args...)
		normalized := []string{args[0]}
		for _, arg := range args[1:] {
			switch arg {
			case "--porcelain":
				normalized = append(normalized, "--porcelain=1")
			case "-sb", "-bs":
				normalized = append(normalized, "--short", "--branch")
			default:
				normalized = append(normalized, arg)
			}
		}
		args = normalized
	}
	var sha string
	if args[0] == "switch" {
		f.StringVar(&sha, "sha", "", "legacy alias for positional revision")
	}
	if err := f.Parse(orderFlags(args[1:])); err != nil {
		return err
	}
	if porcelain != "" && porcelain != "1" && porcelain != "v1" && porcelain != "2" && porcelain != "v2" {
		return fmt.Errorf("unsupported porcelain version %q", porcelain)
	}
	if zero && porcelain == "" {
		porcelain = "1"
	}
	if *timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	if (args[0] == "status" || args[0] == "update") && f.NArg() != 0 {
		return fmt.Errorf("%s takes no revision", args[0])
	}
	if args[0] == "switch" {
		if f.NArg() == 1 && sha == "" {
			sha = f.Arg(0)
		} else if f.NArg() != 0 || sha == "" {
			return fmt.Errorf("usage: gyit switch [options] REVISION")
		}
	}
	if *socket == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return err
		}
		discovered, err := control.Discover(cwd)
		if err != nil {
			return err
		}
		*socket = discovered
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client := control.Client{Endpoint: *socket}
	var s *pb.Snapshot
	var err error
	if args[0] == "status" {
		s, err = client.Status(ctx)
	} else if args[0] == "update" {
		s, err = client.Update(ctx)
	} else {
		s, err = client.Switch(ctx, sha)
	}
	if err != nil {
		return err
	}
	if args[0] == "status" {
		return writeStatus(stdout, s, short, branch, zero, porcelain)
	}
	_, err = fmt.Fprintf(stdout, "sha %s\ntree %s\n", s.Sha, s.Tree)
	return err
}

// Allow flags before or after the revision, including a lone '-' for previous.
func orderFlags(args []string) []string {
	var flags, positional []string
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
		flags = append(flags, arg)
		name := strings.TrimLeft(arg, "-")
		if !strings.Contains(arg, "=") && (name == "socket" || name == "timeout" || name == "sha" || name == "n" || name == "max-count" || name == "L" || name == "U" || name == "unified") && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return append(append(flags, "--"), positional...)
}

// Every mounted snapshot is immutable. Status describes its checkout identity;
// there is no index or writable worktree to scan for changes.
func writeStatus(out io.Writer, s *pb.Snapshot, short, branch, zero bool, porcelain string) error {
	if short || porcelain != "" {
		if !branch {
			return nil
		}
		if porcelain == "2" || porcelain == "v2" {
			name := s.Branch
			if name == "" {
				name = "(detached)"
			}
			end := "\n"
			if zero {
				end = "\x00"
			}
			_, err := fmt.Fprintf(out, "# branch.oid %s%s# branch.head %s%s", s.Sha, end, name, end)
			return err
		}
		name := s.Branch
		if name == "" {
			name = "HEAD (no branch)"
		}
		end := "\n"
		if zero {
			end = "\x00"
		}
		_, err := fmt.Fprintf(out, "## %s%s", name, end)
		return err
	}
	heading := "On branch " + s.Branch
	if s.Branch == "" {
		label := s.DetachedAt
		if label == "" {
			label = s.Sha[:min(7, len(s.Sha))]
		}
		heading = "HEAD detached at " + label
	}
	_, err := fmt.Fprintf(out, "%s\nnothing to commit, working tree clean\n", heading)
	return err
}
