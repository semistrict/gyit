package controlcli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
	"gyit/internal/repo"
)

func runLog(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	f := flag.NewFlagSet("log", flag.ContinueOnError)
	f.SetOutput(stderr)
	socket := f.String("socket", "", "mount control socket (default: discover from current directory)")
	timeout := f.Duration("timeout", 0, "maximum time for log output (0 waits until completion or cancellation)")
	oneline := f.Bool("oneline", false, "show abbreviated IDs and subjects")
	follow := f.Bool("follow", false, "continue file history across renames")
	firstParent := f.Bool("first-parent", false, "follow only the first parent at merges")
	count := 0
	f.IntVar(&count, "n", 0, "limit the number of commits (default: all)")
	f.IntVar(&count, "max-count", 0, "limit the number of commits (default: all)")
	var normalized, paths []string
	separated := false
	for _, arg := range args {
		if separated {
			paths = append(paths, arg)
			continue
		}
		if arg == "--" {
			separated = true
			continue
		}
		if strings.HasPrefix(arg, "-n") && len(arg) > 2 && arg[2] != '=' {
			normalized = append(normalized, "-n", arg[2:])
			continue
		}
		if len(arg) > 1 && arg[0] == '-' {
			if _, err := strconv.Atoi(arg[1:]); err == nil {
				normalized = append(normalized, "-n", arg[1:])
				continue
			}
		}
		normalized = append(normalized, arg)
	}
	if err := f.Parse(orderFlags(normalized)); err != nil {
		return err
	}
	if count < 0 || count > repo.MaxLogCount {
		return fmt.Errorf("log count must be between 0 and %d", repo.MaxLogCount)
	}
	unlimited := true
	f.Visit(func(flag *flag.Flag) {
		if flag.Name == "n" || flag.Name == "max-count" {
			unlimited = false
		}
	})
	if separated && f.NArg() > 1 {
		return fmt.Errorf("usage: gyit log [--oneline] [-n COUNT] [--first-parent] [REVISION] [-- PATH...]")
	}
	if *timeout < 0 {
		return fmt.Errorf("--timeout must not be negative")
	}
	revision := ""
	if f.NArg() > 0 {
		revision = f.Arg(0)
	}
	possiblePath := !separated && f.NArg() > 0
	if possiblePath {
		paths = append([]string{revision}, f.Args()[1:]...)
	}
	resolvedSocket, location, err := historyLocation(*socket, []string{"."})
	if err != nil {
		return err
	}
	*socket = resolvedSocket
	converted := make([][]byte, len(paths))
	for i, p := range paths {
		if filepath.IsAbs(p) {
			_, absolute, err := historyLocation(*socket, []string{p})
			if err != nil {
				return err
			}
			p = ":(top)" + string(absolute[0])
		}
		converted[i] = []byte(p)
	}
	request := &pb.LogRequest{Revision: revision, MaxCount: uint32(count), Unlimited: unlimited, FirstParent: *firstParent, FullCommitIds: !*oneline, Paths: converted, Follow: *follow, PathPrefix: location[0]}
	if possiblePath {
		request.PossiblePath, request.Paths = converted[0], converted[1:]
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	client := control.Client{Endpoint: *socket}
	first := true
	return client.LogPaths(ctx, request, func(entry *pb.LogEntry) error {
		if !*oneline && !first {
			if _, err := fmt.Fprintln(stdout); err != nil {
				return err
			}
		}
		first = false
		return writeLogEntry(stdout, entry, *oneline)
	})
}

func writeLogEntry(out io.Writer, e *pb.LogEntry, oneline bool) error {
	var shortParents []string
	if len(e.ParentAbbrevLengths) != 0 {
		if len(e.ParentAbbrevLengths) != len(e.Parents) {
			return fmt.Errorf("invalid parent abbreviation count")
		}
		shortParents = make([]string, len(e.Parents))
		for i, length := range e.ParentAbbrevLengths {
			if length == 0 || int(length) > len(e.Parents[i]) {
				return fmt.Errorf("invalid parent abbreviation length")
			}
			shortParents[i] = e.Parents[i][:length]
		}
	}
	return repo.WriteLogEntry(out, repo.LogEntry{SHA: e.Sha, ShortSHA: e.ShortSha, Parents: e.Parents, ShortParents: shortParents, Author: e.Author, Message: e.Message, AuthorTime: e.AuthorTime, AuthorOffset: e.AuthorOffsetMinutes, MessageTruncated: e.MessageTruncated, AuthorTruncated: e.AuthorTruncated}, oneline)
}
