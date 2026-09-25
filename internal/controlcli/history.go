package controlcli

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gat/internal/control"
	pb "gat/internal/gen/gat/control/v1"
)

func runHistory(ctx context.Context, args []string, out, stderr io.Writer) error {
	command := args[0]
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(stderr)
	socket := f.String("socket", "", "mount control socket (default: discover from current directory)")
	timeout := f.Duration("timeout", 5*time.Minute+5*time.Second, "maximum request time")
	var nameOnly, nameStatus, firstParent, porcelain bool
	noRenames := true
	var lines string
	unified := 3
	if command == "diff" {
		f.BoolVar(&noRenames, "no-renames", true, "compare paths without rename detection")
		f.BoolVar(&nameOnly, "name-only", false, "show changed paths without reading files")
		f.BoolVar(&nameStatus, "name-status", false, "show change types and paths")
		f.IntVar(&unified, "unified", 3, "context lines (0..100)")
		f.IntVar(&unified, "U", 3, "context lines (0..100)")
	} else {
		f.BoolVar(&firstParent, "first-parent", false, "follow only first parents at merges")
		f.BoolVar(&porcelain, "line-porcelain", false, "show structured per-line attribution")
		f.StringVar(&lines, "L", "", "line range START,END (inclusive)")
	}
	var before, paths []string
	separated := false
	for _, a := range args[1:] {
		if separated {
			paths = append(paths, a)
			continue
		}
		if a == "--" {
			separated = true
			continue
		}
		if command == "diff" && strings.HasPrefix(a, "-U") && len(a) > 2 && a[2] != '=' {
			before = append(before, "-U", a[2:])
			continue
		}
		if command != "diff" && strings.HasPrefix(a, "-L") && len(a) > 2 && a[2] != '=' {
			before = append(before, "-L", a[2:])
			continue
		}
		before = append(before, a)
	}
	if err := f.Parse(orderFlags(before)); err != nil {
		return err
	}
	if *timeout <= 0 {
		return fmt.Errorf("--timeout must be positive")
	}
	if unified < 0 || unified > 100 {
		return fmt.Errorf("diff context must be 0..100")
	}
	if !noRenames {
		return fmt.Errorf("rename detection is not supported; use --no-renames")
	}
	if nameOnly && nameStatus {
		return fmt.Errorf("choose --name-only or --name-status")
	}
	revisions := f.Args()
	if command == "diff" {
		if len(revisions) == 1 && strings.Contains(revisions[0], "..") {
			if strings.Contains(revisions[0], "...") {
				return fmt.Errorf("three-dot merge-base diff is not supported")
			}
			revisions = strings.Split(revisions[0], "..")
			for i := range revisions {
				if revisions[i] == "" {
					revisions[i] = "HEAD"
				}
			}
		}
		if len(revisions) > 2 {
			return fmt.Errorf("usage: gat diff [FROM [TO]] [-- PATH...]")
		}
	} else {
		if !separated && len(revisions) > 0 {
			paths = []string{revisions[len(revisions)-1]}
			revisions = revisions[:len(revisions)-1]
		}
		if len(paths) != 1 || len(revisions) > 1 {
			return fmt.Errorf("usage: gat %s [-L START,END] [REVISION] -- FILE", command)
		}
	}
	resolvedSocket, converted, err := historyLocation(*socket, paths)
	if err != nil {
		return err
	}
	*socket = resolvedSocket
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	client := control.Client{Socket: *socket}
	if command == "diff" {
		req := &pb.DiffRequest{Paths: converted, NameOnly: nameOnly, NameStatus: nameStatus, ContextLines: uint32(unified)}
		if len(revisions) > 0 {
			req.FromRevision = revisions[0]
		}
		if len(revisions) > 1 {
			req.ToRevision = revisions[1]
		}
		return client.Diff(ctx, req, func(data []byte) error { _, err := out.Write(data); return err })
	}
	req := &pb.BlameRequest{Path: converted[0], FirstParent: firstParent}
	if len(revisions) > 0 {
		req.Revision = revisions[0]
	}
	if lines != "" {
		parts := strings.Split(lines, ",")
		if len(parts) != 2 {
			return fmt.Errorf("-L expects START,END")
		}
		a, ae := strconv.Atoi(parts[0])
		b, be := strconv.Atoi(parts[1])
		if ae != nil || be != nil || a < 1 || b < a || b > 100000 {
			return fmt.Errorf("-L requires 1 <= START <= END <= 100000")
		}
		req.StartLine, req.EndLine = uint32(a), uint32(b)
	}
	req.IncludeCommitter = porcelain
	if porcelain || command == "annotate" {
		return client.Blame(ctx, req, func(line *pb.BlameLine) error {
			return writeBlameLine(out, line, command == "annotate", porcelain, 0, 0)
		})
	}
	// Git aligns author and line-number columns across the complete output. Spool
	// protobuf records to a private bounded file instead of retaining all lines.
	spool, err := os.CreateTemp("", "gat-blame-*")
	if err != nil {
		return err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	authorWidth, lineWidth, total := 0, 0, 0
	err = client.Blame(ctx, req, func(line *pb.BlameLine) error {
		name, _, _ := strings.Cut(string(line.Author), " <")
		authorWidth = max(authorWidth, utf8.RuneCountInString(name))
		lineWidth = max(lineWidth, len(strconv.Itoa(int(line.FinalLine))))
		data, e := proto.Marshal(line)
		if e != nil {
			return e
		}
		total += 4 + len(data)
		if total > 256<<20 {
			return fmt.Errorf("blame output exceeds 256 MiB spool limit")
		}
		if e = binary.Write(spool, binary.LittleEndian, uint32(len(data))); e != nil {
			return e
		}
		_, e = spool.Write(data)
		return e
	})
	if err != nil {
		return err
	}
	if _, err = spool.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var n uint32
		if err = binary.Read(spool, binary.LittleEndian, &n); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		if n > 64<<10 {
			return fmt.Errorf("oversized blame record")
		}
		data := make([]byte, n)
		if _, err = io.ReadFull(spool, data); err != nil {
			return err
		}
		var line pb.BlameLine
		if err = proto.Unmarshal(data, &line); err != nil {
			return err
		}
		if err = writeBlameLine(out, &line, false, false, authorWidth, lineWidth); err != nil {
			return err
		}
	}
}

func writeBlameLine(out io.Writer, e *pb.BlameLine, annotate, porcelain bool, authorWidth, lineWidth int) error {
	author := string(e.Author)
	name, mail := author, ""
	if p := strings.LastIndex(author, " <"); p >= 0 {
		name, mail = author[:p], author[p+1:]
	}
	zone := time.FixedZone("", int(e.AuthorOffsetMinutes)*60)
	date := time.Unix(e.AuthorTime, 0).In(zone)
	var b strings.Builder
	if e.AuthorTruncated {
		name += " [author truncated]"
	}
	if porcelain {
		if !e.HasCommitter {
			return fmt.Errorf("commit committer metadata is missing; re-import this store for porcelain blame")
		}
		fmt.Fprintf(&b, "%s %d %d", e.Sha, e.OriginalLine, e.FinalLine)
		if e.GroupLines > 0 {
			fmt.Fprintf(&b, " %d", e.GroupLines)
		}
		committerName, committerMail := string(e.Committer), ""
		if p := strings.LastIndex(committerName, " <"); p >= 0 {
			committerName, committerMail = committerName[:p], committerName[p+1:]
		}
		if e.CommitterTruncated {
			committerName += " [committer truncated]"
		}
		committerDate := time.Unix(e.CommitterTime, 0).In(time.FixedZone("", int(e.CommitterOffsetMinutes)*60))
		fmt.Fprintf(&b, "\nauthor %s\nauthor-mail %s\nauthor-time %d\nauthor-tz %s\ncommitter %s\ncommitter-mail %s\ncommitter-time %d\ncommitter-tz %s\nsummary %s", name, mail, e.AuthorTime, date.Format("-0700"), committerName, committerMail, e.CommitterTime, committerDate.Format("-0700"), e.Summary)
		if e.MessageTruncated {
			b.WriteString(" [message truncated]")
		}
		b.WriteByte('\n')
		if e.Boundary {
			b.WriteString("boundary\n")
		}
		filename := string(e.Path)
		if strings.ContainsAny(filename, "\t\n\r\"\\") {
			filename = strconv.Quote(filename)
		}
		if e.PreviousSha != "" {
			previousPath := string(e.PreviousPath)
			if previousPath == "" {
				previousPath = string(e.Path)
			}
			if strings.ContainsAny(previousPath, "\t\n\r\"\\") {
				previousPath = strconv.Quote(previousPath)
			}
			fmt.Fprintf(&b, "previous %s %s\n", e.PreviousSha, previousPath)
		}
		fmt.Fprintf(&b, "filename %s\n\t%s", filename, e.Content)
	} else {
		sha := e.Sha[:min(8, len(e.Sha))]
		if e.Boundary && !annotate {
			sha = "^" + e.Sha[:min(7, len(e.Sha))]
		}
		if annotate {
			fmt.Fprintf(&b, "%s\t(%10s\t%s\t%d)%s", sha, name, date.Format("2006-01-02 15:04:05 -0700"), e.FinalLine, e.Content)
		} else {
			fmt.Fprintf(&b, "%s (%-*s %s %*d) %s", sha, authorWidth, name, date.Format("2006-01-02 15:04:05 -0700"), lineWidth, e.FinalLine, e.Content)
		}
	}
	if len(e.Content) == 0 || e.Content[len(e.Content)-1] != '\n' {
		b.WriteByte('\n')
	}
	_, err := io.WriteString(out, b.String())
	return err
}

// Resolve paths lexically so a final symlink remains a repository entry.
func historyLocation(socket string, paths []string) (string, [][]byte, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", nil, err
	}
	cwd, err = filepath.EvalSymlinks(cwd)
	if err != nil {
		return "", nil, err
	}
	found, root, discoverErr := control.DiscoverMount(cwd)
	if socket == "" {
		if discoverErr != nil {
			return "", nil, discoverErr
		}
		socket = found
	} else if discoverErr != nil || found != socket {
		root = ""
	}
	converted := make([][]byte, len(paths))
	for i, p := range paths {
		if root != "" {
			// Keep a final symlink as a repository path: its stored contents
			// are themselves valid diff/blame input.
			if !filepath.IsAbs(p) {
				p = filepath.Join(cwd, p)
			}
			p, err = filepath.Rel(root, filepath.Clean(p))
			if err != nil {
				return "", nil, err
			}
		} else {
			if filepath.IsAbs(p) {
				return "", nil, fmt.Errorf("with --socket outside a mount, use repository-relative paths")
			}
			p = filepath.Clean(p)
		}
		if p == ".." || strings.HasPrefix(p, ".."+string(filepath.Separator)) {
			return "", nil, fmt.Errorf("path is outside the mounted repository")
		}
		converted[i] = []byte(filepath.ToSlash(p))
	}
	return socket, converted, nil
}
