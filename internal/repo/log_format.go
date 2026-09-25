package repo

import (
	"fmt"
	"io"
	"strings"
	"time"
)

func WriteLogEntry(out io.Writer, e LogEntry, oneline bool) error {
	var text strings.Builder
	message := strings.Trim(string(e.Message), "\n")
	if oneline {
		var subject []string
		for _, line := range strings.Split(message, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				break
			}
			subject = append(subject, line)
		}
		fmt.Fprintf(&text, "%s %s", e.ShortSHA, strings.Join(subject, " "))
		if e.MessageTruncated {
			text.WriteString(" [message truncated]")
		}
		text.WriteByte('\n')
	} else {
		fmt.Fprintf(&text, "commit %s\n", e.SHA)
		if len(e.Parents) > 1 {
			text.WriteString("Merge:")
			for i, parent := range e.Parents {
				short := parent[:min(7, len(parent))]
				if i < len(e.ShortParents) && e.ShortParents[i] != "" {
					short = e.ShortParents[i]
				}
				fmt.Fprintf(&text, " %s", short)
			}
			text.WriteByte('\n')
		}
		fmt.Fprintf(&text, "Author: %s", e.Author)
		if e.AuthorTruncated {
			text.WriteString(" [author truncated]")
		}
		date := time.Unix(e.AuthorTime, 0).In(time.FixedZone("", int(e.AuthorOffset)*60))
		fmt.Fprintf(&text, "\nDate:   %s\n\n", date.Format("Mon Jan 2 15:04:05 2006 -0700"))
		if message != "" {
			for _, line := range strings.Split(message, "\n") {
				fmt.Fprintf(&text, "    %s\n", expandTabs(line))
			}
		}
		if e.MessageTruncated {
			text.WriteString("    [message truncated after 24576 bytes]\n")
		}
	}
	_, err := io.WriteString(out, text.String())
	return err
}

func expandTabs(line string) string {
	if !strings.ContainsRune(line, '\t') {
		return line
	}
	var result strings.Builder
	column := 0
	for _, r := range line {
		if r == '\t' {
			n := 8 - column%8
			result.WriteString(strings.Repeat(" ", n))
			column += n
		} else {
			result.WriteRune(r)
			column++
		}
	}
	return result.String()
}
