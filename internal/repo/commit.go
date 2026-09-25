package repo

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strconv"
	"strings"
)

const maxLogMessage = 24 << 10
const maxLogAuthor = 4 << 10

type commitInfo struct {
	Author, Message                   []byte
	AuthorTime, CommitTime            int64
	AuthorOffset                      int32
	MessageTruncated, AuthorTruncated bool
	Committer                         []byte
	CommitterOffset                   int32
	CommitterTruncated, HasCommitter  bool
}

// Consume even very large signatures/headers while retaining only bounded
// prefixes and suffixes. The outer importer still hashes every original byte.
func commitLine(r *bufio.Reader) (prefix, tail []byte, truncated bool, err error) {
	total := 0
	for {
		part, e := r.ReadSlice('\n')
		total += len(part)
		if len(prefix) < maxLogAuthor+128 {
			n := min(len(part), maxLogAuthor+128-len(prefix))
			prefix = append(prefix, part[:n]...)
		}
		tail = append(tail, part...)
		if len(tail) > 256 {
			tail = append([]byte(nil), tail[len(tail)-256:]...)
		}
		if e != bufio.ErrBufferFull {
			return bytes.TrimSuffix(prefix, []byte{'\n'}), bytes.TrimSuffix(tail, []byte{'\n'}), total > maxLogAuthor+128, e
		}
	}
}

func parseIdentity(prefix, tail []byte, truncated bool) ([]byte, int64, int32, bool, error) {
	fields := strings.Fields(string(tail))
	if len(fields) < 2 {
		return nil, 0, 0, false, fmt.Errorf("invalid commit identity")
	}
	stamp, zone := fields[len(fields)-2], fields[len(fields)-1]
	timestamp, err := strconv.ParseInt(stamp, 10, 64)
	if err != nil {
		return nil, 0, 0, false, fmt.Errorf("invalid commit timestamp: %w", err)
	}
	if len(zone) != 5 || (zone[0] != '+' && zone[0] != '-') {
		return nil, 0, 0, false, fmt.Errorf("invalid commit timezone")
	}
	hours, e1 := strconv.Atoi(zone[1:3])
	minutes, e2 := strconv.Atoi(zone[3:])
	if e1 != nil || e2 != nil || hours > 23 || minutes > 59 {
		return nil, 0, 0, false, fmt.Errorf("invalid commit timezone")
	}
	offset := int32(hours*60 + minutes)
	if zone[0] == '-' {
		offset = -offset
	}
	identity := prefix
	if !truncated {
		identity = bytes.TrimSuffix(prefix, []byte(" "+stamp+" "+zone))
	}
	return append([]byte(nil), identity[:min(len(identity), maxLogAuthor)]...), timestamp, offset, truncated || len(identity) > maxLogAuthor, nil
}

func parseCommit(body io.Reader) (string, commitInfo, error) {
	return parseBufferedCommit(bufio.NewReaderSize(body, 32<<10))
}

func parseBufferedCommit(r *bufio.Reader) (string, commitInfo, error) {
	return parseBufferedCommitParents(r, nil, 0)
}

// A nil parent sink preserves the original parser. The qualified all-local
// mode captures ordered parents while consuming the same bounded body once.
func parseBufferedCommitParents(r *bufio.Reader, parentIDs *[]string, oidBytes int) (string, commitInfo, error) {
	var tree string
	var info commitInfo
	for {
		prefix, tail, truncated, err := commitLine(r)
		if err != nil {
			return "", info, fmt.Errorf("read commit headers: %w", err)
		}
		if len(prefix) == 0 {
			break
		}
		key, value, found := bytes.Cut(prefix, []byte{' '})
		if !found {
			continue
		}
		switch string(key) {
		case "tree":
			tree = string(value)
		case "parent":
			if parentIDs != nil {
				if truncated || len(value) != oidBytes*2 || len(*parentIDs) >= maxLogParents {
					return "", info, fmt.Errorf("raw commit parent width or count exceeds supported bound")
				}
				var decoded [32]byte
				if oidBytes != 20 && oidBytes != 32 {
					return "", info, fmt.Errorf("raw parent object format")
				}
				if _, err := hex.Decode(decoded[:oidBytes], value); err != nil {
					return "", info, fmt.Errorf("invalid raw commit parent: %w", err)
				}
				*parentIDs = append(*parentIDs, hex.EncodeToString(decoded[:oidBytes]))
			}
		case "author":
			info.Author, info.AuthorTime, info.AuthorOffset, info.AuthorTruncated, err = parseIdentity(value, tail, truncated)
			if err != nil {
				return "", info, err
			}
		case "committer":
			info.Committer, info.CommitTime, info.CommitterOffset, info.CommitterTruncated, err = parseIdentity(value, tail, truncated)
			if err != nil {
				return "", info, err
			}
			info.HasCommitter = true
		}
	}
	data, err := io.ReadAll(io.LimitReader(r, maxLogMessage+1))
	if err != nil {
		return "", info, err
	}
	info.MessageTruncated = len(data) > maxLogMessage
	info.Message = append([]byte(nil), data[:min(len(data), maxLogMessage)]...)
	if _, err := io.Copy(io.Discard, r); err != nil {
		return "", info, err
	}
	if tree == "" {
		return "", info, fmt.Errorf("commit missing tree")
	}
	return tree, info, nil
}

func readCommitMetadata(r *bufio.Reader, oid, format string) (commitInfo, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return commitInfo{}, err
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != oid || fields[1] != "commit" {
		return commitInfo{}, fmt.Errorf("invalid commit metadata response")
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 {
		return commitInfo{}, fmt.Errorf("invalid commit size")
	}
	var h hash.Hash = sha1.New()
	if format == "sha256" {
		h = sha256.New()
	}
	fmt.Fprintf(h, "commit %d\x00", size)
	limited := &io.LimitedReader{R: r, N: size}
	_, info, err := parseCommit(io.TeeReader(limited, h))
	if err != nil {
		return info, err
	}
	if limited.N != 0 || hex.EncodeToString(h.Sum(nil)) != oid {
		return info, fmt.Errorf("commit checksum mismatch")
	}
	if b, err := r.ReadByte(); err != nil || b != '\n' {
		return info, fmt.Errorf("invalid commit separator")
	}
	return info, nil
}
