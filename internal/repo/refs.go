package repo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"gat/internal/store"
	bolt "go.etcd.io/bbolt"
)

// Parent records are streamed into the disk-backed stage. Old publications are
// upgraded with commit metadata only; existing blob packs are never reread.
func stageParents(ctx context.Context, source, input, format string, st *stage) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := git(ctx, source, "rev-list", "--parents", "--stdin")
	cmd.Stdin = strings.NewReader(input)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	defer func() { cancel(); _ = cmd.Wait() }()
	cat := git(ctx, source, "cat-file", "--batch")
	inputPipe, err := cat.StdinPipe()
	if err != nil {
		return err
	}
	output, err := cat.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cat.Start(); err != nil {
		return err
	}
	defer func() { cancel(); _ = inputPipe.Close(); _ = cat.Wait() }()
	reader := bufio.NewReaderSize(output, 64<<10)
	scan := bufio.NewScanner(out)
	scan.Buffer(make([]byte, 4096), 8<<20)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) == 0 {
			continue
		}
		if err := st.put("p/"+fields[0], parents{Parents: fields[1:]}); err != nil {
			return err
		}
		present, err := st.hasCommit(fields[0])
		if err != nil {
			return err
		}
		if !present {
			if _, err := fmt.Fprintln(inputPipe, fields[0]); err != nil {
				return err
			}
			info, err := readCommitMetadata(reader, fields[0], format)
			if err != nil {
				return err
			}
			if err := st.put("c/"+fields[0], info); err != nil {
				return err
			}
		}
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if err := cmd.Wait(); err != nil {
		return err
	}
	if err := inputPipe.Close(); err != nil {
		return err
	}
	return cat.Wait()
}

// Rebuild only the small refs index each publication, so deleted/renamed refs
// disappear atomically. Resolve tags through Git, including nested tags, while
// retaining tree/blob refs as reference identities. Filter commit refs outside
// imported history (for --rev imports); noncommit refs never become mount tips.
func importRefs(ctx context.Context, source string, db *bolt.DB, objects *index, base manifest) (pageRef, string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	refs := git(ctx, source, "for-each-ref", "--format=%(refname) %(objectname) %(symref)")
	output, err := refs.StdoutPipe()
	if err != nil {
		return pageRef{}, "", err
	}
	if err := refs.Start(); err != nil {
		return pageRef{}, "", err
	}
	defer func() { cancel(); _ = refs.Wait() }()
	cat := git(ctx, source, "cat-file", "--batch-check=%(objectname) %(objecttype)")
	input, err := cat.StdinPipe()
	if err != nil {
		return pageRef{}, "", err
	}
	result, err := cat.StdoutPipe()
	if err != nil {
		return pageRef{}, "", err
	}
	if err := cat.Start(); err != nil {
		return pageRef{}, "", err
	}
	defer func() { cancel(); _ = input.Close(); _ = cat.Wait() }()
	response := bufio.NewReader(result)
	tx, err := db.Begin(true)
	if err != nil {
		return pageRef{}, "", err
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()
	bucket, err := tx.CreateBucket([]byte("refs"))
	if err != nil {
		return pageRef{}, "", err
	}
	count := 0
	add := func(name, original, symbolic string) error {
		if _, err := fmt.Fprintln(input, original+"^{}"); err != nil {
			return err
		}
		line, err := response.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || (fields[1] != "commit" && fields[1] != "tree" && fields[1] != "blob") {
			return fmt.Errorf("cannot resolve source ref %q: %s", name, strings.TrimSpace(line))
		}
		var object object
		if err := objects.get(ctx, "o/"+fields[0], &object); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				if fields[1] == "commit" {
					return nil
				}
				// A complete ref census does not require every noncommit target
				// to be part of the selected committed payload closure. Listing
				// and rev-parse retain its identity; data reads still need data.
			} else {
				return err
			}
		} else if object.Kind != fields[1] {
			return fmt.Errorf("ref %q target kind differs from imported object", name)
		}
		data, err := marshal(reference{Commit: fields[0], ObjectID: original, SymbolicTarget: symbolic})
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("r/"+name), data); err != nil {
			return err
		}
		if original != "" && original != fields[0] {
			if err := bucket.Put([]byte("a/"+original), data); err != nil {
				return err
			}
		}
		count++
		if count%10000 == 0 {
			if err := tx.Commit(); err != nil {
				return err
			}
			tx, err = db.Begin(true)
			if err != nil {
				return err
			}
			bucket = tx.Bucket([]byte("refs"))
		}
		return nil
	}
	scan := bufio.NewScanner(output)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) < 2 || len(fields) > 3 {
			return pageRef{}, "", fmt.Errorf("invalid source ref")
		}
		symbolic := ""
		if len(fields) == 3 {
			symbolic = fields[2]
		}
		if err := add(fields[0], fields[1], symbolic); err != nil {
			return pageRef{}, "", err
		}
	}
	if err := scan.Err(); err != nil {
		return pageRef{}, "", err
	}
	if err := refs.Wait(); err != nil {
		return pageRef{}, "", err
	}
	head, err := git(ctx, source, "rev-parse", "--verify", "HEAD").Output()
	if err == nil {
		symbolic, _ := git(ctx, source, "symbolic-ref", "-q", "HEAD").Output()
		if err := add("HEAD", strings.TrimSpace(string(head)), strings.TrimSpace(string(symbolic))); err != nil {
			return pageRef{}, "", err
		}
	}
	if err := input.Close(); err != nil {
		return pageRef{}, "", err
	}
	if err := cat.Wait(); err != nil {
		return pageRef{}, "", err
	}
	if err := tx.Commit(); err != nil {
		return pageRef{}, "", err
	}
	idx := &index{store: objects.store, cache: objects.cache}
	var root pageRef
	var digest string
	err = db.View(func(tx *bolt.Tx) error {
		var err error
		bucket := tx.Bucket([]byte("refs"))
		hash := sha256.New()
		_ = bucket.ForEach(func(k, v []byte) error {
			for _, value := range [][]byte{k, v} {
				var size [8]byte
				binary.BigEndian.PutUint64(size[:], uint64(len(value)))
				hash.Write(size[:])
				hash.Write(value)
			}
			return nil
		})
		digest = fmt.Sprintf("%x", hash.Sum(nil))
		if digest == base.RefsHash {
			root = base.Refs
			return nil
		}
		root, err = idx.update(ctx, bucket)
		return err
	})
	return root, digest, err
}
