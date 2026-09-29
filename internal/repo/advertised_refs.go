//go:build !js

package repo

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"gyit/internal/store"

	bolt "go.etcd.io/bbolt"
)

// ImportAdvertisedReferences publishes a complete ls-remote --symref advertisement
// and resolves the requested name without requiring its objects to be downloaded.
// Staging lives on disk; neither the advertisement nor the ref set is held in RAM.
func (p *Progressive) ImportAdvertisedReferences(ctx context.Context, input io.Reader, selected string) (string, error) {
	p.writer.Lock()
	defer p.writer.Unlock()
	var sha string
	err := p.stage(func(refs *bolt.Bucket) error {
		// The advertisement is staged beside refs, so iterating it never observes
		// the reference rows derived from it and nothing needs deleting afterwards.
		advertised, err := refs.Tx().CreateBucket([]byte("advertisement"))
		if err != nil {
			return err
		}
		scan := bufio.NewScanner(input)
		scan.Buffer(make([]byte, 4096), 8192)
		for scan.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			fields := strings.Fields(scan.Text())
			if len(fields) == 3 && fields[0] == "ref:" {
				if err := advertised.Put([]byte("s/"+fields[2]), []byte(fields[1])); err != nil {
					return err
				}
				continue
			}
			if len(fields) != 2 || len(fields[0]) != 40 {
				return fmt.Errorf("invalid reference advertisement")
			}
			if _, err := hex.DecodeString(fields[0]); err != nil {
				return fmt.Errorf("invalid advertised object ID: %w", err)
			}
			if err := advertised.Put([]byte("o/"+fields[1]), []byte(strings.ToLower(fields[0]))); err != nil {
				return err
			}
		}
		if err := scan.Err(); err != nil {
			return err
		}
		cursor := advertised.Cursor()
		for key, oid := cursor.Seek([]byte("o/")); key != nil && strings.HasPrefix(string(key), "o/"); key, oid = cursor.Next() {
			name := string(key[2:])
			if strings.HasSuffix(name, "^{}") {
				continue
			}
			ref := reference{Commit: string(oid), ObjectID: string(oid), SymbolicTarget: string(advertised.Get([]byte("s/" + name)))}
			if peeled := advertised.Get([]byte("o/" + name + "^{}")); peeled != nil {
				ref.Commit = string(peeled)
			}
			data, err := marshal(ref)
			if err != nil {
				return err
			}
			if err = refs.Put([]byte("r/"+name), data); err != nil {
				return err
			}
			if ref.Commit != ref.ObjectID {
				if err = refs.Put([]byte("a/"+ref.ObjectID), data); err != nil {
					return err
				}
			}
		}
		if selected == "" || selected == "@" {
			selected = "HEAD"
		}
		if len(selected) == 40 && isHexRevision(selected) {
			sha = strings.ToLower(selected)
		} else {
			for _, name := range []string{"refs/heads/" + selected, selected, "refs/" + selected, "refs/tags/" + selected} {
				if data := refs.Get([]byte("r/" + name)); data != nil {
					var ref reference
					if err := unmarshal(data, &ref); err != nil {
						return err
					}
					sha = ref.Commit
					break
				}
			}
		}
		if sha == "" {
			return fmt.Errorf("revision %q: %w", selected, store.ErrNotFound)
		}
		return p.publishReferences(ctx, refs)
	})
	return sha, err
}

// publishReferences replaces the reference index with exactly the rows in refs,
// then publishes its root through a separate bucket in the same staging tx.
func (p *Progressive) publishReferences(ctx context.Context, refs *bolt.Bucket) error {
	idx := &index{store: p.store, cache: p.cache, containers: true}
	root, err := idx.update(ctx, refs)
	if err != nil {
		return err
	}
	data, err := marshal(root)
	if err != nil {
		return err
	}
	head, err := refs.Tx().CreateBucket([]byte("refs-root"))
	if err != nil {
		return err
	}
	if err = head.Put([]byte("refs-root"), data); err != nil {
		return err
	}
	return p.publish(ctx, head)
}
