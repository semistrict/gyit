package repo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gat/internal/store"
)

type correctnessPopulation struct{ Objects, RawBytes int64 }

// correctnessFixture is an independently saved Git inventory. Its source paths
// are explicit inputs, so the same ordinary test binary can verify retained
// stores without importing them again.
type correctnessFixture struct {
	Source     string                           `json:"source"`
	Head       string                           `json:"head"`
	Pack       string                           `json:"pack"`
	Facts      string                           `json:"facts"`
	FactsSHA   string                           `json:"facts_sha256"`
	FactsBytes int64                            `json:"facts_bytes"`
	Population map[string]correctnessPopulation `json:"population"`
}

func correctnessReadFixture(t *testing.T) correctnessFixture {
	t.Helper()
	path := os.Getenv("GAT_CORRECTNESS_FIXTURE")
	if !filepath.IsAbs(path) {
		t.Fatal("absolute GAT_CORRECTNESS_FIXTURE is required; run scripts/verify_linux.py prepare")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture correctnessFixture
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(fixture.Source) || !filepath.IsAbs(fixture.Pack) || !filepath.IsAbs(fixture.Facts) || len(fixture.Head) != 40 || len(fixture.FactsSHA) != 64 || fixture.FactsBytes <= 0 || fixture.Population["commit"].Objects <= 0 {
		t.Fatal("incomplete independent fixture manifest")
	}
	return fixture
}

type correctnessConfig struct {
	Source, Pack, Head, Store, Work, Report, Facts, FactsSHA string
	FactsBytes                                               int64
	Population                                               map[string]correctnessPopulation
	Revisions, Paths                                         []string
	Workers                                                  int
	FullLinux                                                bool
}
type correctnessEvent struct {
	Name            string
	Count, UnixNano int64
}
type correctnessWrite struct{ Objects, Bytes int64 }
type correctnessStore struct {
	store.Store
	mu           sync.Mutex
	Writes       map[string]correctnessWrite
	HeadAttempts int
	HeadToken    string
}

func (s *correctnessStore) Put(ctx context.Context, key string, b []byte, token string) error {
	if key == "HEAD" {
		s.mu.Lock()
		s.HeadAttempts++
		s.HeadToken = token
		s.mu.Unlock()
	}
	if err := s.Store.Put(ctx, key, b, token); err != nil {
		return err
	}
	category, _, _ := strings.Cut(key, "/")
	if category == "index" {
		p := strings.TrimPrefix(key, "index/")
		category += "/" + strings.SplitN(p, "-", 2)[0]
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Writes == nil {
		s.Writes = map[string]correctnessWrite{}
	}
	v := s.Writes[category]
	v.Objects++
	v.Bytes += int64(len(b))
	s.Writes[category] = v
	return nil
}
func correctnessCapture(t *testing.T) func() (map[string]int64, []correctnessEvent) {
	t.Helper()
	var mu sync.Mutex
	var events []correctnessEvent
	deferredTraceHook = func(name string, count int64) {
		mu.Lock()
		events = append(events, correctnessEvent{name, count, time.Now().UnixNano()})
		mu.Unlock()
	}
	t.Cleanup(func() { deferredTraceHook = nil })
	return func() (map[string]int64, []correctnessEvent) {
		mu.Lock()
		defer mu.Unlock()
		m := map[string]int64{}
		for _, e := range events {
			if _, exists := m[e.Name]; exists {
				t.Fatalf("duplicate import trace %s", e.Name)
			}
			m[e.Name] = e.Count
		}
		return m, append([]correctnessEvent(nil), events...)
	}
}
func correctnessReport(t *testing.T, path string, r map[string]any) {
	t.Helper()
	r["test_failed"] = t.Failed()
	if t.Failed() {
		r["correctness"] = false
	}
	b, e := json.MarshalIndent(r, "", "  ")
	if e == nil {
		e = os.WriteFile(path, append(b, '\n'), 0600)
	}
	if e != nil {
		t.Error("write retained report:", e)
	}
}
func correctnessEmpty(t *testing.T, path string) {
	t.Helper()
	fi, err := os.Lstat(path)
	if err != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("owned real directory required: %s: %v", path, err)
	}
	entries, e := os.ReadDir(path)
	if e != nil || len(entries) != 0 {
		t.Fatalf("fresh owned directory required: %s: %v entries%d", path, e, len(entries))
	}
}
func correctnessFileIdentity(fi os.FileInfo) map[string]int64 {
	m := map[string]int64{"size": fi.Size(), "mtime_ns": fi.ModTime().UnixNano(), "mode": int64(fi.Mode())}
	v := reflect.ValueOf(fi.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	if v.Kind() == reflect.Struct {
		for _, n := range []string{"Dev", "Ino"} {
			f := v.FieldByName(n)
			if f.IsValid() {
				if f.Kind() >= reflect.Uint && f.Kind() <= reflect.Uint64 {
					m[n] = int64(f.Uint())
				} else if f.Kind() >= reflect.Int && f.Kind() <= reflect.Int64 {
					m[n] = f.Int()
				}
			}
		}
		for _, n := range []string{"Ctim", "Ctimespec"} {
			f := v.FieldByName(n)
			if f.IsValid() {
				m["ctime_ns"] = f.FieldByName("Sec").Int()*1e9 + f.FieldByName("Nsec").Int()
				break
			}
		}
	}
	return m
}
func correctnessSourceGuards(t *testing.T, c correctnessConfig) map[string]any {
	t.Helper()
	result := map[string]any{}
	var packHash []byte
	for _, x := range []struct {
		suffix     string
		head, tail int64
	}{{".pack", 12, 20}, {".idx", 1032, 40}, {".rev", 12, 40}} {
		path := c.Pack + x.suffix
		f, e := os.Open(path)
		if x.suffix == ".rev" && os.IsNotExist(e) {
			result[x.suffix] = map[string]bool{"absent": true}
			continue
		}
		if e != nil {
			t.Fatal(e)
		}
		before, e := f.Stat()
		if e != nil {
			f.Close()
			t.Fatal(e)
		}
		head, tail := make([]byte, x.head), make([]byte, x.tail)
		_, e = f.ReadAt(head, 0)
		if e == nil {
			_, e = f.ReadAt(tail, before.Size()-x.tail)
		}
		after, se := f.Stat()
		ce := f.Close()
		if e != nil || se != nil || ce != nil {
			t.Fatal(e, se, ce)
		}
		if !reflect.DeepEqual(correctnessFileIdentity(before), correctnessFileIdentity(after)) {
			t.Fatal("source changed during guards", path)
		}
		if x.suffix == ".pack" {
			if string(head[:4]) != "PACK" || binary.BigEndian.Uint32(head[4:8]) != 2 {
				t.Fatal("source pack header")
			}
			var count int64
			for _, p := range c.Population {
				count += p.Objects
			}
			if int64(binary.BigEndian.Uint32(head[8:])) != count {
				t.Fatal("source population differs")
			}
			packHash = bytes.Clone(tail)
			if hex.EncodeToString(tail) != strings.TrimPrefix(filepath.Base(c.Pack), "pack-") {
				t.Fatal("source pack filename differs from its trailer")
			}
		} else if !bytes.Equal(tail[:20], packHash) {
			t.Fatal("source index/reverse-index pack checksum differs", path)
		}
		result[x.suffix] = map[string]any{"identity": correctnessFileIdentity(before), "header": hex.EncodeToString(head), "trailer": hex.EncodeToString(tail)}
	}
	if c.Facts != "" {
		fi, e := os.Stat(c.Facts)
		if e != nil {
			t.Fatal(e)
		}
		result["facts"] = correctnessFileIdentity(fi)
	}
	return result
}
func correctnessFacts(t *testing.T, ctx context.Context, c correctnessConfig) map[string]any {
	t.Helper()
	if c.Facts == "" {
		return map[string]any{"source": "tiny independent Git inventory"}
	}
	start := time.Now()
	f, e := os.Open(c.Facts)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	h := sha256.New()
	buf := make([]byte, 1<<20)
	var n int64
	for {
		if e = ctx.Err(); e != nil {
			t.Fatal(e)
		}
		r, re := f.Read(buf)
		if r > 0 {
			h.Write(buf[:r])
			n += int64(r)
		}
		if re == io.EOF {
			break
		}
		if re != nil {
			t.Fatal(re)
		}
	}
	digest := hex.EncodeToString(h.Sum(nil))
	if n != c.FactsBytes || digest != c.FactsSHA {
		t.Fatal("saved independent facts mismatch", n, digest)
	}
	return map[string]any{"bytes": n, "sha256": digest, "seconds": time.Since(start).Seconds(), "population_scope": "all original source-local objects; tag bodies are excluded from catalog import"}
}
func correctnessPhysicalStore(ctx context.Context, path string) (int64, int64, error) {
	var files, size int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("store contains symlink")
		}
		if !d.IsDir() {
			fi, e := d.Info()
			if e != nil {
				return e
			}
			files++
			size += fi.Size()
		}
		return nil
	})
	return files, size, err
}
func correctnessLinuxConfig(t *testing.T, verify bool) correctnessConfig {
	t.Helper()
	fixture := correctnessReadFixture(t)
	c := correctnessConfig{Source: os.Getenv("GAT_CORRECTNESS_SOURCE"), Store: os.Getenv("GAT_CORRECTNESS_STORE"), Work: os.Getenv("GAT_CORRECTNESS_WORK"), Report: os.Getenv("GAT_CORRECTNESS_IMPORT_REPORT"), Head: fixture.Head, Pack: fixture.Pack, Facts: fixture.Facts, FactsSHA: fixture.FactsSHA, FactsBytes: fixture.FactsBytes, Population: fixture.Population, Workers: 0, FullLinux: true}
	if verify {
		c.Report = os.Getenv("GAT_CORRECTNESS_VERIFY_REPORT")
	}
	if c.Source != fixture.Source {
		t.Fatal("source differs from the independent fixture")
	}
	for _, path := range []string{c.Source, c.Store, c.Report} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			t.Fatal("absolute clean source, store, and report paths are required")
		}
	}
	if !verify && (!filepath.IsAbs(c.Work) || filepath.Clean(c.Work) != c.Work) {
		t.Fatal("absolute clean work directory is required")
	}
	if c.Store == c.Source || strings.HasPrefix(c.Store, c.Source+string(os.PathSeparator)) || strings.HasPrefix(c.Source, c.Store+string(os.PathSeparator)) {
		t.Fatal("source and store must be separate directories")
	}
	c.Revisions = []string{fixture.Head, "v4.4^{commit}"}
	c.Paths = []string{"README", "Makefile", "COPYING", "init/main.c", "include/linux"}
	return c
}

func correctnessImport(t *testing.T, c correctnessConfig) {
	t.Helper()
	r := map[string]any{"scope": "complete source-local commit/tree/blob catalog with reachable history and source refs; normal atomic publication", "published": false, "correctness": false, "store": c.Store, "source": c.Source, "store_preserved": true, "operational_context_seconds": 300}
	defer correctnessReport(t, c.Report, r)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()
	correctnessEmpty(t, c.Store)
	correctnessEmpty(t, c.Work)
	pre := time.Now()
	guards := correctnessSourceGuards(t, c)
	r["source_before"] = guards
	r["facts"] = correctnessFacts(t, ctx, c)
	refs := correctnessExpectedRefs(t, ctx, c.Source, c.Head)
	r["source_refs"] = refs
	r["expected_population"] = c.Population
	r["preflight_seconds"] = time.Since(pre).Seconds()
	local, e := store.NewLocal(c.Store)
	if e != nil {
		t.Fatal(e)
	}
	backend := &correctnessStore{Store: local}
	capture := correctnessCapture(t)
	defer func() {
		postctx, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		m, token, err := readHead(postctx, local)
		if err == nil {
			r["published"] = true
			r["manifest"] = m
			r["head_token"] = token
		} else {
			r["head_error"] = err.Error()
		}
		n, b, err := correctnessPhysicalStore(postctx, c.Store)
		r["physical_store_files"], r["physical_store_bytes"] = n, b
		if err != nil {
			r["physical_store_error"] = err.Error()
		}
		entries, err := os.ReadDir(c.Work)
		r["scratch_empty"] = err == nil && len(entries) == 0
		if err != nil {
			r["scratch_error"] = err.Error()
		}
		backend.mu.Lock()
		r["writes"], r["head_attempts"], r["publication_token"] = backend.Writes, backend.HeadAttempts, backend.HeadToken
		backend.mu.Unlock()
		counters, events := capture()
		r["counters"], r["events"] = counters, events
		after := correctnessSourceGuards(t, c)
		r["source_after"] = after
		r["source_unchanged"] = reflect.DeepEqual(guards, after)
		if !reflect.DeepEqual(guards, after) {
			t.Error("source changed during import")
		}
		if postctx.Err() == nil {
			postRefs := correctnessExpectedRefs(t, postctx, c.Source, c.Head)
			equal := reflect.DeepEqual(refs.Refs, postRefs.Refs) && reflect.DeepEqual(refs.Tips, postRefs.Tips)
			r["source_refs_unchanged"] = equal
			if !equal {
				t.Error("source refs changed during import")
			}
		}
	}()
	start := time.Now()
	var stats Stats
	var err error
	opt := ImportOptions{Repo: c.Source, TempDir: c.Work, CompressionWorkers: c.Workers}
	if c.FullLinux {
		stats, err = Import(ctx, backend, opt)
	} else {
		stats, err = importWithMetadataThreshold(ctx, backend, opt, 0)
	}
	r["import_seconds"], r["import_over_1s"], r["stats"] = time.Since(start).Seconds(), time.Since(start) > time.Second, stats
	if err != nil {
		r["import_error"] = err.Error()
		t.Fatal(err)
	}
	var objects, raw int64
	for kind, p := range c.Population {
		if kind != "tag" {
			objects += p.Objects
			raw += p.RawBytes
		}
	}
	if stats.Phase != "done" || stats.Objects != objects || stats.Bytes != raw || stats.Blobs != c.Population["blob"].Objects {
		t.Fatal("complete catalog populations differ", stats, c.Population)
	}
	m, token, e := readHead(ctx, local)
	if e != nil {
		t.Fatal(e)
	}
	if m.Version != formatVersion || m.Root == (pageRef{}) || m.Blobs == (pageRef{}) || m.History == (pageRef{}) || m.Refs == (pageRef{}) || !m.RevisionGraph || !m.CommitMetadata || token != stats.Generation {
		t.Fatal("published roots/generation incomplete", m, token, stats.Generation)
	}
	head, _, e := local.Get(ctx, "HEAD", 0, -1)
	if e != nil {
		t.Fatal(e)
	}
	generation, _, e := local.Get(ctx, "generations/"+stats.Generation, 0, -1)
	if e != nil || !bytes.Equal(head, generation) {
		t.Fatal("immutable generation differs from HEAD", e)
	}
	r["refs_check"] = correctnessCheckRefs(t, ctx, local, m, refs)
	counts, _ := capture()
	for key, want := range map[string]int64{"ordered_selected_objects": objects, "pack_metadata_objects": objects + c.Population["tag"].Objects, "all_local_commit_rows": c.Population["commit"].Objects, "whole_database_tree_rows": c.Population["tree"].Objects, "whole_database_blob_rows": c.Population["blob"].Objects, "all_local_parent_records_staged": c.Population["commit"].Objects, "archive_plan_reused": 1} {
		if got, ok := counts[key]; !ok || got != want {
			t.Fatalf("full import trace%s=%d present%t want%d", key, got, ok, want)
		}
	}
	if backend.HeadAttempts != 1 || backend.HeadToken != "*" {
		t.Fatal("fresh publication did not use one atomic create", backend.HeadAttempts, backend.HeadToken)
	}
	correctnessEmpty(t, c.Work)
	r["correctness"] = true
}

func TestCorrectnessImportLinux(t *testing.T) {
	if os.Getenv("GAT_RUN_CORRECTNESS_IMPORT") != "1" {
		t.Skip("explicit full-source correctness import opt-in required")
	}
	correctnessImport(t, correctnessLinuxConfig(t, false))
}

func correctnessParseCommit(t *testing.T, raw []byte) (string, []string, commitInfo) {
	t.Helper()
	headers, message, ok := bytes.Cut(raw, []byte("\n\n"))
	if !ok {
		t.Fatal("oracle commit lacks separator")
	}
	var tree string
	var parents []string
	var info commitInfo
	for _, line := range bytes.Split(headers, []byte{'\n'}) {
		key, value, ok := bytes.Cut(line, []byte{' '})
		if !ok {
			continue
		}
		switch string(key) {
		case "tree":
			tree = string(value)
		case "parent":
			parents = append(parents, string(value))
		case "author", "committer":
			zoneAt := bytes.LastIndexByte(value, ' ')
			if zoneAt < 0 {
				t.Fatal("oracle identity zone")
			}
			stampAt := bytes.LastIndexByte(value[:zoneAt], ' ')
			if stampAt < 0 {
				t.Fatal("oracle identity timestamp")
			}
			stamp, e := strconv.ParseInt(string(value[stampAt+1:zoneAt]), 10, 64)
			if e != nil {
				t.Fatal(e)
			}
			zone := string(value[zoneAt+1:])
			if len(zone) != 5 {
				t.Fatal("oracle timezone")
			}
			hh, e1 := strconv.Atoi(zone[1:3])
			mm, e2 := strconv.Atoi(zone[3:])
			if e1 != nil || e2 != nil {
				t.Fatal("oracle timezone digits")
			}
			offset := int32(hh*60 + mm)
			if zone[0] == '-' {
				offset = -offset
			}
			if string(key) == "committer" {
				info.CommitTime, info.CommitterOffset, info.HasCommitter = stamp, offset, true
				info.Committer = bytes.Clone(value[:min(stampAt, maxLogAuthor)])
				info.CommitterTruncated = stampAt > maxLogAuthor
				continue
			}
			info.Author = bytes.Clone(value[:min(stampAt, maxLogAuthor)])
			info.AuthorTruncated = stampAt > maxLogAuthor
			info.AuthorTime = stamp
			info.AuthorOffset = offset
		}
	}
	info.Message = bytes.Clone(message[:min(len(message), maxLogMessage)])
	info.MessageTruncated = len(message) > maxLogMessage
	return tree, parents, info
}
func correctnessVerify(t *testing.T, c correctnessConfig) {
	t.Helper()
	report := map[string]any{"scope": "persisted complete refs and commit-only tips, reachable history count/boundaries, selected exact commit/tree/blob comparisons against independent Git", "correctness": false, "store": c.Store, "store_preserved": true, "cache_bytes": 32 << 20, "operational_context_seconds": 180}
	defer correctnessReport(t, c.Report, report)
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	guards := correctnessSourceGuards(t, c)
	report["source_before"] = guards
	fi, e := os.Lstat(c.Store)
	if e != nil || !fi.IsDir() || fi.Mode()&os.ModeSymlink != 0 {
		t.Fatal("published real store directory required", e)
	}
	backend, e := store.NewLocal(c.Store)
	if e != nil {
		t.Fatal(e)
	}
	m, headToken, e := readHead(ctx, backend)
	if e != nil {
		t.Fatal(e)
	}
	if os.Getenv("GAT_CORRECTNESS_NEW_IMPORT") == "1" && m.Version != formatVersion {
		t.Fatalf("new import wrote format %d; want current format %d", m.Version, formatVersion)
	}
	report["manifest"] = m
	report["head_token"] = headToken
	defer func() {
		after := correctnessSourceGuards(t, c)
		report["source_after"] = after
		report["source_unchanged"] = reflect.DeepEqual(guards, after)
		if !reflect.DeepEqual(guards, after) {
			t.Error("source changed during verification")
		}
		check, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_, token, e := readHead(check, backend)
		if e != nil || token != headToken {
			t.Error("verification changed HEAD", e)
		}
		report["head_unchanged"] = e == nil && token == headToken
	}()
	refs := correctnessExpectedRefs(t, ctx, c.Source, c.Head)
	report["refs_check"] = correctnessCheckRefs(t, ctx, backend, m, refs)
	start := time.Now()
	countRaw := correctnessGit(t, ctx, c.Source, "", "rev-list", "--count", "--all")
	count, e := strconv.ParseUint(strings.TrimSpace(string(countRaw)), 10, 64)
	if e != nil || count != m.HistoryCount {
		t.Fatal("complete reachable history count differs", count, m.HistoryCount, e)
	}
	rows := []map[string]any{{"operation": "independent_reachable_commit_count", "seconds": time.Since(start).Seconds(), "over_1s": time.Since(start) > time.Second, "count": count, "ok": true}}
	report["operations"] = &rows
	r, e := New(backend, 32<<20)
	if e != nil {
		t.Fatal(e)
	}
	s, e := r.Open(ctx, c.Head)
	if e != nil {
		t.Fatal(e)
	}
	commitIDs := map[string]bool{c.Head: true}
	cursor := &historyCursor{idx: s.history}
	for _, pos := range []uint64{1, m.HistoryCount} {
		node, e := cursor.get(ctx, pos)
		if e != nil {
			t.Fatal(e)
		}
		oid := hex.EncodeToString(node.Oid)
		commitIDs[oid] = true
		var reverse historyPosition
		if e = s.history.get(ctx, "g/"+oid, &reverse); e != nil || uint64(reverse) != pos {
			t.Fatal("history boundary reverse index differs", e)
		}
		if pos == 1 && len(node.Parents) != 0 {
			t.Fatal("first history node has parents")
		}
	}
	for _, rev := range c.Revisions {
		oid := strings.TrimSpace(string(correctnessGit(t, ctx, c.Source, "", "rev-parse", "--verify", rev)))
		commitIDs[oid] = true
		snap, e := r.Open(ctx, oid)
		if e != nil {
			t.Fatal(e)
		}
		paths := append([]string{""}, c.Paths...)
		for _, path := range paths {
			name := oid
			if path != "" {
				name += ":" + path
			} else {
				name += "^{tree}"
			}
			objectID := strings.TrimSpace(string(correctnessGit(t, ctx, c.Source, "", "rev-parse", "--verify", name)))
			kind := strings.TrimSpace(string(correctnessGit(t, ctx, c.Source, "", "cat-file", "-t", objectID)))
			start = time.Now()
			if kind == "blob" {
				want := correctnessGit(t, ctx, c.Source, "", "cat-file", "blob", objectID)
				got := make([]byte, len(want)+1)
				n, e := snap.ReadAt(ctx, objectID, got, 0)
				if e != io.EOF || n != len(want) || !bytes.Equal(got[:n], want) {
					t.Fatal("exact blob differs", oid, path, n, e)
				}
			} else if kind == "tree" {
				want := correctnessParseEntries(t, correctnessGit(t, ctx, c.Source, "", "ls-tree", "-z", "-l", objectID))
				var got []Entry
				after := ""
				for {
					page, e := snap.ReadDir(ctx, objectID, after, 128)
					if e != nil {
						t.Fatal(e)
					}
					for _, entry := range page {
						entry.RawMode = 0
						got = append(got, entry)
					}
					if len(page) < 128 {
						break
					}
					next := page[len(page)-1].Name
					if next <= after {
						t.Fatal("nonadvancing directory cursor")
					}
					after = next
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatal("directory names/modes/OIDs/sizes differ", oid, path)
				}
			} else {
				t.Fatal("unexpected selected kind", kind)
			}
			rows = append(rows, map[string]any{"operation": "exact_" + kind, "revision": oid, "path": path, "oid": objectID, "seconds_including_git": time.Since(start).Seconds(), "over_1s": time.Since(start) > time.Second, "ok": true})
		}
	}
	for oid := range commitIDs {
		start = time.Now()
		raw := correctnessGit(t, ctx, c.Source, "", "cat-file", "commit", oid)
		tree, parentIDs, info := correctnessParseCommit(t, raw)
		var o object
		var gotInfo commitInfo
		var gotParents parents
		if e = s.idx.get(ctx, "o/"+oid, &o); e != nil {
			t.Fatal(e)
		}
		if e = s.idx.get(ctx, "c/"+oid, &gotInfo); e != nil {
			t.Fatal(e)
		}
		if e = s.idx.get(ctx, "p/"+oid, &gotParents); e != nil {
			t.Fatal(e)
		}
		if o.Kind != "commit" || o.Size != int64(len(raw)) || o.Tree != tree || !reflect.DeepEqual(gotInfo, info) || strings.Join(gotParents.Parents, " ") != strings.Join(parentIDs, " ") {
			t.Fatal("commit metadata/ordered parents differ", oid)
		}
		var position historyPosition
		if e = s.history.get(ctx, "g/"+oid, &position); e != nil {
			t.Fatal(e)
		}
		node, e := cursor.get(ctx, uint64(position))
		if e != nil || hex.EncodeToString(node.Oid) != oid || hex.EncodeToString(node.Tree) != tree || len(node.Parents) != len(parentIDs) {
			t.Fatal("selected history node differs", oid, e)
		}
		for i, pos := range append([]uint64(nil), node.Parents...) {
			p, e := cursor.get(ctx, pos)
			if e != nil || hex.EncodeToString(p.Oid) != parentIDs[i] {
				t.Fatal("history parent order differs", oid, e)
			}
		}
		rows = append(rows, map[string]any{"operation": "exact_commit_and_history", "oid": oid, "seconds_including_git": time.Since(start).Seconds(), "over_1s": time.Since(start) > time.Second, "ok": true})
	}
	report["correctness"] = true
}

func TestCorrectnessVerifyLinux(t *testing.T) {
	if os.Getenv("GAT_RUN_CORRECTNESS_VERIFY") != "1" {
		t.Skip("explicit persisted-store core verification opt-in required")
	}
	correctnessVerify(t, correctnessLinuxConfig(t, true))
}
