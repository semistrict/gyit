package repo

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	storagev1 "gat/internal/gen/gat/storage/v1"
	"gat/internal/store"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type catalogPopulation struct{ Objects, Bytes int64 }
type catalogFactsSpec struct {
	SHA256     string
	Bytes      int64
	Population map[string]catalogPopulation
}
type catalogPhase struct {
	Name    string
	Seconds float64
	Over1s  bool
}
type catalogReport struct {
	Completed                                            bool
	Error                                                string
	Seconds                                              float64
	Over1s                                               bool
	FactsPath, FactsSHA256, SortedSHA256                 string
	FactsBytes                                           int64
	Expected, Compared                                   map[string]catalogPopulation
	Pages, Rows, ObjectRows, MetadataGets, MetadataBytes uint64
	MaxDepth                                             int
	HeadToken                                            string
	Root                                                 pageRef
	HeadUnchanged, CleanupVerified                       bool
	TempPath                                             string
	SortProgram, SortVersion                             string
	Phases                                               []catalogPhase
	Limitations                                          []string
}

func TestCorrectnessFullCatalog(t *testing.T) {
	if os.Getenv("GAT_RUN_FULL_CATALOG_CORRECTNESS") != "1" {
		t.Skip("set GAT_RUN_FULL_CATALOG_CORRECTNESS=1 for complete persisted identity verification")
	}
	location, output := os.Getenv("GAT_CORRECTNESS_STORE"), os.Getenv("GAT_CORRECTNESS_CATALOG_REPORT")
	if !filepath.IsAbs(location) || !filepath.IsAbs(output) {
		t.Fatal("absolute GAT_CORRECTNESS_STORE and GAT_CORRECTNESS_CATALOG_REPORT are required")
	}
	if _, err := os.Stat(filepath.Join(location, "HEAD")); err != nil {
		t.Fatal(err)
	}
	fixture := correctnessReadFixture(t)
	facts := fixture.Facts
	if override := os.Getenv("GAT_CORRECTNESS_FACTS"); override != "" && override != facts {
		t.Fatal("facts path differs from the independent fixture")
	}
	spec := catalogFactsSpec{SHA256: fixture.FactsSHA, Bytes: fixture.FactsBytes, Population: map[string]catalogPopulation{}}
	for kind, count := range fixture.Population {
		spec.Population[kind] = catalogPopulation{Objects: count.Objects, Bytes: count.RawBytes}
	}
	backend, err := store.NewLocal(location)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 180*time.Second)
	defer cancel()
	report, runErr := catalogVerify(ctx, backend, facts, os.TempDir(), spec)
	b, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.WriteFile(output, append(b, '\n'), 0600)
	}
	if err != nil {
		t.Error("write catalog report:", err)
	}
	for _, p := range report.Phases {
		if p.Over1s {
			t.Logf("over 1 second: %s %.3f s", p.Name, p.Seconds)
		}
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
}

func catalogVerify(ctx context.Context, backend store.Store, facts, tempBase string, spec catalogFactsSpec) (report catalogReport, runErr error) {
	started := time.Now()
	report.FactsPath, report.Expected = facts, spec.Population
	report.Compared = make(map[string]catalogPopulation)
	report.Limitations = []string{
		"Complete o/ identity, type, and size comparison against previously saved, independently hashed Git metadata; tags are intentionally absent from this catalog.",
		"All main-index pages and routing bounds are checked; non-object values, direct blob recipes, commit display fields, history contents, and payload bytes are not exhaustively verified here.",
		"Verifier memory uses a separate 32 MiB GNU sort budget plus bounded page decoding; this is not a mounted-reader cache measurement.",
	}
	defer func() {
		report.Seconds = time.Since(started).Seconds()
		report.Over1s = report.Seconds > 1
		report.Completed = runErr == nil
		if runErr != nil {
			report.Error = runErr.Error()
		}
	}()
	readonly := &correctnessReadStore{Store: backend}
	m, token, err := readHead(ctx, readonly)
	if err != nil {
		return report, err
	}
	if m.Root == (pageRef{}) || token == "" {
		return report, fmt.Errorf("persisted main catalog is absent")
	}
	report.HeadToken, report.Root = token, m.Root
	defer func() {
		check, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		latest, latestToken, err := readHead(check, readonly)
		report.HeadUnchanged = err == nil && latestToken == token && latest.Root == m.Root
		if err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("final HEAD read: %w", err))
		} else if !report.HeadUnchanged {
			runErr = errors.Join(runErr, fmt.Errorf("HEAD changed during catalog verification"))
		}
		if readonly.writes.Load() != 0 {
			runErr = errors.Join(runErr, fmt.Errorf("catalog verifier attempted a store write"))
		}
		report.MetadataGets, report.MetadataBytes = readonly.gets.Load(), readonly.bytes.Load()
	}()
	tmp, err := os.MkdirTemp(tempBase, "gat-catalog-verify-")
	if err != nil {
		return report, err
	}
	report.TempPath = tmp
	defer func() {
		err := os.RemoveAll(tmp)
		_, statErr := os.Stat(tmp)
		report.CleanupVerified = err == nil && errors.Is(statErr, os.ErrNotExist)
		if !report.CleanupVerified {
			runErr = errors.Join(runErr, fmt.Errorf("owned catalog scratch cleanup failed: remove=%v stat=%v", err, statErr))
		}
	}()
	phase := time.Now()
	sorted, err := catalogPrepareExpected(ctx, facts, tmp, spec, &report)
	report.Phases = append(report.Phases, catalogPhaseOf("hash facts and independently sort expected identities", phase))
	if err != nil {
		return report, err
	}
	phase = time.Now()
	err = catalogCompare(ctx, &index{store: readonly, root: m.Root}, sorted, &report)
	report.Phases = append(report.Phases, catalogPhaseOf("stream complete main catalog and compare identities", phase))
	return report, err
}

func catalogPhaseOf(name string, start time.Time) catalogPhase {
	seconds := time.Since(start).Seconds()
	return catalogPhase{Name: name, Seconds: seconds, Over1s: seconds > 1}
}

func catalogGNU(ctx context.Context) (string, string, error) {
	for _, name := range []string{"gsort", "sort"} {
		path, err := exec.LookPath(name)
		if err != nil {
			continue
		}
		check, cancel := context.WithTimeout(ctx, 3*time.Second)
		cmd := exec.CommandContext(check, path, "--version")
		var out strings.Builder
		cmd.Stdout = &correctnessLimitedWriter{out: &out, remaining: 4096}
		cmd.Stderr = &correctnessLimitedWriter{out: io.Discard, remaining: 4096}
		cmd.WaitDelay = time.Second
		err = cmd.Run()
		cancel()
		if err == nil && strings.Contains(out.String(), "GNU") {
			version, _, _ := strings.Cut(out.String(), "\n")
			return path, version, nil
		}
	}
	return "", "", fmt.Errorf("GNU sort is required for the independent 32 MiB expected-identity sort")
}

func catalogPrepareExpected(ctx context.Context, facts, tmp string, spec catalogFactsSpec, report *catalogReport) (string, error) {
	sortPath, version, err := catalogGNU(ctx)
	if err != nil {
		return "", err
	}
	report.SortProgram, report.SortVersion = sortPath, version
	f, err := os.Open(facts)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !st.Mode().IsRegular() || st.Size() != spec.Bytes {
		return "", fmt.Errorf("facts file size: got %d, expected %d", st.Size(), spec.Bytes)
	}
	output := filepath.Join(tmp, "expected.sorted")
	out, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", err
	}
	defer out.Close()
	childCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(childCtx, sortPath, "--buffer-size=32M", "--temporary-directory="+tmp, "--parallel=1")
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "LC_ALL=") {
			cmd.Env = append(cmd.Env, e)
		}
	}
	cmd.Env = append(cmd.Env, "LC_ALL=C")
	cmd.Stdout = out
	var stderr strings.Builder
	cmd.Stderr = &correctnessLimitedWriter{out: &stderr, remaining: 65536}
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	if err = cmd.Start(); err != nil {
		stdin.Close()
		return "", err
	}
	w := bufio.NewWriterSize(stdin, 64<<10)
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(f, hash))
	scanner.Buffer(make([]byte, 256), 256)
	population := make(map[string]catalogPopulation)
	var rows int64
	var feedErr error
	for scanner.Scan() {
		if rows&1023 == 0 {
			if feedErr = ctx.Err(); feedErr != nil {
				break
			}
		}
		row, err := catalogParseFact(scanner.Text())
		if err != nil {
			feedErr = fmt.Errorf("facts row %d: %w", rows+1, err)
			break
		}
		p := population[row.kind]
		if row.size > math.MaxInt64-p.Bytes {
			feedErr = fmt.Errorf("facts byte total overflow")
			break
		}
		p.Objects++
		p.Bytes += row.size
		population[row.kind] = p
		rows++
		if row.kind == "tag" {
			continue
		}
		// Canonical rows keep the independently sorted input unambiguous. No -u:
		// a duplicate OID remains present and must fail comparison.
		for _, part := range []string{row.oid, " ", row.kind, " ", strconv.FormatInt(row.size, 10), "\n"} {
			if _, feedErr = w.WriteString(part); feedErr != nil {
				break
			}
		}
		if feedErr != nil {
			break
		}
	}
	if feedErr == nil {
		feedErr = scanner.Err()
	}
	report.FactsBytes = st.Size()
	report.FactsSHA256 = hex.EncodeToString(hash.Sum(nil))
	if feedErr == nil && report.FactsSHA256 != spec.SHA256 {
		feedErr = fmt.Errorf("facts SHA256 mismatch: %s", report.FactsSHA256)
	}
	if feedErr == nil && !reflect.DeepEqual(population, spec.Population) {
		feedErr = fmt.Errorf("facts population mismatch: got %+v, expected %+v", population, spec.Population)
	}
	if feedErr == nil {
		feedErr = w.Flush()
	}
	if feedErr != nil {
		cancel()
	}
	closeErr := stdin.Close()
	waitErr := cmd.Wait() // Always join the only subprocess before scratch cleanup.
	if feedErr != nil {
		return "", feedErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if waitErr != nil {
		return "", fmt.Errorf("GNU sort: %w: %s", waitErr, stderr.String())
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	if err = out.Close(); err != nil {
		return "", err
	}
	return output, nil
}

type catalogFact struct {
	oid, kind string
	size      int64
}

func catalogParseFact(line string) (catalogFact, error) {
	fields := strings.Fields(line)
	if len(fields) != 3 || !catalogOID(fields[0]) {
		return catalogFact{}, fmt.Errorf("invalid object identity row")
	}
	switch fields[1] {
	case "blob", "tree", "commit", "tag":
	default:
		return catalogFact{}, fmt.Errorf("unknown object kind %q", fields[1])
	}
	size, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || size < 0 {
		return catalogFact{}, fmt.Errorf("invalid object size %q", fields[2])
	}
	return catalogFact{fields[0], fields[1], size}, nil
}
func catalogOID(oid string) bool {
	if len(oid) != 40 {
		return false
	}
	for i := range oid {
		if !((oid[i] >= '0' && oid[i] <= '9') || (oid[i] >= 'a' && oid[i] <= 'f')) {
			return false
		}
	}
	return true
}

func catalogCompare(ctx context.Context, idx *index, sorted string, report *catalogReport) error {
	f, err := os.Open(sorted)
	if err != nil {
		return err
	}
	defer f.Close()
	hash := sha256.New()
	scanner := bufio.NewScanner(io.TeeReader(f, hash))
	scanner.Buffer(make([]byte, 256), 256)
	previousExpected := ""
	next := func() (catalogFact, error) {
		if err := ctx.Err(); err != nil {
			return catalogFact{}, err
		}
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return catalogFact{}, err
			}
			return catalogFact{}, io.EOF
		}
		row, err := catalogParseFact(scanner.Text())
		if err != nil {
			return row, err
		}
		if row.kind == "tag" || row.oid <= previousExpected {
			return row, fmt.Errorf("expected identities are not strictly ordered at %s", row.oid)
		}
		previousExpected = row.oid
		return row, nil
	}
	err = catalogWalk(ctx, idx, report, func(key string, value []byte) error {
		if !strings.HasPrefix(key, "o/") {
			return nil
		}
		oid := strings.TrimPrefix(key, "o/")
		if !catalogOID(oid) {
			return fmt.Errorf("invalid stored object identity %q", key)
		}
		want, err := next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("unexpected stored object %s after expected EOF", oid)
		}
		if err != nil {
			return err
		}
		if oid != want.oid {
			return fmt.Errorf("catalog identity mismatch: stored %s, expected %s", oid, want.oid)
		}
		if len(value) > 64<<10 {
			return fmt.Errorf("oversized object metadata at %s", oid)
		}
		var object storagev1.ObjectRecord
		if err = (proto.UnmarshalOptions{RecursionLimit: 8, DiscardUnknown: true}).Unmarshal(value, &object); err != nil {
			return err
		}
		kind, err := decodeKind(object.Kind)
		if err != nil {
			return fmt.Errorf("object %s: %w", oid, err)
		}
		if kind != want.kind || object.Size != want.size {
			return fmt.Errorf("catalog object %s: got %s %d, expected %s %d", oid, kind, object.Size, want.kind, want.size)
		}
		p := report.Compared[kind]
		if object.Size < 0 || object.Size > math.MaxInt64-p.Bytes {
			return fmt.Errorf("stored object byte total overflow")
		}
		p.Objects++
		p.Bytes += object.Size
		report.Compared[kind] = p
		report.ObjectRows++
		return nil
	})
	if err != nil {
		return err
	}
	if extra, err := next(); !errors.Is(err, io.EOF) {
		if err != nil {
			return err
		}
		return fmt.Errorf("missing stored object at expected %s", extra.oid)
	}
	report.SortedSHA256 = hex.EncodeToString(hash.Sum(nil))
	for kind, p := range report.Expected {
		if kind != "tag" && report.Compared[kind] != p {
			return fmt.Errorf("compared %s census differs: got %+v, expected %+v", kind, report.Compared[kind], p)
		}
	}
	return nil
}

// This independent walk visits every leaf, including unrelated namespaces. A
// repeated scan(after) could conceal cross-page duplicates; routing-based prefix
// pruning could conceal misrouted extra identities. Neither is used here.
func catalogWalk(ctx context.Context, idx *index, report *catalogReport, visit func(string, []byte) error) error {
	last := ""
	var walk func(pageRef, int) (string, error)
	walk = func(ref pageRef, depth int) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if depth > 16 || report.Pages >= 1000000 {
			return "", fmt.Errorf("catalog structure exceeds bounded depth or page count")
		}
		report.MaxDepth = max(report.MaxDepth, depth)
		p, err := catalogLoadPage(ctx, idx.store, ref)
		if err != nil {
			return "", err
		}
		report.Pages++
		if len(p.Items) > 0 {
			for _, item := range p.Items {
				if err = ctx.Err(); err != nil {
					return "", err
				}
				if item == nil || item.Key == "" || len(item.Key) > 128 || item.Key <= last {
					return "", fmt.Errorf("main catalog keys are not strictly ordered at %q", item.GetKey())
				}
				last = item.Key
				report.Rows++
				if err = visit(item.Key, item.Value); err != nil {
					return "", err
				}
			}
			return last, nil
		}
		// Copy only the small child references. Do not retain a raw page buffer or
		// an arbitrarily sized protobuf unknown field at each recursion level.
		children := make([]edge, len(p.Children))
		for i, child := range p.Children {
			if child == nil || child.Page == nil || child.MaxKey == "" || len(child.MaxKey) > 128 || len(child.Page.Pack) > 512 || (i > 0 && child.MaxKey <= children[i-1].Max) {
				return "", fmt.Errorf("invalid main catalog routing bounds")
			}
			children[i] = edge{Max: child.MaxKey, ID: decodePageRef(child.Page)}
		}
		p = nil
		for _, child := range children {
			end, err := walk(child.ID, depth+1)
			if err != nil {
				return "", err
			}
			if end != child.Max {
				return "", fmt.Errorf("main catalog separator %q differs from child maximum %q", child.Max, end)
			}
		}
		return last, nil
	}
	_, err := walk(idx.root, 1)
	return err
}

func catalogLoadPage(ctx context.Context, backend store.Store, ref pageRef) (*storagev1.IndexPage, error) {
	if !strings.HasPrefix(ref.Pack, "index/") || len(ref.Pack) > 512 || ref.Offset < 0 || ref.Length <= 0 || ref.Length > indexPackSize || ref.Offset > indexPackSize-ref.Length || len(ref.Hash) != 64 {
		return nil, fmt.Errorf("invalid main catalog page range")
	}
	// Synchronous reads avoid leaving cache-flight goroutines behind on timeout.
	b, _, err := backend.Get(ctx, ref.Pack, ref.Offset, ref.Length)
	if err != nil {
		return nil, err
	}
	if int64(len(b)) != ref.Length || fmt.Sprintf("%x", sha256.Sum256(b)) != ref.Hash {
		return nil, fmt.Errorf("main catalog page checksum mismatch")
	}
	if err = catalogPageShape(b); err != nil {
		return nil, err
	}
	var p storagev1.IndexPage
	if err = (proto.UnmarshalOptions{RecursionLimit: 8, DiscardUnknown: true}).Unmarshal(b, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// Count repeated records before Unmarshal can allocate an attacker-controlled
// number of nested messages from a checksum-valid but malformed page.
func catalogPageShape(b []byte) error {
	items, children := 0, 0
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		b = b[n:]
		if (num != 1 && num != 2) || typ != protowire.BytesType {
			return fmt.Errorf("unexpected main catalog page field")
		}
		value, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return protowire.ParseError(n)
		}
		if num == 1 {
			items++
		} else {
			children++
			if len(value) > 2048 {
				return fmt.Errorf("oversized main catalog routing record")
			}
		}
		if items > fanout || children > fanout || (items > 0 && children > 0) {
			return fmt.Errorf("main catalog page is not one bounded leaf or routing node")
		}
		b = b[n:]
	}
	if items+children == 0 {
		return fmt.Errorf("empty main catalog page")
	}
	return nil
}
