package repo

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"gat/internal/store"
)

func TestCorrectnessHarnessTinyPersistentStore(t *testing.T) {
	f, _ := makeAllLocalCommitFixture(t)
	population := map[string]correctnessPopulation{}
	for _, obj := range f.local {
		v := population[obj.kind]
		v.Objects++
		v.RawBytes += obj.size
		population[obj.kind] = v
	}
	owned := t.TempDir()
	target, work := filepath.Join(owned, "store"), filepath.Join(owned, "work")
	if e := os.Mkdir(target, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(work, 0700); e != nil {
		t.Fatal(e)
	}
	c := correctnessConfig{Source: f.source, Pack: f.pack, Head: f.tip, Store: target, Work: work, Report: filepath.Join(owned, "import.json"), Population: population, Workers: 3, Revisions: []string{f.tip}, Paths: []string{"fast.txt", "empty", "large", "dir"}}
	correctnessImport(t, c)
	data, e := os.ReadFile(c.Report)
	if e != nil {
		t.Fatal(e)
	}
	var report map[string]any
	if e = json.Unmarshal(data, &report); e != nil {
		t.Fatal(e)
	}
	if report["published"] != true || report["correctness"] != true || report["scratch_empty"] != true || report["source_unchanged"] != true || report["source_refs_unchanged"] != true {
		t.Fatal("complete import report", report)
	}
	backend, e := store.NewLocal(target)
	if e != nil {
		t.Fatal(e)
	}
	head, token, e := backend.Get(t.Context(), "HEAD", 0, -1)
	if e != nil {
		t.Fatal(e)
	}
	c.Report = filepath.Join(owned, "verify.json")
	correctnessVerify(t, c)
	data, e = os.ReadFile(c.Report)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &report); e != nil {
		t.Fatal(e)
	}
	if report["correctness"] != true || report["head_unchanged"] != true || report["source_unchanged"] != true {
		t.Fatal("complete verification report", report)
	}
	after, afterToken, e := backend.Get(t.Context(), "HEAD", 0, -1)
	if e != nil || token != afterToken || !bytes.Equal(head, after) {
		t.Fatal("verification altered persisted publication", e)
	}
	if files, e := os.ReadDir(work); e != nil || len(files) != 0 {
		t.Fatal("scratch remains", files, e)
	}
	// A failed independent check is a failing child test, not a passing repro.
	// Its report must retain the failure while the already published store stays
	// available for another verification run.
	c.Paths = []string{"required-file-that-does-not-exist"}
	c.Report = filepath.Join(owned, "failed-verify.json")
	config, e := json.Marshal(c)
	if e != nil {
		t.Fatal(e)
	}
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestCorrectnessHarnessFailureChild$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "GAT_CORRECTNESS_FAILURE_CHILD="+string(config))
	output, e := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(e, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("incorrect expected child outcome: %v: %s", e, output)
	}
	data, e = os.ReadFile(c.Report)
	if e != nil {
		t.Fatal(e)
	}
	report = nil
	if e = json.Unmarshal(data, &report); e != nil {
		t.Fatal(e)
	}
	if report["correctness"] != false || report["test_failed"] != true || report["head_unchanged"] != true || report["store_preserved"] != true {
		t.Fatal("failed verification did not retain evidence", report)
	}
	after, afterToken, e = backend.Get(t.Context(), "HEAD", 0, -1)
	if e != nil || afterToken != token || !bytes.Equal(head, after) {
		t.Fatal("failed verification altered store", e)
	}
}

func TestCorrectnessHarnessFailureChild(t *testing.T) {
	raw := os.Getenv("GAT_CORRECTNESS_FAILURE_CHILD")
	if raw == "" {
		t.Skip("tiny intended-failure child only")
	}
	var c correctnessConfig
	if e := json.Unmarshal([]byte(raw), &c); e != nil {
		t.Fatal(e)
	}
	if c.FullLinux || filepath.Base(c.Source) == "linux-repo.git" {
		t.Fatal("failure child requires an owned tiny fixture")
	}
	correctnessVerify(t, c)
}

func TestCorrectnessHarnessAllRefsCommitOnlyTips(t *testing.T) {
	f := makePackMetadataFixture(t)
	command(t, f.source, "update-ref", "refs/tags/data-only", f.orphanBlob)
	command(t, f.source, "update-ref", "refs/tags/tree-only", f.orphanTree)
	want := correctnessExpectedRefs(t, t.Context(), f.source, f.tip)
	found := map[string]string{}
	for _, ref := range want.Refs {
		found[ref.Name] = ref.Commit
	}
	if want.NonCommitRefs != 2 || found["refs/tags/data-only"] != f.orphanBlob || found["refs/tags/tree-only"] != f.orphanTree {
		t.Fatal("non-commit refs missing from independent complete oracle", want)
	}
	for _, tip := range want.Tips {
		if tip == f.orphanBlob || tip == f.orphanTree {
			t.Fatal("non-commit ref entered history tips")
		}
	}
}
