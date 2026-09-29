package githubfs

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gyit/internal/control"
	"gyit/internal/controlcli"
	pb "gyit/internal/gen/gyit/control/v1"

	"google.golang.org/protobuf/proto"
)

func TestProgressiveUpdateAndLog(t *testing.T) { testProgressiveUpdateAndLog(t, "") }

func TestProgressiveGCSUpdateAndLog(t *testing.T) {
	root := os.Getenv("GYIT_TEST_GCS_PREFIX")
	if root == "" {
		t.Skip("set GYIT_TEST_GCS_PREFIX to an isolated disposable GCS prefix")
	}
	testProgressiveUpdateAndLog(t, root)
}
func testProgressiveUpdateAndLog(t *testing.T, storeRoot string) {
	opts, first, second := fixture(t)

	opts.StoreRoot = storeRoot
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", remote}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("config", "uploadpack.allowFilter", "true")
	git("config", "uploadpack.allowAnySHA1InWant", "true")
	git("update-ref", "refs/tags/release", first)
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	endpoint := func(path string) control.Client {
		t.Helper()
		data, err := f.Endpoint(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		var ep pb.MountEndpoint
		if err = proto.Unmarshal(data, &ep); err != nil {
			t.Fatal(err)
		}
		return control.Client{Endpoint: ep.Socket}
	}
	paths := []string{"acme/project", "acme/project@main", "acme/project@release", "acme/project@" + first}
	clients := make([]control.Client, len(paths))
	for i, p := range paths {
		clients[i] = endpoint(p)
	}
	checkLog := func(client control.Client, sha string, extra ...string) {
		t.Helper()
		var out, stderr bytes.Buffer
		args := append([]string{"log", "--socket", client.Endpoint}, extra...)
		if err := controlcli.Run(t.Context(), args, &out, &stderr); err != nil {
			t.Fatal(err)
		}
		nativeExtra := extra
		if len(extra) == 1 && extra[0] == "dir/hello" {
			nativeExtra = []string{"--", extra[0]}
		}
		native := append([]string{"-C", remote, "log", "--format=medium", "--no-color", "--no-decorate", "-n", "20", sha}, nativeExtra...)
		want, err := exec.Command("git", native...).Output()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("log %v differs\n%s\nwant\n%s", extra, out.Bytes(), want)
		}
	}
	checkLog(clients[0], first)
	generation := f.Generation(paths[0])
	git("update-ref", "refs/heads/main", second)
	git("update-ref", "refs/tags/release", second)
	for i, client := range clients {
		before, err := client.Status(t.Context())
		if err != nil || before.Sha != first {
			t.Fatalf("update happened without request: %v %v", before, err)
		}
		next, err := client.Update(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		want := second
		content := "second revision\n"
		if i == 3 {
			want = first
			content = "first revision\n"
		}
		if next.Sha != want {
			t.Fatalf("%s: %s != %s", paths[i], next.Sha, want)
		}
		if got := read(t, f, paths[i]+"/dir/hello"); got != content {
			t.Fatalf("stale read %q", got)
		}
		checkLog(client, want)
		checkLog(client, want, "--", "dir/hello")
		checkLog(client, want, "dir/hello")
		checkLog(client, want, "--", ":(glob)dir/**")
		checkLog(client, want, "--follow", "--", "dir/hello")
		same := f.Generation(paths[i])
		if _, err := client.Update(t.Context()); err != nil {
			t.Fatal(err)
		}
		if f.Generation(paths[i]) != same {
			t.Fatal("unchanged update invalidates generation")
		}
	}
	if f.Generation(paths[0]) != generation+1 {
		t.Fatal("update failed to advance generation")
	}
	// File/revision disambiguation must use the advertised refs, even offline.
	var expected bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"log", "--socket", clients[0].Endpoint, "--", "dir/hello"}, &expected, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	// Failed refresh preserves the current checkout, and a pinned SHA needs no network.
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	var offline bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"log", "--socket", clients[0].Endpoint, "dir/hello"}, &offline, &bytes.Buffer{}); err != nil {
		t.Fatalf("file log contacted unavailable upstream: %v", err)
	}
	if !bytes.Equal(offline.Bytes(), expected.Bytes()) {
		t.Fatal("offline file log differs")
	}
	if _, err := clients[0].Update(t.Context()); err == nil {
		t.Fatal("missing remote accepted")
	}
	got, err := clients[0].Status(t.Context())
	if err != nil || got.Sha != second {
		t.Fatal("failed update changed checkout", got, err)
	}
	if _, err := clients[3].Update(t.Context()); err != nil {
		t.Fatal("pinned SHA contacted remote", err)
	}
}

func TestAdvertisedRefsPreserveOfflineLogAmbiguity(t *testing.T) {
	opts, first, _ := fixture(t)
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	for _, args := range [][]string{
		{"update-ref", "refs/heads/dir/hello", first},
		{"-c", "user.name=Test", "-c", "user.email=test@example.com", "tag", "-a", "annotated", "-m", "release", first},
	} {
		if out, err := exec.Command("git", append([]string{"-C", remote}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, out)
		}
	}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	data, err := f.Endpoint(t.Context(), "acme/project")
	if err != nil {
		t.Fatal(err)
	}
	var ep pb.MountEndpoint
	if err = proto.Unmarshal(data, &ep); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		var output, stderr bytes.Buffer
		err := controlcli.Run(t.Context(), append([]string{"log", "--socket", ep.Socket}, args...), &output, &stderr)
		return output.String(), err
	}
	// Wait for the complete file index, then make upstream unavailable.
	want, err := run("--", "dir/hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	if _, err := run("dir/hello"); err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("branch/file collision must remain ambiguous offline: %v", err)
	}
	for _, revision := range []string{"main", "annotated", "refs/heads/dir/hello"} {
		got, err := run(revision, "--", "dir/hello")
		if err != nil || got != want {
			t.Fatalf("offline %s: %v\n%s\nwant %s", revision, err, got, want)
		}
	}
}

// Missing file contents must not prevent metadata-only history queries. This
// makes snapshot acquisition fail while blobless commit/tree acquisition works.
func TestFileHistoryIndependentOfSnapshotContents(t *testing.T) {
	opts, first, _ := fixture(t)
	remote := filepath.Join(strings.TrimPrefix(opts.RemoteBase, "file://"), "acme", "project.git")
	for _, args := range [][]string{{"config", "uploadpack.allowFilter", "true"}, {"config", "uploadpack.allowAnySHA1InWant", "true"}} {
		if out, err := exec.Command("git", append([]string{"-C", remote}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git: %v %s", err, out)
		}
	}
	oid, err := exec.Command("git", "-C", remote, "rev-parse", first+":dir/hello").Output()
	if err != nil {
		t.Fatal(err)
	}
	blob := strings.TrimSpace(string(oid))
	if err := os.Remove(filepath.Join(remote, "objects", blob[:2], blob[2:])); err != nil {
		t.Fatal(err)
	}
	f, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	data, err := f.Endpoint(ctx, "acme/project")
	if err != nil {
		t.Fatal(err)
	}
	var ep pb.MountEndpoint
	if err := proto.Unmarshal(data, &ep); err != nil {
		t.Fatal(err)
	}
	var got []string
	err = (control.Client{Endpoint: ep.Socket}).LogPaths(ctx, &pb.LogRequest{MaxCount: 10, Paths: [][]byte{[]byte("dir/hello")}, FullCommitIds: true}, func(e *pb.LogEntry) error { got = append(got, e.Sha); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != first {
		t.Fatalf("file history = %v, want %s", got, first)
	}
}
