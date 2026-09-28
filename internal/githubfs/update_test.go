package githubfs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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
	// Failed refresh preserves the current checkout, and a pinned SHA needs no network.
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
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
