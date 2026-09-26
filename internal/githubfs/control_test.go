package githubfs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"gyit/internal/control"
	"gyit/internal/controlcli"
	pb "gyit/internal/gen/gyit/control/v1"
)

func TestMountedHistoryControl(t *testing.T) {
	f, opts, first, second := openFixture(t)
	n := Namespace{f}
	path := "github.com/acme/project@feature%2Flogin"
	data, err := n.Endpoint(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := new(pb.MountEndpoint)
	if err := proto.Unmarshal(data, endpoint); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(filepath.Dir(opts.DataDir), "source")
	expected, err := exec.Command("git", "-C", source, "log", "--no-color", "--no-decorate", "--format=medium", second).Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(strings.TrimPrefix(opts.RemoteBase, "file://")); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if err := controlcli.Run(t.Context(), []string{"log", "--socket", endpoint.Socket}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.Bytes(), expected) {
		t.Fatalf("log mismatch:\n%s\nexpected:\n%s", &out, expected)
	}
	client := control.Client{Endpoint: endpoint.Socket}
	if _, err := client.Switch(t.Context(), first); err == nil {
		t.Fatal("pinned mount allowed checkout")
	}
	status, err := client.Status(t.Context())
	if err != nil || status.Sha != second {
		t.Fatalf("pin: %v %v", status, err)
	}
	again, err := n.Endpoint(t.Context(), path)
	if err != nil || !bytes.Equal(again, data) {
		t.Fatal("endpoint changed", err)
	}
	if b, err := n.Endpoint(t.Context(), path+"/dir"); err != nil || b != nil {
		t.Fatal("endpoint on subdirectory", err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(endpoint.Socket); !os.IsNotExist(err) {
		t.Fatalf("socket not cleaned up: %v", err)
	}
}

func TestNativeControlUsesWritableContainerRoot(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("native sandbox layout")
	}
	root := filepath.Join(t.TempDir(), "Data")
	f := &FS{opts: Options{DataDir: filepath.Join(root, "Library", "Application Support", "gyit", "repositories")}}
	if got := f.controlParent(); got != root {
		t.Fatalf("control parent %q, want %q", got, root)
	}
}
