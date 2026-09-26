package control

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pb "gat/internal/gen/gat/control/v1"
	"google.golang.org/protobuf/proto"
)

func TestDiscoveryWalksToNearestPhysicalMount(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nested := filepath.Join(root, "nested")
	cwd := filepath.Join(nested, "sub", "directory")
	if err := os.MkdirAll(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	endpoint := func(socket string) []byte {
		b, err := proto.Marshal(&pb.MountEndpoint{Version: Version, Socket: socket})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	attributes := map[string][]byte{root: endpoint("/outer/control.sock"), nested: endpoint("/inner/control.sock")}
	var visited []string
	read := func(path string) ([]byte, error) { visited = append(visited, path); return attributes[path], nil }
	for _, start := range []string{cwd, nested, root} {
		visited = nil
		got, err := discover(start, read)
		want := "/inner/control.sock"
		if start == root {
			want = "/outer/control.sock"
		}
		if err != nil || got != want {
			t.Fatal("nearest mount", start, got, err)
		}
		if visited[0] != start {
			t.Fatal("did not start at working directory", visited)
		}
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(cwd, alias); err != nil {
		t.Fatal(err)
	}
	if got, err := discover(alias, read); err != nil || got != "/inner/control.sock" {
		t.Fatal("symlink discovery", got, err)
	}
	delete(attributes, nested)
	if got, err := discover(cwd, read); err != nil || got != "/outer/control.sock" {
		t.Fatal("ancestor discovery", got, err)
	}
}

func TestDiscoveryStopsOnInvalidEndpoint(t *testing.T) {
	cwd, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	encode := func(e *pb.MountEndpoint) []byte { b, _ := proto.Marshal(e); return b }
	for _, data := range [][]byte{
		{}, {0xff}, make([]byte, maxEndpoint+1),
		encode(&pb.MountEndpoint{Version: 99, Socket: "/socket"}),
		encode(&pb.MountEndpoint{Version: Version, Socket: "relative.sock"}),
		encode(&pb.MountEndpoint{Version: Version, ControlFile: "/outside/.gat.control"}),
		encode(&pb.MountEndpoint{Version: Version, ControlFile: "relative"}),
		encode(&pb.MountEndpoint{Version: Version, Socket: "/socket", ControlFile: filepath.Join(cwd, ControlFileName)}),
	} {
		calls := 0
		_, err := discover(cwd, func(string) ([]byte, error) { calls++; return data, nil })
		if err == nil || calls != 1 {
			t.Fatal("bad attribute must not fall back to another mount", err, calls)
		}
	}
	failure := errors.New("access denied")
	if _, err := discover(cwd, func(string) ([]byte, error) { return nil, failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if _, err := discover(cwd, func(string) ([]byte, error) { return nil, nil }); err == nil || !strings.Contains(err.Error(), "no mounted repository") {
		t.Fatal("missing mount", err)
	}
}

func TestDiscoveryFindsControlFileFromNestedDirectory(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cwd := filepath.Join(root, "nested")
	if err := os.Mkdir(cwd, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(&pb.MountEndpoint{Version: Version, ControlFile: filepath.Join(root, ControlFileName)})
	if err != nil {
		t.Fatal(err)
	}
	address, gotRoot, err := discoverMount(cwd, func(path string) ([]byte, error) {
		if path == root {
			return data, nil
		}
		return nil, nil
	})
	if err != nil || address != "fuse:"+filepath.Join(root, ".gat.control") || gotRoot != root {
		t.Fatalf("discovery: %q %q %v", address, gotRoot, err)
	}
}
