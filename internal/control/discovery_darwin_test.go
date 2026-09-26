package control

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	pb "gyit/internal/gen/gyit/control/v1"
)

func TestDarwinDiscoversNativeControlEndpoint(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}
	data, err := proto.Marshal(&pb.MountEndpoint{Version: Version, Socket: "/tmp/private-gyit/control.sock"})
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Setxattr(root, EndpointAttribute, data, 0); err != nil {
		t.Fatal(err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, mount, err := DiscoverMount(sub)
	if err != nil || endpoint != "/tmp/private-gyit/control.sock" || mount != root {
		t.Fatalf("discover: %q %q %v", endpoint, mount, err)
	}
}

func TestDarwinDiscoversRepositoryWithinSharedVolume(t *testing.T) {
	volume := t.TempDir()
	for _, name := range []string{"linux", "other"} {
		root := filepath.Join(volume, name)
		sub := filepath.Join(root, "nested")
		if err := os.MkdirAll(sub, 0700); err != nil {
			t.Fatal(err)
		}
		socket := filepath.Join("/tmp", name, "control.sock")
		data, err := proto.Marshal(&pb.MountEndpoint{Version: Version, Socket: socket})
		if err != nil {
			t.Fatal(err)
		}
		if err := unix.Setxattr(root, EndpointAttribute, data, 0); err != nil {
			t.Fatal(err)
		}
		physical, err := filepath.EvalSymlinks(root)
		if err != nil {
			t.Fatal(err)
		}
		endpoint, mount, err := DiscoverMount(sub)
		if err != nil || endpoint != socket || mount != physical {
			t.Fatalf("%s: discover %q %q %v", name, endpoint, mount, err)
		}
	}
	if _, _, err := DiscoverMount(volume); err == nil {
		t.Fatal("shared volume root must not select a repository")
	}
}
