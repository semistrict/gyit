package control

import (
	"fmt"
	"path/filepath"

	pb "gat/internal/gen/gat/control/v1"
	"google.golang.org/protobuf/proto"
)

const EndpointAttribute = "user.gat.control"
const maxEndpoint = 4096

// Discover finds the nearest mount by walking physical ancestors. The endpoint
// lives in a root attribute, so discovery adds no names to the repository tree.
func Discover(start string) (string, error) { return discover(start, readEndpoint) }

// DiscoverMount also returns the physical mount root for cwd-relative paths.
func DiscoverMount(start string) (socket, root string, err error) {
	return discoverMount(start, readEndpoint)
}

func discover(start string, read func(string) ([]byte, error)) (string, error) {
	socket, _, err := discoverMount(start, read)
	return socket, err
}
func discoverMount(start string, read func(string) ([]byte, error)) (string, string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", "", err
	}
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return "", "", err
	}
	for {
		data, err := read(dir)
		if err != nil {
			return "", "", fmt.Errorf("discover mount at %s: %w", dir, err)
		}
		if data != nil {
			endpoint := new(pb.MountEndpoint)
			if len(data) == 0 || len(data) > maxEndpoint || proto.Unmarshal(data, endpoint) != nil {
				return "", "", fmt.Errorf("invalid mount control attribute at %s", dir)
			}
			if endpoint.Version != Version {
				return "", "", fmt.Errorf("unsupported mount control version %d at %s", endpoint.Version, dir)
			}
			if !filepath.IsAbs(endpoint.Socket) {
				return "", "", fmt.Errorf("mount control socket at %s must be absolute", dir)
			}
			return endpoint.Socket, dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", "", fmt.Errorf("no mounted repository found above %s; run inside a mount or use --socket", start)
		}
		dir = parent
	}
}
