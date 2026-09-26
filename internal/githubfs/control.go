package githubfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"google.golang.org/protobuf/proto"
	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"
)

// Endpoint advertises the existing protobuf command service only at a
// repository root. Commands share its pinned snapshot and durable object store.
func (f *FS) Endpoint(ctx context.Context, path string) ([]byte, error) {
	p, t, err := parse(path)
	if err != nil || len(p) != 2 {
		return nil, err
	}
	j, err := f.ensure(ctx, t, strings.Join(p, "/"))
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, os.ErrClosed
	}
	snap, notice, _ := j.status()
	if snap == nil {
		return nil, fmt.Errorf("repository is not ready: %s", notice)
	}
	if j.control == nil {
		if f.controlDir == "" {
			f.controlDir, err = os.MkdirTemp(f.controlParent(), "gy-")
			if err != nil {
				return nil, err
			}
		}
		// Different aliases may have been opened before their command endpoints.
		file, err := os.CreateTemp(f.controlDir, "s-")
		if err != nil {
			return nil, err
		}
		endpoint := file.Name()
		file.Close()
		if err := os.Remove(endpoint); err != nil {
			return nil, err
		}
		controller := control.New(j.repository, snap)
		server, err := control.ListenStream(f.ctx, endpoint, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
			if req.GetSwitch() != nil {
				return send(&pb.Response{Version: control.Version, Result: &pb.Response_Error{Error: &pb.Error{Code: pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, Message: "GitHub mounts pin a revision; open a different @revision path to change versions"}}})
			}
			return controller.Serve(ctx, req, send)
		})
		if err != nil {
			return nil, err
		}
		j.control, j.endpoint = server, endpoint
	}
	return proto.Marshal(&pb.MountEndpoint{Version: control.Version, Socket: j.endpoint})
}

func (n Namespace) Endpoint(ctx context.Context, path string) ([]byte, error) {
	p, err := namespacePath(path)
	if err != nil {
		return nil, nil
	}
	return n.FS.Endpoint(ctx, p)
}

func (f *FS) controlParent() string {
	if runtime.GOOS != "darwin" {
		return ""
	}
	// FSKit runs in an App Sandbox: the process-wide TMPDIR points outside the
	// extension's writable container. Keep Unix sockets under the same container
	// as durable repository data while using a short path for sockaddr_un.
	const marker = "/Library/Application Support/"
	if i := strings.Index(f.opts.DataDir, marker); i > 0 {
		return filepath.Clean(f.opts.DataDir[:i])
	}
	return ""
}
