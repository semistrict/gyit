package githubfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"gyit/internal/control"
	pb "gyit/internal/gen/gyit/control/v1"

	"google.golang.org/protobuf/proto"
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
		server, err := control.ListenStream(f.ctx, endpoint, func(ctx context.Context, req *pb.Request, send func(*pb.Response) error) error {
			if req.Version != control.Version {
				return send(commandError(pb.ErrorCode_ERROR_CODE_UNSUPPORTED_VERSION, "unsupported control protocol version"))
			}
			current, notice, _ := j.status()
			if current == nil {
				return send(commandError(pb.ErrorCode_ERROR_CODE_INTERNAL, fmt.Sprintf("repository is not ready: %s", strings.TrimSpace(notice))))
			}
			j.mu.RLock()
			progressive := j.progressive
			j.mu.RUnlock()
			repository := progressive.HistoryRepository()
			if req.GetUpdate() != nil {
				next, err := f.update(ctx, j)
				if err != nil {
					return send(commandError(pb.ErrorCode_ERROR_CODE_INTERNAL, f.redact(err.Error())))
				}
				return send(commandSnapshot(next))
			}
			if req.GetLog() != nil || req.GetPathLog() != nil || req.GetHistoryLog() != nil {
				current, _, _ := j.status()
				return control.New(repository, current).Serve(ctx, req, send)
			}
			if req.GetStatus() != nil {
				current, _, _ := j.status()
				return send(commandSnapshot(current))
			}
			return send(commandError(pb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, "this command is not yet supported by mounts; file browsing, gyit log and gyit update are available"))
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
