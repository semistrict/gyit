# Linux FUSE wire ABI on a Darwin host

This overlay is applied only to the ignored dependency copy used by
`scripts/build_virtiofs.py`. Normal gyit and FSKit builds retain upstream Go-FUSE.
The pinned dependency version comes from go.mod.

Go-FUSE selects its protocol structures according to the host OS. A macOS
Virtio server needs Linux structures for its Linux guest. `types_darwin.go` and
`fs/bridge_nonlinux.go` adapt the existing lnx test fixture; request/opcode files
use the upstream Linux implementations, and print flags follow that ABI.
`virtio_flat.go` adapts VZ's flattened request/reply buffers to ProtocolServer.
All Go-FUSE adaptations retain its BSD license (LICENSE in this directory).
These files are source inputs, not edits to the module cache.
