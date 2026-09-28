# Linux FUSE wire adapters

This overlay is applied only to the ignored dependency copy used by
`scripts/build_virtiofs.py` and `scripts/build_vhost.py`. Normal gyit and FSKit
builds retain upstream Go-FUSE.
The pinned dependency version comes from go.mod.

Go-FUSE selects its protocol structures according to the host OS. A macOS
Virtio server needs Linux structures for its Linux guest. `types_darwin.go` and
`fs/bridge_nonlinux.go` adapt the existing lnx test fixture; request/opcode files
use the upstream Linux implementations, and print flags follow that ABI.
`virtio_flat.go` adapts VZ's flattened request/reply buffers to ProtocolServer.
The errno conversion is selected by host OS. The Linux vhost-user entry point
uses `HandleScatter`: guest descriptors can split a READ payload across arbitrary
pages, which upstream `HandleRequest` currently rejects. The adapter gathers
the request, uses the same flat protocol path, then scatters exactly the reply
bytes. `TestScatterReadAcrossGuestPages` covers fragmented headers, multi-page
content, offsets, EOF and reply bounds. Both build scripts generate the overlay
from these source inputs; the Linux build also produces `fuse.test` for execution
on its target host.
All Go-FUSE adaptations retain its BSD license (LICENSE in this directory).
These files are source inputs, not edits to the module cache.
