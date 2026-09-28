//go:build linux

package githubmount

import (
	"context"
	"gyit/internal/control"
	"strings"
	"sync"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Invalidation runs off the request goroutine: the kernel can wait on locks
// held by the request that caused publication. Coalesce updates by subtree.
func (n *node) watchChanges(ctx context.Context) func() {
	var mu sync.Mutex
	pending := make(map[string]bool)
	wake := make(chan struct{}, 1)
	unsubscribe := n.source.SubscribeChanges(func(path string) {
		mu.Lock()
		pending[path] = true
		mu.Unlock()
		select {
		case wake <- struct{}{}:
		default:
		}
	})
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake:
				mu.Lock()
				paths := pending
				pending = make(map[string]bool)
				mu.Unlock()
				for path := range paths {
					inode := n.GetChild("github.com")
					if path != "" {
						for _, part := range strings.Split(path, "/") {
							if inode == nil {
								break
							}
							inode = inode.GetChild(part)
						}
					}
					if inode != nil {
						invalidate(inode, strings.Contains(path, "/"))
					}
				}
			}
		}
	}()
	return func() { unsubscribe(); cancel() }
}

func invalidate(inode *fs.Inode, recursive bool) {
	_ = inode.NotifyContent(0, 0)
	for name, child := range inode.Children() {
		if name == control.ControlFileName {
			continue
		}
		if recursive {
			// Control streams must not be invalidated while carrying update.
			if _, ok := child.Operations().(*node); ok {
				invalidate(child, true)
			}
		}
		_ = inode.NotifyEntry(name)
	}
}
