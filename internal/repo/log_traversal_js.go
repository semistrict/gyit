//go:build js

package repo

import "fmt"

func newLogTraversalDisk(string) (logTraversalDisk, error) {
	return nil, fmt.Errorf("history traversal exceeds the browser demo's memory window; disk-backed traversal requires the native runtime")
}
