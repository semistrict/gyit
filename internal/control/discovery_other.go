//go:build !linux

package control

import "fmt"

func readEndpoint(string) ([]byte, error) {
	return nil, fmt.Errorf("mount discovery requires Linux; use --socket for an explicit endpoint")
}
