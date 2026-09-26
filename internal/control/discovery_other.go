//go:build !linux && !darwin

package control

import "fmt"

func readEndpoint(string) ([]byte, error) {
	return nil, fmt.Errorf("mount discovery requires Linux or macOS; use --socket for an explicit endpoint")
}
