package control

import (
	"errors"
	"golang.org/x/sys/unix"
)

func readEndpoint(path string) ([]byte, error) {
	data := make([]byte, maxEndpoint)
	n, err := unix.Getxattr(path, EndpointAttribute, data)
	if errors.Is(err, unix.ENOATTR) || errors.Is(err, unix.ENOTSUP) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}
