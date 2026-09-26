package control

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

const Version = 1
const MaxFrameSize = 64 << 10
const maxFrame = MaxFrameSize

func readFrame(r io.Reader, message proto.Message) error {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxFrame {
		return fmt.Errorf("invalid control frame size %d", size)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return err
	}
	return proto.Unmarshal(data, message)
}

func writeFrame(w io.Writer, message proto.Message) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if len(data) == 0 || len(data) > maxFrame {
		return fmt.Errorf("invalid control frame size %d", len(data))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(data)))
	for _, part := range [][]byte{header[:], data} {
		for len(part) > 0 {
			n, err := w.Write(part)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}
