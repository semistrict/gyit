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
	data, err := (proto.MarshalOptions{}).MarshalAppend(make([]byte, 4), message)
	if err != nil {
		return err
	}
	if len(data) == 4 || len(data)-4 > maxFrame {
		return fmt.Errorf("invalid control frame size %d", len(data)-4)
	}
	binary.BigEndian.PutUint32(data[:4], uint32(len(data)-4))
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
