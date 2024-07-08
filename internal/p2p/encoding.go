package p2p

import (
	"encoding/binary"
	"io"
)

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type JSONDecoder struct{}

func (j *JSONDecoder) Decode(r io.Reader, rpc *RPC) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	rpc.Payload = data
	return nil
}

type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	// peek the first byte for checking status
	peekBuf := make([]byte, 1)
	if _, err := r.Read(peekBuf); err != nil {
		return err
	}

	// logging.Info("tcp decoder called", "peek", peekBuf[0])

	if peekBuf[0] == lockMessage {
		msg.lock = true
		return nil
	}

	// take the size from binary
	var size int64
	binary.Read(r, binary.LittleEndian, &size)

	// take the payload
	buf := make([]byte, size)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	msg.Payload = buf[:n]

	return nil
}
