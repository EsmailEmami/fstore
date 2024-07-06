package p2p

import (
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

	if peekBuf[0] == StreamMessage {
		msg.isStream = true
		return nil
	} else if peekBuf[0] == TextMessage {
		msg.isText = true
	}

	// normal messaging
	buf := make([]byte, 1024)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}
	msg.Payload = buf[:n]

	return nil
}
