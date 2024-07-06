package fileserver

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/esmailemami/fstore/internal/p2p"
	"github.com/stretchr/testify/assert"
)

func newServer(listenAddr string, nodes ...string) (*FileServer, error) {
	tcpOpts := p2p.TCPTransportOpts{
		LinstenAddr: listenAddr,
		Decoder:     p2p.DefaultDecoder{},
	}

	tcp := p2p.NewTCPTransport(tcpOpts)

	fileServerOpts := FileServerOpts{
		Transport:      tcp,
		BootstrapNodes: nodes,
		EncKey:         []byte("thisis32bitlongpassphraseimusing"),
	}

	fs := NewFileServer(fileServerOpts)

	if err := fs.Start(); err != nil {
		return nil, err
	}
	return fs, nil
}

func TestServerStore(t *testing.T) {
	_, err := newServer(":6000")
	assert.Nil(t, err)
	s2, err := newServer(":5000", ":6000")
	assert.Nil(t, err)

	key := "myFile.jpg"
	buf := bytes.NewReader([]byte("oh this is my nude!!!"))

	n, err := s2.Store(key, buf)
	assert.Nil(t, err)
	fmt.Printf("written size: %d\n", n)
}
