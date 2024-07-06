package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/esmailemami/fstore/internal/fileserver"
	"github.com/esmailemami/fstore/internal/p2p"
)

func newServer(listenAddr string, nodes ...string) *fileserver.FileServer {
	tcpOpts := p2p.TCPTransportOpts{
		LinstenAddr: listenAddr,
		Decoder:     p2p.DefaultDecoder{},
	}

	tcp := p2p.NewTCPTransport(tcpOpts)

	fileServerOpts := fileserver.FileServerOpts{
		Transport:      tcp,
		BootstrapNodes: nodes,
		EncKey:         []byte("thisis32bitlongpassphraseimusing"),
	}

	fs := fileserver.NewFileServer(fileServerOpts)

	if err := fs.Start(); err != nil {
		log.Fatal(err)
	}
	return fs
}

func main() {
	newServer(":3000")
	s2 := newServer(":5000", ":3000")

	// content := make([]byte, 1*1024*1024)
	// io.ReadFull(rand.Reader, content)

	for i := 0; i < 1; i++ {
		_, err := s2.Store(fmt.Sprintf("myFile%d.jpg", i), bytes.NewReader([]byte("this is a big file over here!!!")))
		if err != nil {
			log.Fatal(err)
		}
	}

	select {}
}
