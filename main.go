package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"strconv"

	"github.com/esmailemami/fstore/internal/fileserver"
	"github.com/esmailemami/fstore/internal/p2p"
	"github.com/esmailemami/fstore/pkg/logging"
)

func newServer(listenAddr string, encKey []byte, nodes ...string) *fileserver.FileServer {
	tcpOpts := p2p.TCPTransportOpts{
		LinstenAddr: listenAddr,
		Decoder:     p2p.DefaultDecoder{},
	}

	tcp := p2p.NewTCPTransport(tcpOpts)

	fileServerOpts := fileserver.FileServerOpts{
		Transport:      tcp,
		BootstrapNodes: nodes,
		EncKey:         encKey,
	}

	fs := fileserver.NewFileServer(fileServerOpts)

	if err := fs.Start(); err != nil {
		log.Fatal(err)
	}
	return fs
}

func main() {
	fileserver.Transformer = fileserver.NewAESGCMMessageTransformer([]byte("thisis32bitlongpassphraseimusing"))

	s1 := newServer(":3000", []byte("thisis32bitlongpassphraseimusing"))
	_ = s1
	s2 := newServer(":4000", []byte("thisisexactly32byteslong1234gfg5"), ":3000")
	_ = s2
	s3 := newServer(":5000", []byte("32characterslongpassphrase123544"), ":4000", ":3000")

	//
	if err := s1.AddNode(":4000", ":5000"); err != nil {
		log.Fatal(err)
	}

	if err := s2.AddNode(":5000"); err != nil {
		log.Fatal(err)
	}

	//
	key := "supersecurefile.png"
	content := []byte("7 bytes")

	_, err := s3.Store(key, bytes.NewReader(content), -1)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err := s3.Store("file"+strconv.Itoa(i)+".jpg", bytes.NewReader([]byte("this is number "+strconv.Itoa(i))), -1)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		_, err := s2.Store("file"+strconv.Itoa(i)+".jpg", bytes.NewReader([]byte("this is number "+strconv.Itoa(i))), -1)
		if err != nil {
			log.Fatal(err)
		}
	}

	for i := 0; i < 10; i++ {
		_, err := s1.Store("file"+strconv.Itoa(i)+".jpg", bytes.NewReader([]byte("this is number "+strconv.Itoa(i))), -1)
		if err != nil {
			log.Fatal(err)
		}
	}

	n, r, err := s3.Get(key)
	if err != nil {
		logging.ErrorE(err.Error(), err)
		select {}
	}

	buf, err := io.ReadAll(r)
	if err != nil {
		logging.ErrorE(err.Error(), err)
	} else {
		fmt.Println("file size:", n)
		fmt.Println("content:", string(buf))
	}

	select {}
}
