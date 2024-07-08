package main

import (
	"fmt"
	"io"
	"log"
	"time"

	"github.com/esmailemami/fstore/internal/fileserver"
	"github.com/esmailemami/fstore/internal/p2p"
	"github.com/esmailemami/fstore/pkg/logging"
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
	s2 := newServer(":4000", ":3000")
	// newServer(":5000", ":4000", ":3000")
	// s2 := newServer(":7000", ":5000")

	// content := make([]byte, 1*1024*1024)
	// io.ReadFull(rand.Reader, content)

	// for i := 0; i < 100; i++ {
	// 	content := make([]byte, 20*1024*1024)
	// 	io.ReadFull(rand.Reader, content)
	// 	//[]byte("this is a new big file over here!!!")
	// 	_, err := s2.Store(fmt.Sprintf("myFile%d.jpg", i), bytes.NewReader(content))
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }

	// _, err := s2.Store("myFile0.jpg", bytes.NewReader([]byte("this is a new big file over here!!!")))
	// if err != nil {
	// 	log.Fatal(err)
	// }

	time.Sleep(time.Second)

	fmt.Println("getting file...")

	n, r, err := s2.Get("myFile0.jpg")
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
