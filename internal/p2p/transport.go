package p2p

import "net"

type Peer interface {
	RemoteAddr() net.Addr
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	TextNotify() error
	StreamNotify() error
	CloseStream()
}

type PeerFunc func(Peer)

type Transport interface {
	Addr() string
	OnPeerConnect(PeerFunc)
	OnPeerDisconnect(PeerFunc)
	Listen() error
	Dial(string) error
	Consume() <-chan RPC
	Close() error
}
