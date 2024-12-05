package p2p

import (
	"errors"
	"io"
	"net"
	"sync"

	"github.com/esmailemami/fstore/pkg/logging"
)

type TCPPeer struct {
	conn net.Conn

	lockWg sync.WaitGroup
}

func NewTCPPeer(conn net.Conn) *TCPPeer {
	return &TCPPeer{
		conn:   conn,
		lockWg: sync.WaitGroup{},
	}
}

func (t *TCPPeer) RemoteAddr() net.Addr {
	return t.conn.RemoteAddr()
}

func (t *TCPPeer) Read(b []byte) (n int, err error) {
	return t.conn.Read(b)
}

func (t *TCPPeer) Write(b []byte) (n int, err error) {
	return t.conn.Write(b)

}

func (t *TCPPeer) Lock() error {
	_, err := t.conn.Write(LockMessage)
	return err
}
func (t *TCPPeer) UnLock() {
	t.lockWg.Done()
}

type TCPTransportOpts struct {
	LinstenAddr  string
	HanshakeFunc HandshakeFunc
	Decoder      Decoder
}

func (t *TCPTransportOpts) prepare() {
	if t.Decoder == nil {
		t.Decoder = &DefaultDecoder{}
	}
}

type TCPTransport struct {
	TCPTransportOpts
	listener     net.Listener
	onConnect    PeerFunc
	onDisconnect PeerFunc

	rpcCh chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	opts.prepare()

	t := &TCPTransport{
		TCPTransportOpts: opts,
		rpcCh:            make(chan RPC, 1024),
	}

	return t
}

func (t *TCPTransport) Addr() string {
	return t.LinstenAddr
}

func (t *TCPTransport) Listen() (err error) {
	t.listener, err = net.Listen("tcp", t.LinstenAddr)
	if err != nil {
		return err
	}

	go t.accept()

	return nil
}

func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcCh
}

func (t *TCPTransport) OnPeerDisconnect(fn PeerFunc) {
	t.onDisconnect = fn
}

func (t *TCPTransport) OnPeerConnect(fn PeerFunc) {
	t.onConnect = fn
}

func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	go t.handleConn(conn)
	return nil
}

func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

func (t *TCPTransport) accept() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			logging.ErrorE("TCP accept error", err)
		}

		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	peer := NewTCPPeer(conn)
	defer func() {
		logging.Warn("TCP closing the per connection", "listenAddr", t.LinstenAddr, "peerAddr", conn.RemoteAddr().String())
		conn.Close()
		if t.onDisconnect != nil {
			t.onDisconnect(peer)
		}
	}()

	if t.HanshakeFunc != nil {
		if err := t.HanshakeFunc(peer); err != nil {
			logging.WarnE("TCP handshake failed", err, "listenAddr", t.LinstenAddr, "peerAddr", conn.RemoteAddr().String())
		}
		return
	}

	if t.onConnect != nil {
		t.onConnect(peer)
	}

	for {
		msg := RPC{
			From: conn.RemoteAddr().String(),
		}

		err := t.Decoder.Decode(peer, &msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}
			logging.ErrorE("TCP peer connection failed to decode", err, "listenAddr", t.LinstenAddr, "peerAddr", conn.RemoteAddr().String())
			continue
		}

		// needs to block reading
		if msg.lock {
			peer.lockWg.Add(1)
			logging.Info("Peer locked...", "listenAddr", t.LinstenAddr, "peerAddr", conn.RemoteAddr().String())
			peer.lockWg.Wait()
			logging.Info("Peer unlocked. continueing...", "listenAddr", t.LinstenAddr, "peerAddr", conn.RemoteAddr().String())
			continue
		}

		t.rpcCh <- msg
	}
}
