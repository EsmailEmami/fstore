package fileserver

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/esmailemami/fstore/internal/p2p"
	"github.com/esmailemami/fstore/internal/store"
	"github.com/esmailemami/fstore/pkg/logging"
	"github.com/esmailemami/fstore/pkg/utils/security"
)

var (
	ErrInvalidMessageType = errors.New("unexpected message")
)

type FileServerOpts struct {
	Transport          p2p.Transport
	BootstrapNodes     []string
	MessageTransformer MessageTransformer
	KeyEncrypter       security.TextEncrypter
	EncKey             []byte
	Encrypter          security.IOEncrypter
}

func (f *FileServerOpts) prepare() {
	if f.MessageTransformer == nil {
		f.MessageTransformer = NewAESGCMMessageTransformer([]byte("thisis32bitlongpassphraseimusing"))
	}

	if f.KeyEncrypter == nil {
		f.KeyEncrypter = &security.SHA1TextEncrypter{}
	}

	if f.Encrypter == nil {
		f.Encrypter = security.NewAESGCMIOEncrypter(f.EncKey)
	}
}

type FileServer struct {
	FileServerOpts

	store *store.Store

	peerLock sync.RWMutex
	peers    map[string]p2p.Peer

	quitch chan struct{}
}

func NewFileServer(opts FileServerOpts) *FileServer {
	opts.prepare()

	storeOpts := store.StoreOpts{
		RootPath:          fmt.Sprintf("server%s", opts.Transport.Addr()),
		PathTransformFunc: store.MD5PathTransformFunc,
		Encrypter:         security.NewAESGCMIOEncrypter(opts.EncKey),
	}

	store := store.NewStore(storeOpts)

	fs := &FileServer{
		FileServerOpts: opts,
		peerLock:       sync.RWMutex{},
		peers:          make(map[string]p2p.Peer),
		quitch:         make(chan struct{}),
		store:          store,
	}

	fs.Transport.OnPeerConnect(fs.onConnectPeer)
	fs.Transport.OnPeerDisconnect(fs.onDisconnectPeer)

	return fs
}

func (fs *FileServer) Start() error {
	if err := fs.Transport.Listen(); err != nil {
		return err
	}

	if err := fs.boostrapNodes(); err != nil {
		return err
	}

	go fs.listen()

	logging.Info("File Server started.", "listenAddr", fs.Transport.Addr())

	return nil
}

func (fs *FileServer) boostrapNodes() error {
	for _, addr := range fs.BootstrapNodes {
		logging.Debug("File Server Dial", "listenAddr", fs.Transport.Addr(), "addr", addr)

		if err := fs.Transport.Dial(addr); err != nil {
			logging.WarnE("File Server failed to dial node", err, "listenAddr", fs.Transport.Addr(), "boostrapAddr", addr)
			return err
		}
	}

	return nil
}

func (fs *FileServer) listen() {
	defer func() {
		logging.Warn("File Server closing the transport...", "listenAddr", fs.Transport.Addr())
		fs.Transport.Close()
	}()

	for {
		select {
		case rpc := <-fs.Transport.Consume():
			var msg Message
			if err := fs.MessageTransformer.Decode(bytes.NewReader(rpc.Payload), &msg); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				logging.ErrorE("File Server failed to decode", err, "listenAddr", fs.Transport.Addr(), "payload", rpc.Payload)
				continue
			}

			go func() {
				if err := fs.handleMessage(rpc.From, &msg); err != nil {
					logging.Warn("File Server failed to handle message", err, "listenAddr", fs.Transport.Addr())
				}
			}()
		case <-fs.quitch:
			logging.Warn("File Server closing...")
			return
		}
	}
}

func (fs *FileServer) onDisconnectPeer(peer p2p.Peer) {
	fs.peerLock.Lock()
	delete(fs.peers, peer.RemoteAddr().String())
	fs.peerLock.Unlock()

	logging.Info("File Server Peer Disconnected", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String())
}

func (fs *FileServer) onConnectPeer(peer p2p.Peer) {
	fs.peerLock.Lock()
	fs.peers[peer.RemoteAddr().String()] = peer
	fs.peerLock.Unlock()

	logging.Info("File Server New Peer Connected", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String())
}

func (fs *FileServer) Stop() {
	close(fs.quitch)
}

func (fs *FileServer) Store(key string, r io.Reader) (int64, error) {
	var (
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	n, err := fs.store.Write(key, tee)
	if err != nil {
		return 0, err
	}

	logging.Info("File Server locally stored.", "listenAddr", fs.Transport.Addr(), "size", n)

	for _, peer := range fs.peers {
		msg := Message{
			Payload: MessageStoreFile{
				Key:      fs.KeyEncrypter.Encrypt(key),
				FileSize: fs.Encrypter.Size(n),
			},
		}

		peer.TextNotify()

		//time.Sleep(time.Second)

		if err := fs.MessageTransformer.Encode(peer, &msg); err != nil {
			return 0, err
		}

		//time.Sleep(time.Second)

		if err := peer.StreamNotify(); err != nil {
			return 0, err
		}

		//time.Sleep(time.Second)

		_, err := fs.Encrypter.Encrypt(fileBuffer, peer)
		if err != nil {
			return 0, err
		}

	}

	return n, nil
}

func (fs *FileServer) handleMessage(from string, msg *Message) error {
	fs.peerLock.RLock()
	peer, ok := fs.peers[from]
	fs.peerLock.RUnlock()

	if !ok {
		return fmt.Errorf("peer with addr (%s) not found", from)
	}

	switch payload := msg.Payload.(type) {
	case MessageGetFile:
		return fs.handleMessageGetFile(from, payload)
	case MessageStoreFile:
		return fs.handleMessageStoreFile(peer, payload)
	}

	return ErrInvalidMessageType
}

type MessageGetFile struct {
	Key string
}

func (fs *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	_ = msg
	return nil
}

type MessageStoreFile struct {
	Key      string
	FileSize int64
}

func (fs *FileServer) handleMessageStoreFile(peer p2p.Peer, msg MessageStoreFile) error {
	logging.Debug("File Server store called. reading...", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String(), "size", msg.FileSize)

	n, err := fs.store.Write(msg.Key, io.LimitReader(peer, msg.FileSize))

	logging.Debug("File Server store called. read done", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String(), "writtenSize", n, "calledSize", msg.FileSize)

	peer.CloseStream()

	return err
}

func init() {
	gob.Register(MessageGetFile{})
	gob.Register(MessageStoreFile{})
}
