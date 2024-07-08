package fileserver

import (
	"bytes"
	"encoding/binary"
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
		RootPath:          fmt.Sprintf("storage/server%s", opts.Transport.Addr()),
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

			go func(msg Message) {
				if err := fs.handleMessage(rpc.From, msg); err != nil {
					logging.Warn("File Server failed to handle message", err, "listenAddr", fs.Transport.Addr())
				}
			}(msg)
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
		encKey     = fs.KeyEncrypter.Encrypt(key)
		fileBuffer = new(bytes.Buffer)
		tee        = io.TeeReader(r, fileBuffer)
	)

	n, err := fs.store.Write(encKey, tee)
	if err != nil {
		return 0, err
	}

	logging.Info("File Server locally stored.", "listenAddr", fs.Transport.Addr(), "size", n)

	mu := fs.multiWriter()

	msg := Message{
		Payload: MessageStoreFile{
			Key:      encKey,
			FileSize: fs.Encrypter.Size(n),
		},
	}

	if err := fs.sendMessage(mu, &msg); err != nil {
		return 0, err
	}

	// notify incomming is stream
	if _, err := mu.Write(p2p.LockMessage); err != nil {
		return 0, err
	}

	// write encrypted bytes to peers
	if _, err := fs.Encrypter.Encrypt(fileBuffer, mu); err != nil {
		return 0, err
	}

	return n, nil
}

func (fs *FileServer) Get(key string) (int64, io.Reader, error) {
	encKey := fs.KeyEncrypter.Encrypt(key)
	if fs.store.Has(encKey) {
		return fs.store.Read(encKey)
	}

	logging.Info("trying to get file from peers...", "listenAddr", fs.Transport.Addr())

	msg := Message{
		Payload: MessageGetFileRequest{
			Key: encKey,
		},
	}

	settedFromPeer := false

	for _, peer := range fs.peers {

		// send the request
		if err := fs.sendMessage(peer, &msg); err != nil {
			return 0, nil, err
		}

		logging.Info("trying to get file from peer, decoding message...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		// get response
		msg, err := fs.readMessage(peer)
		if err != nil {
			peer.UnLock()
			return 0, nil, err
		}
		resp := msg.Payload.(MessageGetFileResponse)

		logging.Info("Response got", "resp", resp)

		// not exists or not streaming
		if !resp.Exists || !resp.Stream {
			peer.UnLock()
			continue
		}

		logging.Info("got file size. writting to local storage...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String(), "size", resp.Size)

		if _, err := fs.store.WriteDecrypt(encKey, io.LimitReader(peer, resp.Size)); err != nil {
			peer.UnLock()
			return 0, nil, err
		}

		logging.Info("file got successfully from peer", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		peer.UnLock()
		settedFromPeer = true
		break
	}

	if settedFromPeer {
		return fs.store.Read(encKey)
	}

	return 0, nil, errors.New("file not found")
}

func (fs *FileServer) handleMessage(from string, msg Message) error {
	fs.peerLock.RLock()
	peer, ok := fs.peers[from]
	fs.peerLock.RUnlock()

	if !ok {
		return fmt.Errorf("peer with addr (%s) not found", from)
	}

	switch payload := msg.Payload.(type) {
	case MessageGetFileRequest:
		return fs.handleMessageGetFileRequest(peer, payload)
	case MessageStoreFile:
		return fs.handleMessageStoreFile(peer, payload)
	}

	return ErrInvalidMessageType
}

type MessageGetFileRequest struct {
	Key string
}

type MessageGetFileResponse struct {
	Exists bool
	Size   int64
	Stream bool
}

func (fs *FileServer) handleMessageGetFileRequest(peer p2p.Peer, msg MessageGetFileRequest) error {
	logging.Info("message get file called.", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String(), "storeAddr", fs.store.RootPath)

	// lock the peer for giving the response
	if err := peer.Lock(); err != nil {
		return err
	}

	// logging.Info("locked...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String(), "storeAddr", fs.store.RootPath)

	resp := MessageGetFileResponse{}

	if !fs.store.Has(msg.Key) {
		logging.Info("file does not exists", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		// set not stream content
		resp.Exists = false
		resp.Stream = false

		if err := fs.MessageTransformer.Encode(peer, &Message{Payload: resp}); err != nil {
			return err
		}

		return nil
	}

	logging.Info("file exists. processing...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	n, f, err := fs.store.Read(msg.Key)
	if err != nil {
		return err
	}
	defer f.Close()

	logging.Info("file readed. ", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	// send the data before streaming
	resp.Exists = true
	resp.Size = n
	resp.Stream = true
	if err := fs.MessageTransformer.Encode(peer, &Message{Payload: resp}); err != nil {
		return err
	}

	logging.Info("writting file to peer", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	// // if err := binary.Write(peer, binary.LittleEndian, n); err != nil {
	// // 	return err
	// // }

	if _, err := io.Copy(peer, f); err != nil {
		return err
	}

	logging.Info("writting file completed", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

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

	peer.UnLock()

	return err
}

func (fs *FileServer) multiWriter() io.Writer {
	peers := []io.Writer{}
	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}
	return io.MultiWriter(peers...)
}

func (fs *FileServer) readMessage(r io.Reader) (*Message, error) {
	// take the size from binary
	var size int64
	binary.Read(r, binary.LittleEndian, &size)

	// decode the message
	var msg Message
	if err := fs.MessageTransformer.Decode(io.LimitReader(r, size), &msg); err != nil {
		logging.ErrorE("File Server failed to decode", err, "listenAddr", fs.Transport.Addr())
		return nil, err
	}
	return &msg, nil
}

func (fs *FileServer) sendMessage(w io.Writer, msg *Message) error {
	// notify incomming message type
	if _, err := w.Write(p2p.TextMessage); err != nil {
		return err
	}

	return fs.MessageTransformer.Encode(w, msg)
}

func init() {
	gob.Register(MessageGetFileRequest{})
	gob.Register(MessageGetFileResponse{})
	gob.Register(MessageStoreFile{})
}
