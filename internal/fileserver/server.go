package fileserver

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/esmailemami/fstore/internal/p2p"
	"github.com/esmailemami/fstore/internal/store"
	"github.com/esmailemami/fstore/pkg/logging"
	"github.com/esmailemami/fstore/pkg/utils/security"
)

// ErrInvalidMessageType is returned when an unexpected message type is encountered.
var ErrInvalidMessageType = errors.New("unexpected message")

// ErrFileNotExists is returned when a requested file does not exist.
var ErrFileNotExists = errors.New("sorry. file not found")

// ErrFileNotExists is returned when a peer does not exist.
var ErrPeerNotFound = errors.New("peer not found")

// FileServerOpts contains configuration options for FileServer.
type FileServerOpts struct {
	Transport      p2p.Transport          // Transport interface for communication
	BootstrapNodes []string               // List of bootstrap nodes to connect to initially
	KeyEncrypter   security.TextEncrypter // KeyEncrypter for encrypting file keys
	EncKey         []byte                 // Encryption key for Encrypter
	Encrypter      security.IOEncrypter   // Encrypter for encrypting file data
}

// prepare initializes default values for FileServerOpts if not provided.
func (f *FileServerOpts) prepare() {
	if f.KeyEncrypter == nil {
		f.KeyEncrypter = &security.SHA1TextEncrypter{}
	}

	if f.Encrypter == nil {
		f.Encrypter = security.NewAESGCMIOEncrypter(f.EncKey)
	}
}

// FileServer represents a server that manages file storage and communication with peers.
type FileServer struct {
	FileServerOpts                     // Embedded configuration options
	store          *store.Store        // Local file storage
	peerLock       sync.RWMutex        // Mutex for peers map access
	peers          map[string]p2p.Peer // Map of connected peers
	quitch         chan struct{}       // Channel for quitting
}

// NewFileServer creates a new FileServer instance with the provided options.
func NewFileServer(opts FileServerOpts) *FileServer {
	opts.prepare()

	// Configure store options
	storeOpts := store.StoreOpts{
		RootPath:          fmt.Sprintf("storage/server%s", opts.Transport.Addr()),
		PathTransformFunc: store.MD5PathTransformFunc,
		Encrypter:         opts.Encrypter,
	}

	// Create a new store instance
	store := store.NewStore(storeOpts)

	// Initialize FileServer instance
	fs := &FileServer{
		FileServerOpts: opts,
		peerLock:       sync.RWMutex{},
		peers:          make(map[string]p2p.Peer),
		quitch:         make(chan struct{}),
		store:          store,
	}

	// Register peer connect and disconnect callbacks
	fs.Transport.OnPeerConnect(fs.onConnectPeer)
	fs.Transport.OnPeerDisconnect(fs.onDisconnectPeer)

	return fs
}

// Start starts the FileServer, listens on the transport, and connects to bootstrap nodes.
func (fs *FileServer) Start() error {
	// Listen on transport
	if err := fs.Transport.Listen(); err != nil {
		return err
	}

	// Connect to bootstrap nodes
	if err := fs.boostrapNodes(); err != nil {
		return err
	}

	// Start listening for incoming messages
	go fs.listen()

	logging.Info("[FileServer] started.", "listenAddr", fs.Transport.Addr())

	return nil
}

// boostrapNodes connects to the specified bootstrap nodes.
func (fs *FileServer) boostrapNodes() error {
	for _, addr := range fs.BootstrapNodes {
		logging.Debug("[FileServer] dialing", "listenAddr", fs.Transport.Addr(), "addr", addr)

		// Dial each bootstrap node
		if err := fs.Transport.Dial(addr); err != nil {
			logging.WarnE("[FileServer] failed to dial node", err, "listenAddr", fs.Transport.Addr(), "boostrapAddr", addr)
			return err
		}
	}

	return nil
}

// listen listens for incoming messages and handles them asynchronously.
func (fs *FileServer) listen() {
	defer func() {
		logging.Warn("[FileServer] closing the transport...", "listenAddr", fs.Transport.Addr())
		fs.Transport.Close()
	}()

	for {
		select {
		case rpc := <-fs.Transport.Consume():
			var msg Message

			logging.Debug("[FileServer] transport RPC message", "from", rpc.From, "payload", rpc.Payload)

			// Decode incoming message
			if err := Transformer.Decode(bytes.NewReader(rpc.Payload), &msg); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				logging.ErrorE("[FileServer] failed to decode", err, "listenAddr", fs.Transport.Addr(), "payload", rpc.Payload)
				continue
			}

			// Handle message asynchronously
			go func(msg Message) {
				if err := fs.handleMessage(rpc.From, msg); err != nil {
					logging.Warn("[FileServer] failed to handle message", err, "listenAddr", fs.Transport.Addr())
				}
			}(msg)

		case <-fs.quitch:
			logging.Warn("[FileServer] closing...")
			return
		}
	}
}

// onDisconnectPeer handles disconnection of a peer.
func (fs *FileServer) onDisconnectPeer(peer p2p.Peer) {
	fs.peerLock.Lock()
	delete(fs.peers, peer.RemoteAddr().String())
	fs.peerLock.Unlock()

	logging.Info("[FileServer] peer disconnected", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String())
}

// onConnectPeer handles connection of a new peer.
func (fs *FileServer) onConnectPeer(peer p2p.Peer) {
	fs.peerLock.Lock()
	fs.peers[peer.RemoteAddr().String()] = peer
	fs.peerLock.Unlock()

	logging.Info("[FileServer] peer connected", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String())
}

// Stop stops the FileServer.
func (fs *FileServer) Stop() {
	close(fs.quitch)
}

// Store stores a file from the given reader to local storage and peers.
func (fs *FileServer) Store(key string, r io.Reader) (int64, error) {
	// Encrypt key
	encKey := fs.KeyEncrypter.Encrypt(key)

	// Buffer for storing file data
	fileBuffer := new(bytes.Buffer)
	tee := io.TeeReader(r, fileBuffer)

	// Write to local store
	n, err := fs.store.Write(encKey, tee)
	if err != nil {
		return 0, err
	}

	logging.Info("[FileServer] file stored to local storage.", "listenAddr", fs.Transport.Addr(), "size", n)

	// MultiWriter for writing to multiple peers
	mu := fs.multiWriter()

	// Prepare store message
	msg := Message{
		Payload: MessageStoreFile{
			Key:      encKey,
			FileSize: fs.Encrypter.Size(n),
		},
	}

	// Send store message
	if err := fs.sendMessage(mu, &msg); err != nil {
		return 0, err
	}

	// Notify incoming stream
	if _, err := mu.Write(p2p.LockMessage); err != nil {
		return 0, err
	}

	// Encrypt and send file data to peers
	if _, err := fs.Encrypter.Encrypt(fileBuffer, mu); err != nil {
		return 0, err
	}

	return n, nil
}

// Get retrieves a file identified by key from local storage or peers.
func (fs *FileServer) Get(key string) (int64, io.Reader, error) {
	// Encrypt key
	encKey := fs.KeyEncrypter.Encrypt(key)

	// Check if file exists in local storage
	if fs.store.Has(encKey) {
		return fs.store.Read(encKey)
	}

	logging.Info("[FileServer] file not found in local storage. trying to find from peers...", "listenAddr", fs.Transport.Addr())

	// Prepare message for requesting file from peers
	msg := Message{
		Payload: MessageGetFileRequest{
			Key: encKey,
		},
	}

	fileExists := false

	// Iterate through connected peers
	for _, peer := range fs.peers {

		// Send request to peer
		if err := fs.sendMessage(peer, &msg); err != nil {
			return 0, nil, err
		}

		logging.Info("[FileServer] requesting file from peer...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		// Read response from peer
		getFileResponse, err := fs.readGetFileResponse(peer)
		if err != nil {
			peer.UnLock()
			continue
		}

		// Check if file exists and is streamable
		if !getFileResponse.Exists {
			peer.UnLock()
			continue
		}

		logging.Info("[FileServer] peer has the file. reading...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String(), "size", getFileResponse.Size)

		// Write decrypted file data to local store
		if _, err := fs.store.WriteDecrypt(encKey, io.LimitReader(peer, getFileResponse.Size)); err != nil {
			peer.UnLock()
			return 0, nil, err
		}

		logging.Info("[FileServer] file read.", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		peer.UnLock()
		fileExists = true
		break
	}

	// Return file data from local store if found
	if fileExists {
		return fs.store.Read(encKey)
	}

	// Return error if file not found
	return 0, nil, ErrFileNotExists
}

// readMessage reads and decodes a message from the given reader.
func (fs *FileServer) readMessage(r io.Reader) (*Message, error) {
	// Read message size from binary
	var size int64
	binary.Read(r, binary.LittleEndian, &size)

	// Decode the message using MessageTransformer
	var msg Message
	if err := Transformer.Decode(io.LimitReader(r, size), &msg); err != nil {
		logging.ErrorE("[FileServer] failed to decode", err, "listenAddr", fs.Transport.Addr())
		return nil, err
	}
	return &msg, nil
}

func (fs *FileServer) readGetFileResponse(peer p2p.Peer) (*MessageGetFileResponse, error) {
	msg, err := fs.readMessage(peer)
	if err != nil {
		return nil, err
	}
	resp := msg.Payload.(MessageGetFileResponse)

	return &resp, nil
}

// sendMessage sends a message to the given writer.
func (fs *FileServer) sendMessage(w io.Writer, msg *Message) error {
	// Notify incoming message type
	if _, err := w.Write(p2p.TextMessage); err != nil {
		return err
	}

	// Encode and send message using MessageTransformer
	return Transformer.Encode(w, msg)
}

// multiWriter returns a writer that writes to all connected peers simultaneously.
func (fs *FileServer) multiWriter() io.Writer {
	peers := []io.Writer{}
	for _, peer := range fs.peers {
		peers = append(peers, peer)
	}
	return io.MultiWriter(peers...)
}
