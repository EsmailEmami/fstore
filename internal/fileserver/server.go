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

// Error definitions
var (
	ErrInvalidMessageType = errors.New("unexpected message")
	ErrFileNotExists      = errors.New("sorry. file not found")
	ErrPeerNotFound       = errors.New("peer not found")
)

type Peer struct {
	conn p2p.Peer

	Name          string
	TransportAddr string
	BaseAddr      string
}

func newPeer(peer p2p.Peer) *Peer {
	return &Peer{
		conn: peer,
	}
}

func (p *Peer) Read(b []byte) (n int, err error) {
	return p.conn.Read(b)
}
func (p *Peer) Write(b []byte) (n int, err error) {
	return p.conn.Write(b)
}

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
	FileServerOpts                  // Embedded configuration options
	store          *store.Store     // Local file storage
	peerLock       sync.RWMutex     // Mutex for peers map access
	peers          map[string]*Peer // Map of connected peers
	quitch         chan struct{}    // Channel for quitting
}

// NewFileServer creates a new FileServer instance with the provided options.
func NewFileServer(opts FileServerOpts) *FileServer {
	opts.prepare()

	// Configure store options
	storeOpts := store.StoreOpts{
		RootPath:          fmt.Sprintf("storage/server%s", opts.Transport.Addr()),
		PathTransformFunc: store.SHA256PathTransformFunc,
		//Encrypter:         opts.Encrypter,
	}

	// Create a new store instance
	store := store.NewStore(storeOpts)

	// Initialize FileServer instance
	fs := &FileServer{
		FileServerOpts: opts,
		peerLock:       sync.RWMutex{},
		peers:          make(map[string]*Peer),
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
	if err := fs.bootstrapNodes(); err != nil {
		return err
	}

	// Start listening for incoming messages
	go fs.listen()

	logging.Info("[FileServer] started.", "listenAddr", fs.Transport.Addr())

	return nil
}

// bootstrapNodes connects to the specified bootstrap nodes.
func (fs *FileServer) bootstrapNodes() error {
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
			// Decode and handle incoming message asynchronously
			go fs.handleRPC(rpc)
		case <-fs.quitch:
			logging.Warn("[FileServer] closing...")
			return
		}
	}
}

// handleRPC decodes and processes an incoming RPC message.
func (fs *FileServer) handleRPC(rpc p2p.RPC) {
	var msg Message

	// Decode incoming message
	if err := Transformer.Decode(bytes.NewReader(rpc.Payload), &msg); err != nil {
		if errors.Is(err, io.EOF) {
			return
		}
		logging.ErrorE("[FileServer] failed to decode", err, "listenAddr", fs.Transport.Addr(), "payload", rpc.Payload)
		return
	}

	// Handle message asynchronously
	if err := fs.handleMessage(rpc.From, msg); err != nil {
		logging.Warn("[FileServer] failed to handle message", err, "listenAddr", fs.Transport.Addr())
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
	fs.peers[peer.RemoteAddr().String()] = newPeer(peer)
	fs.peerLock.Unlock()

	identifyMessage := &Message{
		Payload: MessageIdentifyPeer{
			TransportAddr: fs.Transport.Addr(),
			Name:          "this is " + fs.Transport.Addr(),
			BaseAddr:      fs.Transport.Addr(),
		},
	}

	fs.sendMessage(peer, identifyMessage)

	logging.Info("[FileServer] peer connected", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String())
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

	// Prepare and send store message to peers
	if err := fs.storeFileToPeers(encKey, n, mu, fileBuffer); err != nil {
		return 0, err
	}

	return n, nil
}

// storeFileToPeers sends the store file message to all connected peers.
func (fs *FileServer) storeFileToPeers(encKey string, fileSize int64, mu io.Writer, fileBuffer *bytes.Buffer) error {
	// Prepare store message
	msg := Message{
		Payload: MessageStoreFile{
			Key:      encKey,
			FileSize: fs.Encrypter.Size(fileSize),
		},
	}

	// Send store message
	if err := fs.sendMessage(mu, &msg); err != nil {
		return err
	}

	// Notify incoming stream
	if _, err := mu.Write(p2p.LockMessage); err != nil {
		return err
	}

	// Encrypt and send file data to peers
	if _, err := fs.Encrypter.Encrypt(fileBuffer, mu); err != nil {
		return err
	}

	return nil
}

// Get retrieves a file identified by key from local storage or peers.
func (fs *FileServer) Get(key string) (int64, io.Reader, error) {
	encKey := fs.KeyEncrypter.Encrypt(key)

	// Check if file exists in local storage
	if fs.store.Has(encKey) {
		return fs.store.Read(encKey)
	}

	logging.Info("[FileServer] file not found in local storage. trying to find from peers...", "listenAddr", fs.Transport.Addr())

	// Try to retrieve from peers
	return fs.getFromPeers(encKey)
}

// getFromPeers attempts to retrieve the file from connected peers.
func (fs *FileServer) getFromPeers(encKey string) (int64, io.Reader, error) {
	fileExists := false

	// Iterate through connected peers
	for _, peer := range fs.peers {
		// Request file from peer
		msg := Message{
			Payload: MessageGetFileRequest{
				Key: encKey,
			},
		}

		if err := fs.sendMessage(peer, &msg); err != nil {
			return 0, nil, err
		}

		logging.Info("[FileServer] requesting file from peer...", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

		// Read response from peer
		getFileResponse, err := fs.readGetFileResponse(peer.conn)
		if err != nil || !getFileResponse.Exists {
			continue
		}

		logging.Info("[FileServer] peer has the file. reading...", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String(), "size", getFileResponse.Size)

		// Write decrypted file data to local store
		// if _, err := fs.store.WriteDecrypt(encKey, io.LimitReader(peer, getFileResponse.Size)); err != nil {
		// 	return 0, nil, err
		// }

		fileWriter, err := fs.store.NewFile(encKey)
		if err != nil {
			return 0, nil, err
		}
		defer fileWriter.Close()

		if _, err := fs.Encrypter.Decrypt(io.LimitReader(peer, getFileResponse.Size), fileWriter); err != nil {
			return 0, nil, err
		}

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

// readGetFileResponse reads and decodes the response message for file retrieval.
func (fs *FileServer) readGetFileResponse(peer p2p.Peer) (*MessageGetFileResponse, error) {
	msg, err := fs.readMessage(peer)
	if err != nil {
		return nil, err
	}
	resp := msg.Payload.(MessageGetFileResponse)

	return &resp, nil
}

// readMessage reads and decodes a message from the given reader.
func (fs *FileServer) readMessage(r io.Reader) (*Message, error) {
	var size int64
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}

	var msg Message
	if err := Transformer.Decode(io.LimitReader(r, size), &msg); err != nil {
		logging.ErrorE("[FileServer] failed to decode", err, "listenAddr", fs.Transport.Addr())
		return nil, err
	}
	return &msg, nil
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

// Stop stops the FileServer.
func (fs *FileServer) Stop() {
	close(fs.quitch)
}
