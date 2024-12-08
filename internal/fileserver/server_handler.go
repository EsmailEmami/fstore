package fileserver

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/esmailemami/fstore/internal/store"
	"github.com/esmailemami/fstore/pkg/logging"
)

// handleMessage handles incoming messages from peers.
func (fs *FileServer) handleMessage(from string, msg Message) error {
	// Retrieve peer from map using from address
	fs.peerLock.RLock()
	peer, ok := fs.peers[from]
	fs.peerLock.RUnlock()

	// If peer not found, return an error
	if !ok {
		return fmt.Errorf("peer with addr (%s) %w", from, ErrPeerNotFound)
	}

	// Switch on the type of payload in the message
	switch payload := msg.Payload.(type) {
	case MessageGetFileRequest:
		return fs.handleMessageGetFileRequest(peer, payload)
	case MessageStoreFile:
		return fs.handleMessageStoreFile(peer, payload)
	case MessageIdentifyPeer:
		return fs.handleMessageIdentifyPeer(peer, payload)
	case MessageDeleteFile:
		return fs.handleMessageDeleteFile(peer, payload)
	}

	// Return error for unexpected message type
	return ErrInvalidMessageType
}

// handleMessageGetFileRequest handles a request to get a file from a peer.
func (fs *FileServer) handleMessageGetFileRequest(peer *Peer, msg MessageGetFileRequest) error {
	logging.Info("message get file called.", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Lock the peer for sending a response
	if err := peer.conn.Lock(); err != nil {
		return err
	}

	resp := MessageGetFileResponse{}

	// Check if file exists in local store
	if !fs.store.Has(store.NewKey(peer.BaseAddr, msg.Key)) {
		logging.Info("file does not exist", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

		// Set response flags for non-existent file
		resp.Exists = false

		// Encode and send response to peer
		if err := Transformer.Encode(peer, &Message{Payload: resp}); err != nil {
			return err
		}

		return nil
	}

	logging.Info("file exists. processing...", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Read file data from local store
	n, f, err := fs.store.Read(store.NewKey(peer.BaseAddr, msg.Key))
	if err != nil {
		return err
	}
	defer f.Close()

	logging.Info("file read", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Set response flags for existing file
	resp.Exists = true
	//resp.Size = n

	resp.Size = fs.Encrypter.DecryptSize(n)

	// Encode and send response to peer
	if err := Transformer.Encode(peer, &Message{Payload: resp}); err != nil {
		return err
	}

	logging.Info("writing file to peer", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Copy file data to peer

	// if _, err := io.Copy(peer, f); err != nil {
	// 	return err
	// }

	if _, err := fs.Encrypter.Decrypt(f, peer); err != nil {
		return err
	}

	logging.Info("file transfer completed", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	return nil
}

// handleMessageStoreFile handles a request to store a file from a peer.
func (fs *FileServer) handleMessageStoreFile(peer *Peer, msg MessageStoreFile) error {
	defer peer.conn.UnLock()

	logging.Debug("File Server store called. reading...", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.conn.RemoteAddr().String(), "size", msg.FileSize)

	fileWriter, err := fs.store.NewWriter(store.NewKey(peer.BaseAddr, msg.Key), fs.Encrypter.Size(msg.FileSize))
	if err != nil {
		return err
	}
	defer fileWriter.Close()

	// Write file data received from peer to local store
	n, err := fs.Encrypter.Encrypt(io.LimitReader(peer, msg.FileSize), fileWriter)

	logging.Debug("File Server store called. read done", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.conn.RemoteAddr().String(), "writtenSize", n, "calledSize", msg.FileSize)

	// Unlock peer after writing file

	return err
}

func (fs *FileServer) handleMessageIdentifyPeer(peer *Peer, msg MessageIdentifyPeer) error {
	fs.peerLock.Lock()
	defer fs.peerLock.Unlock()

	peer.Name = msg.Name
	peer.BaseAddr = msg.BaseAddr
	peer.TransportAddr = msg.TransportAddr

	return nil
}

func (fs *FileServer) handleMessageDeleteFile(peer *Peer, msg MessageDeleteFile) error {
	logging.Info("message delete file called.", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Check if file exists in local store
	if !fs.store.Has(store.NewKey(peer.BaseAddr, msg.Key)) {
		logging.Info("file does not exist", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

		return nil
	}

	logging.Info("file exists. processing...", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	// Read file data from local store
	err := fs.store.Delete(store.NewKey(peer.BaseAddr, msg.Key))
	if err != nil {
		return err
	}

	logging.Info("file deleted successfully", "listenAddr", fs.Transport.Addr(), "peer", peer.conn.RemoteAddr().String())

	return nil
}

// MessageGetFileRequest defines the structure of a file retrieval request message.
type MessageGetFileRequest struct {
	Key string
}

// MessageGetFileResponse defines the structure of a file retrieval response message.
type MessageGetFileResponse struct {
	Exists bool
	Size   int64
}

// MessageStoreFile defines the structure of a file storage message.
type MessageStoreFile struct {
	Key      string
	FileSize int64
}

// MessageIdentifyPeer defines the peer information
type MessageIdentifyPeer struct {
	TransportAddr string
	Name          string
	BaseAddr      string
}

// MessageDeleteFilet defines the structure of a file message.
type MessageDeleteFile struct {
	Key string
}

// init registers message types with gob for serialization.
func init() {
	gob.Register(MessageGetFileRequest{})
	gob.Register(MessageGetFileResponse{})
	gob.Register(MessageStoreFile{})
	gob.Register(MessageIdentifyPeer{})
	gob.Register(MessageDeleteFile{})
}
