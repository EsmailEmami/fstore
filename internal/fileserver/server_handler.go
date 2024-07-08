package fileserver

import (
	"encoding/gob"
	"fmt"
	"io"

	"github.com/esmailemami/fstore/internal/p2p"
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
	}

	// Return error for unexpected message type
	return ErrInvalidMessageType
}

// handleMessageGetFileRequest handles a request to get a file from a peer.
func (fs *FileServer) handleMessageGetFileRequest(peer p2p.Peer, msg MessageGetFileRequest) error {
	logging.Info("message get file called.", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String(), "storeAddr", fs.store.RootPath)

	// Lock the peer for sending a response
	if err := peer.Lock(); err != nil {
		return err
	}

	resp := MessageGetFileResponse{}

	// Check if file exists in local store
	if !fs.store.Has(msg.Key) {
		logging.Info("file does not exist", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

		// Set response flags for non-existent file
		resp.Exists = false
		resp.Stream = false

		// Encode and send response to peer
		if err := fs.MessageTransformer.Encode(peer, &Message{Payload: resp}); err != nil {
			return err
		}

		return nil
	}

	logging.Info("file exists. processing...", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	// Read file data from local store
	n, f, err := fs.store.Read(msg.Key)
	if err != nil {
		return err
	}
	defer f.Close()

	logging.Info("file read", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	// Set response flags for existing file
	resp.Exists = true
	resp.Size = n
	resp.Stream = true

	// Encode and send response to peer
	if err := fs.MessageTransformer.Encode(peer, &Message{Payload: resp}); err != nil {
		return err
	}

	logging.Info("writing file to peer", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	// Copy file data to peer
	if _, err := io.Copy(peer, f); err != nil {
		return err
	}

	logging.Info("file transfer completed", "listenAddr", fs.Transport.Addr(), "peer", peer.RemoteAddr().String())

	return nil
}

// handleMessageStoreFile handles a request to store a file from a peer.
func (fs *FileServer) handleMessageStoreFile(peer p2p.Peer, msg MessageStoreFile) error {
	logging.Debug("File Server store called. reading...", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String(), "size", msg.FileSize)

	// Write file data received from peer to local store
	n, err := fs.store.Write(msg.Key, io.LimitReader(peer, msg.FileSize))

	logging.Debug("File Server store called. read done", "listenAddr", fs.Transport.Addr(), "peerAddr", peer.RemoteAddr().String(), "writtenSize", n, "calledSize", msg.FileSize)

	// Unlock peer after writing file
	peer.UnLock()

	return err
}

// MessageGetFileRequest defines the structure of a file retrieval request message.
type MessageGetFileRequest struct {
	Key string
}

// MessageGetFileResponse defines the structure of a file retrieval response message.
type MessageGetFileResponse struct {
	Exists bool
	Size   int64
	Stream bool
}

// MessageStoreFile defines the structure of a file storage message.
type MessageStoreFile struct {
	Key      string
	FileSize int64
}

// init registers message types with gob for serialization.
func init() {
	gob.Register(MessageGetFileRequest{})
	gob.Register(MessageGetFileResponse{})
	gob.Register(MessageStoreFile{})
}
