package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

type IOEncrypter interface {
	Size(originalSize int64) int64
	DecryptSize(encryptedSize int64) int64
	Encrypt(src io.Reader, dst io.Writer) (int64, error)
	Decrypt(src io.Reader, dst io.Writer) (int64, error)
}

// AESGCMEncrypter implements Encrypter using AES-GCM encryption.
type AESGCMIOEncrypter struct {
	key       []byte
	chunkSize int

	block cipher.Block
	gcm   cipher.AEAD
}

func NewAESGCMIOEncrypter(key []byte) *AESGCMIOEncrypter {
	if len(key) != 32 {
		panic("key length must be 32 bytes")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		panic(fmt.Errorf("failed to create AES cipher block: %v", err))
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(fmt.Errorf("failed to create AES-GCM cipher: %v", err))
	}

	return &AESGCMIOEncrypter{
		key:       key,
		chunkSize: 4 * 1024 * 1024,
		block:     block,
		gcm:       gcm,
	}
}

func (e *AESGCMIOEncrypter) Size(originalSize int64) int64 {
	// Calculate the number of chunks
	numChunks := (originalSize + int64(e.chunkSize) - 1) / int64(e.chunkSize)

	// Calculate the encrypted size
	encryptedSize := originalSize + int64(e.gcm.NonceSize()) + numChunks*int64(e.gcm.Overhead())

	return encryptedSize
}

func (e *AESGCMIOEncrypter) DecryptSize(encryptedSize int64) int64 {
	// Subtract the size of the nonce (added at the beginning of the encrypted data)
	remainingSize := encryptedSize - int64(e.gcm.NonceSize())
	if remainingSize < 0 {
		// If remaining size is less than 0, the encrypted data is invalid
		panic("invalid encrypted size")
	}

	// Calculate the number of chunks in the encrypted data
	overheadPerChunk := int64(e.gcm.Overhead())
	chunkDataSize := int64(e.chunkSize) + overheadPerChunk
	numChunks := (remainingSize + chunkDataSize - 1) / chunkDataSize

	// Subtract the overhead added by the GCM per chunk
	originalSize := remainingSize - numChunks*overheadPerChunk

	if originalSize < 0 {
		// If the calculated original size is negative, the input is invalid
		panic("invalid encrypted size or chunk size")
	}

	return originalSize
}

func (e *AESGCMIOEncrypter) Encrypt(src io.Reader, dst io.Writer) (int64, error) {
	// Generate nonce
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return 0, fmt.Errorf("failed to generate nonce: %v", err)
	}

	var totalBytesWritten int64 = int64(len(nonce))

	// Write nonce to the beginning of the encrypted data
	if _, err := dst.Write(nonce); err != nil {
		return 0, fmt.Errorf("failed to write nonce to destination: %v", err)
	}

	// Buffer for reading from source and encrypting in chunks
	buf := make([]byte, e.chunkSize) // 4 MB chunk size
	for {
		n, err := src.Read(buf)
		if err != nil && err != io.EOF {
			return totalBytesWritten, fmt.Errorf("error reading from source: %v", err)
		}
		if n == 0 {
			break
		}

		// Encrypt the data chunk
		ciphertext := e.gcm.Seal(nil, nonce, buf[:n], nil)

		// Write the encrypted chunk to the destination
		bytesWritten, err := dst.Write(ciphertext)
		if err != nil {
			return totalBytesWritten, fmt.Errorf("error writing encrypted data: %v", err)
		}
		totalBytesWritten += int64(bytesWritten)
	}

	return totalBytesWritten, nil
}

func (e *AESGCMIOEncrypter) Decrypt(src io.Reader, dst io.Writer) (int64, error) {
	var totalBytesWritten int64 = int64(e.gcm.NonceSize())

	// Read nonce from the beginning of the encrypted data
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(src, nonce); err != nil {
		return 0, fmt.Errorf("failed to read nonce from source: %v", err)
	}

	// Buffer for reading encrypted chunks
	buf := make([]byte, 4*1024*1024) // 4 MB chunk size
	for {
		n, err := src.Read(buf)
		if err != nil && err != io.EOF {
			return totalBytesWritten, fmt.Errorf("error reading from source: %v", err)
		}
		if n == 0 {
			break
		}

		// Decrypt the data chunk
		plaintext, err := e.gcm.Open(nil, nonce, buf[:n], nil)
		if err != nil {
			return totalBytesWritten, fmt.Errorf("error decrypting data chunk: %v", err)
		}

		// Write the decrypted chunk to the destination
		bytesWritten, err := dst.Write(plaintext)
		if err != nil {
			return totalBytesWritten, fmt.Errorf("error writing decrypted data: %v", err)
		}
		totalBytesWritten += int64(bytesWritten)
	}

	return totalBytesWritten, nil
}
