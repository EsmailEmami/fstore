package fileserver

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

var (
	ErrCipherTextTooShort = errors.New("ciphertext too short")
)

type MessageTransformer interface {
	Encode(w io.Writer, msg *Message) error
	Decode(r io.Reader, msg *Message) error
}

type GOBMessageTransformer struct {
}

func (GOBMessageTransformer) Encode(w io.Writer, msg *Message) error {
	return gob.NewEncoder(w).Encode(msg)
}

func (GOBMessageTransformer) Decode(r io.Reader, msg *Message) error {
	return gob.NewDecoder(r).Decode(msg)
}

type AESGCMMessageTransformer struct {
	Key []byte
}

func NewAESGCMMessageTransformer(key []byte) *AESGCMMessageTransformer {
	if len(key) != 32 {
		panic("key length must be 32 bytes")
	}
	return &AESGCMMessageTransformer{Key: key}
}

func (se *AESGCMMessageTransformer) Encode(w io.Writer, msg *Message) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return err
	}

	plaintext := buf.Bytes()

	block, err := aes.NewCipher(se.Key)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)

	_, err = w.Write(ciphertext)
	return err
}

func (se *AESGCMMessageTransformer) Decode(r io.Reader, msg *Message) error {
	ciphertext, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	block, err := aes.NewCipher(se.Key)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return ErrCipherTextTooShort
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(plaintext)
	dec := gob.NewDecoder(buf)
	return dec.Decode(msg)
}

type ChaCha20Poly1305MessageTransformer struct {
	Key []byte
}

func NewChaCha20Poly1305MessageTransformer(key []byte) *ChaCha20Poly1305MessageTransformer {
	if len(key) != chacha20poly1305.KeySize {
		panic(fmt.Sprintf("key length must be %d bytes", chacha20poly1305.KeySize))
	}
	return &ChaCha20Poly1305MessageTransformer{Key: key}
}

func (e *ChaCha20Poly1305MessageTransformer) Encode(w io.Writer, msg *Message) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(msg); err != nil {
		return err
	}

	plaintext := buf.Bytes()

	aead, err := chacha20poly1305.New(e.Key)
	if err != nil {
		return err
	}

	nonce := make([]byte, chacha20poly1305.NonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)

	_, err = w.Write(ciphertext)
	return err
}

func (e *ChaCha20Poly1305MessageTransformer) Decode(r io.Reader, msg *Message) error {
	ciphertext, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	aead, err := chacha20poly1305.New(e.Key)
	if err != nil {
		return err
	}

	nonceSize := chacha20poly1305.NonceSize
	if len(ciphertext) < nonceSize {
		return ErrCipherTextTooShort
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(plaintext)
	dec := gob.NewDecoder(buf)
	return dec.Decode(msg)
}
