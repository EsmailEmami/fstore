package security

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
)

type TextEncrypter interface {
	Encrypt(string) string
}

type MD5Encrypter struct{}

func (m *MD5Encrypter) Encrypt(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type SHA1TextEncrypter struct{}

func (s *SHA1TextEncrypter) Encrypt(text string) string {
	hash := sha1.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

type SHA256TextEncrypter struct{}

func (s *SHA256TextEncrypter) Encrypt(text string) string {
	hash := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hash[:])
}

type SHA512TextEncrypter struct{}

func (s *SHA512TextEncrypter) Encrypt(text string) string {
	hash := sha512.Sum512([]byte(text))
	return hex.EncodeToString(hash[:])
}
