package store

import (
	"io"
)

type Store interface {
	Has(key Key) bool
	Write(key Key, r io.Reader, size int64) (int64, error)
	NewWriter(key Key, size int64) (io.WriteCloser, error)
	Read(key Key) (int64, io.ReadCloser, error)
	Delete(key Key) error
}
