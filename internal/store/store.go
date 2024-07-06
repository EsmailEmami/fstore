package store

import (
	"io"
	"os"

	"github.com/esmailemami/fstore/pkg/utils/security"
)

type StoreOpts struct {
	RootPath          string
	PathTransformFunc PathTransformFunc
	Encrypter         security.IOEncrypter
}

type Store struct {
	StoreOpts
}

func NewStore(opts StoreOpts) *Store {
	s := &Store{
		StoreOpts: opts,
	}
	return s
}

func (s *Store) Write(key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return 0, err
	}

	return io.Copy(f, r)
}

func (s *Store) WriteEncrypt(key string, r io.Reader) (int64, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return 0, err
	}

	return s.Encrypter.Encrypt(r, f)
}

func (s *Store) ReadEncrypt(key string, w io.Writer) (int64, error) {
	_, r, err := s.Read(key)
	if err != nil {
		return 0, err
	}
	defer r.Close()

	return s.Encrypter.Decrypt(r, w)
}

func (s *Store) Read(key string) (int64, io.ReadCloser, error) {
	pathKey := s.PathTransformFunc(key)
	filePath := pathKey.FullFilePath(s.RootPath)

	file, err := os.Open(filePath)
	if err != nil {
		return 0, nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		return 0, nil, err
	}

	return fi.Size(), file, nil
}

func (s *Store) openFileForWritting(key string) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	dirPath := pathKey.FullDirectoryPath(s.RootPath)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, err
	}

	filePath := pathKey.FullFilePath(s.RootPath)
	return os.Create(filePath)
}
