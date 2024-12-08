package store

import (
	"errors"
	"io"
	"os"
)

// Error definitions
var (
	ErrFileNotExists = errors.New("sorry. file not found")
)

type PhysicalStoreOpts struct {
	RootPath          string
	PathTransformFunc PathTransformFunc
}

type PhysicalStore struct {
	PhysicalStoreOpts
}

func NewPhysicalStore(opts PhysicalStoreOpts) *PhysicalStore {
	s := &PhysicalStore{
		PhysicalStoreOpts: opts,
	}
	return s
}

func (s *PhysicalStore) Has(key Key) bool {
	filepath := s.fullFilePath(key)
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *PhysicalStore) Write(key Key, r io.Reader, size int64) (int64, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return 0, err
	}

	if size == -1 {
		return io.Copy(f, r)
	} else {
		return io.CopyN(f, r, size)
	}
}

func (s *PhysicalStore) NewWriter(key Key, size int64) (io.WriteCloser, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (s *PhysicalStore) Read(key Key) (int64, io.ReadCloser, error) {
	filePath := s.fullFilePath(key)
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

func (s *PhysicalStore) Delete(key Key) error {
	if !s.Has(key) {
		return ErrFileNotExists
	}
	filepath := s.fullFilePath(key)

	return os.Remove(filepath)
}

func (s *PhysicalStore) openFileForWritting(key Key) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	dirPath := pathKey.FullDirectoryPath(s.RootPath)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, err
	}

	filePath := pathKey.FullFilePath(s.RootPath)
	return os.Create(filePath)
}

func (s *PhysicalStore) fullFilePath(key Key) string {
	pathKey := s.PathTransformFunc(key)
	return pathKey.FullFilePath(s.RootPath)
}
