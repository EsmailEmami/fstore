package store

import (
	"errors"
	"io"
	"os"
)

type StoreOpts struct {
	RootPath          string
	PathTransformFunc PathTransformFunc
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

func (s *Store) Has(key Key) bool {
	filepath := s.fullFilePath(key)
	_, err := os.Stat(filepath)
	return !errors.Is(err, os.ErrNotExist)
}

func (s *Store) Write(key Key, r io.Reader) (int64, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return 0, err
	}

	return io.Copy(f, r)
}

func (s *Store) NewFile(key Key) (io.WriteCloser, error) {
	f, err := s.openFileForWritting(key)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (s *Store) Read(key Key) (int64, io.ReadCloser, error) {
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

func (s *Store) openFileForWritting(key Key) (*os.File, error) {
	pathKey := s.PathTransformFunc(key)
	dirPath := pathKey.FullDirectoryPath(s.RootPath)

	if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
		return nil, err
	}

	filePath := pathKey.FullFilePath(s.RootPath)
	return os.Create(filePath)
}

func (s *Store) fullFilePath(key Key) string {
	pathKey := s.PathTransformFunc(key)
	return pathKey.FullFilePath(s.RootPath)
}
