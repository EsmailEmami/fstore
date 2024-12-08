package store

import (
	"context"
	"fmt"
	"io"
	"log"

	"github.com/esmailemami/fstore/pkg/logging"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3StoreOpts struct {
	PathTransformFunc PathTransformFunc

	RootPath string

	Endpoint   string
	AccessKey  string
	SecretKey  string
	Region     string
	BucketName string
	Secure     bool
}

type S3Store struct {
	S3StoreOpts

	client *minio.Client
}

// NewS3Store creates a new instance of S3Store.
func NewS3Store(opts S3StoreOpts) (*S3Store, error) {
	client, err := minio.New(opts.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(opts.AccessKey, opts.SecretKey, ""),
		Secure: opts.Secure,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	// Ensure the bucket exists (create if necessary)
	ctx := context.TODO()
	exists, err := client.BucketExists(ctx, opts.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}
	if !exists {
		err = client.MakeBucket(ctx, opts.BucketName, minio.MakeBucketOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	return &S3Store{
		client:      client,
		S3StoreOpts: opts,
	}, nil
}

// Has checks if the given key exists in the bucket.
func (s *S3Store) Has(key Key) bool {
	ctx := context.TODO()

	_, err := s.client.StatObject(ctx, s.BucketName, s.fullFilePath(key), minio.StatObjectOptions{})
	return err == nil
}

// Write uploads data to the bucket using the provided key.
func (s *S3Store) Write(key Key, r io.Reader, size int64) (int64, error) {
	ctx := context.TODO()

	// Stream data directly from the reader to the MinIO client
	info, err := s.client.PutObject(ctx, s.BucketName, s.fullFilePath(key), r, size, minio.PutObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to upload object: %w", err)
	}

	return info.Size, nil
}

// NewWriter returns an io.WriteCloser for direct writing to the bucket.
func (s *S3Store) NewWriter(key Key, size int64) (io.WriteCloser, error) {
	pipeReader, pipeWriter := io.Pipe()

	go func() {
		defer pipeReader.Close()
		_, err := s.client.PutObject(context.TODO(), s.BucketName, s.fullFilePath(key), pipeReader, size, minio.PutObjectOptions{})
		if err != nil {
			log.Printf("failed to upload object: %v", err)
		} else {
			logging.Debug("[S3 Store] writer stored successfully")
		}
	}()
	return pipeWriter, nil
}

// Read retrieves data from the bucket for the given key.
func (s *S3Store) Read(key Key) (int64, io.ReadCloser, error) {
	ctx := context.TODO()

	object, err := s.client.GetObject(ctx, s.BucketName, s.fullFilePath(key), minio.GetObjectOptions{})
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get object: %w", err)
	}

	// Stat the object to get its size
	info, err := object.Stat()
	if err != nil {
		object.Close()
		return 0, nil, fmt.Errorf("failed to stat object: %w", err)
	}

	return info.Size, object, nil
}

// Delete removes the object associated with the key from the bucket.
func (s *S3Store) Delete(key Key) error {
	ctx := context.TODO()

	err := s.client.RemoveObject(ctx, s.BucketName, s.fullFilePath(key), minio.RemoveObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

func (s *S3Store) fullFilePath(key Key) string {
	pathKey := s.PathTransformFunc(key)
	return pathKey.FullFilePath(s.RootPath)
}
