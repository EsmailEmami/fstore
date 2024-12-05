package store

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/esmailemami/fstore/pkg/utils/security"
	"github.com/stretchr/testify/assert"
)

func TestStoreWriteAndReadEncrypted(t *testing.T) {
	opts := StoreOpts{
		RootPath:          "test_esi_network",
		PathTransformFunc: SHA256PathTransformFunc,
		Encrypter:         security.NewAESGCMIOEncrypter([]byte("thisis32bitlongpassphraseimusing")),
	}
	store := NewStore(opts)

	key := "myFile.jpg"
	buf := bytes.NewReader([]byte("oh this is my nude!!!"))

	n, err := store.WriteEncrypt(key, buf)

	assert.Nil(t, err)
	fmt.Printf("written size: %d\n", n)

	fileBuf := new(bytes.Buffer)
	wn, err := store.ReadEncrypt(key, fileBuf)
	assert.Nil(t, err)
	assert.Equal(t, "oh this is my nude!!!", fileBuf.String())

	fmt.Printf("read size: %d\n", wn)
}

func TestStoreWriteAndRead(t *testing.T) {
	opts := StoreOpts{
		RootPath:          "test_esi_network",
		PathTransformFunc: SHA256PathTransformFunc,
	}
	store := NewStore(opts)

	key := "myFile.jpg"
	buf := bytes.NewReader([]byte("oh this is my nude!!!"))

	n, err := store.Write(key, buf)

	assert.Nil(t, err)
	fmt.Printf("written size: %d\n", n)

	// fileBuf := new(bytes.Buffer)
	// wn, err := store.ReadEncrypt(encKey, key, fileBuf)
	// assert.Nil(t, err)
	// assert.Equal(t, "oh this is my nude!!!", fileBuf.String())

	// fmt.Printf("read size: %d\n", wn)

	_, r, err := store.Read(key)
	assert.Nil(t, err)
	defer r.Close()

	fBuf, err := io.ReadAll(r)

	assert.Nil(t, err)
	assert.Equal(t, "oh this is my nude!!!", string(fBuf))
}
