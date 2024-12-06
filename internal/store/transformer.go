package store

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strings"
)

type PathTransformFunc func(string) PathKey

type PathKey struct {
	FilePath string
	FileName string
}

func (p PathKey) FirstPathName() string {
	paths := strings.Split(p.FilePath, "/")
	if len(paths) == 0 {
		return ""
	}
	return paths[0]
}

func (p PathKey) FullDirectoryPath(prefix ...string) string {
	pathSlice := []string{}
	pathSlice = append(pathSlice, prefix...)
	pathSlice = append(pathSlice, p.FilePath)
	return path.Join(pathSlice...)
}

func (p PathKey) FullFilePath(prefix ...string) string {
	pathSlice := []string{}
	pathSlice = append(pathSlice, prefix...)
	pathSlice = append(pathSlice, p.FilePath, p.FileName)
	return path.Join(pathSlice...)
}

func SHA1PathTransformFunc(key string) PathKey {
	prefix, key := parseKey(key)

	hash := sha1.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 5
	hashLen := len(hashStr)
	sliceLen := (hashLen + blocksize - 1) / blocksize
	paths := make([]string, 0, sliceLen+1)

	if len(prefix) > 0 {
		paths = append(paths, prefix)
	}

	for i := 0; i < hashLen; i += blocksize {
		end := i + blocksize
		if end > hashLen {
			end = hashLen
		}
		paths = append(paths, hashStr[i:end])
	}

	return PathKey{
		FilePath: strings.Join(paths, "/"),
		FileName: hashStr,
	}
}
func SHA256PathTransformFunc(key string) PathKey {
	prefix, key := parseKey(key)

	hash := sha256.Sum256([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	blocksize := 6
	hashLen := len(hashStr)
	sliceLen := (hashLen + blocksize - 1) / blocksize
	paths := make([]string, 0, sliceLen+1)
	if len(prefix) > 0 {
		paths = append(paths, prefix)
	}
	for i := 0; i < hashLen; i += blocksize {
		end := i + blocksize
		if end > hashLen {
			end = hashLen
		}
		paths = append(paths, hashStr[i:end])
	}

	return PathKey{
		FilePath: strings.Join(paths, "/"),
		FileName: hashStr,
	}
}

func MD5PathTransformFunc(key string) PathKey {
	prefix, key := parseKey(key)

	hash := md5.Sum([]byte(key))
	hashStr := hex.EncodeToString(hash[:])

	delimiters := []int{3, 4} // Alternating segment lengths of 3 and 4
	hashLen := len(hashStr)
	var paths []string

	if len(prefix) > 0 {
		paths = append(paths, prefix)
	}

	pos := 0

	for i := 0; pos < hashLen; i++ {
		segmentLen := delimiters[i%len(delimiters)]
		end := pos + segmentLen
		if end > hashLen {
			end = hashLen
		}
		paths = append(paths, hashStr[pos:end])
		pos = end
	}

	return PathKey{
		FilePath: strings.Join(paths, "/"),
		FileName: hashStr,
	}
}

func parseKey(key string) (string, string) {
	prefixArr := strings.Split(key, "/")

	var prefix string
	if len(prefixArr) > 1 {
		prefix = prefixArr[0]
		key = strings.TrimPrefix(key, prefix)
	}

	return prefix, key
}
