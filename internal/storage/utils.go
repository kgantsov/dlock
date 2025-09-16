package storage

import (
	"encoding/binary"
	"errors"
)

var (
	ErrKeyNotFound         = errors.New("not found")
	ErrCouldNotAcquireLock = errors.New("could not acquire a lock")
	ErrCouldNotReleaseLock = errors.New("could not release a lock")
)

func addPrefix(prefix []byte, key []byte) []byte {
	return append(prefix, key...)
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
