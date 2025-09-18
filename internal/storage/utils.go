package storage

import (
	"encoding/binary"
)

func addPrefix(prefix []byte, key []byte) []byte {
	return append(prefix, key...)
}

// Converts bytes to an integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
