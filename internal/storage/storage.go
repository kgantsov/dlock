package storage

import (
	"io"
	"time"
)

type Storage interface {
	// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
	Acquire(key []byte, expireAt time.Time) error

	// Release releases a lock for the given key.
	Release(key []byte) error

	PersistSnapshot(w io.Writer) error
	Close() error
}
