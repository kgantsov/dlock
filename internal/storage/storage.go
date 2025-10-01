package storage

import (
	"io"
	"time"

	"github.com/kgantsov/dlock/internal/domain"
)

type Storage interface {
	// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
	Acquire(key, owner string, fencingToken uint64, expireAt time.Time) (*domain.LockEntry, error)

	// Release releases a lock for the given key.
	Release(key, owner string, fencingToken uint64) error

	// Renew renews a lock for the given key.
	Renew(key, owner string, fencingToken uint64, expireAt time.Time) (*domain.LockEntry, error)

	PersistSnapshot(w io.Writer) error
	Close() error
}
