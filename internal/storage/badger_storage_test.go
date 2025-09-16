package storage

import (
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBadgerStore_Acquire_Release(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "store")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	// Successfully creates and returns a store
	store := NewBadgerStorage(db)

	defer store.Close()

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	assert.Equal(t, ErrCouldNotAcquireLock, err)

	err = store.Acquire([]byte("my-lock:2"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Release([]byte("my-lock:1"))
	require.NoError(t, err)

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)
}

func TestBadgerStore_Acquire_Release_WithTTL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "store")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	// Successfully creates and returns a store
	store := NewBadgerStorage(db)
	defer store.Close()

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	assert.Equal(t, ErrCouldNotAcquireLock, err)

	time.Sleep(1 * time.Second)

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)
}

func TestBadgerStore_Locks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "store")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	// Successfully creates and returns a store
	store := NewBadgerStorage(db)
	defer store.Close()

	err = store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Acquire([]byte("my-lock:2"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Acquire([]byte("my-lock:3"), time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	locks := store.Locks()

	assert.Len(t, locks, 3)
}
