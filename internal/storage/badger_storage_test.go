package storage

import (
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/kgantsov/dlock/internal/domain"
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

	_, err = store.Acquire("my-lock:1", "owner-1", 436, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	_, err = store.Acquire("my-lock:1", "owner-1", 436, time.Now().UTC().Add(time.Second*1))
	assert.Equal(t, domain.ErrLockAlreadyAcquired, err)

	_, err = store.Acquire("my-lock:2", "owner-2", 437, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Release("my-lock:1", "owner-1", 435)
	require.Error(t, err)
	assert.Equal(t, domain.ErrFencingTokenMismatch, err)

	err = store.Release("my-lock:1", "owner-2", 436)
	require.Error(t, err)
	assert.Equal(t, domain.ErrOwnerMismatch, err)

	err = store.Release("my-lock:1", "owner-1", 436)
	require.NoError(t, err)

	_, err = store.Acquire("my-lock:1", "owner-1", 438, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	err = store.Release("my-lock:2", "owner-2", 437)
	require.NoError(t, err)

	err = store.Release("my-lock:1", "owner-1", 438)
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

	_, err = store.Acquire("my-lock:1", "", 0, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	_, err = store.Acquire("my-lock:1", "", 0, time.Now().UTC().Add(time.Second*1))
	assert.Equal(t, domain.ErrLockAlreadyAcquired, err)

	time.Sleep(1 * time.Second)

	_, err = store.Acquire("my-lock:1", "", 0, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)
}

func TestBadgerStore_Acquire_Renew(t *testing.T) {
	opts := badger.DefaultOptions("").WithInMemory(true)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}

	store := NewBadgerStorage(db)
	defer store.Close()

	_, err = store.Renew("my-lock:1", "owner-1", 1241, time.Now().UTC().Add(time.Second*1))
	require.Equal(t, domain.ErrLockNotFound, err)

	_, err = store.Acquire("my-lock:1", "owner-1", 1241, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	_, err = store.Renew("my-lock:1", "owner-1", 1241, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	_, err = store.Renew("my-lock:1", "owner-1", 1241, time.Now().UTC().Add(time.Second*1))
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

	_, err = store.Acquire("my-lock:1", "", 0, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	_, err = store.Acquire("my-lock:2", "", 0, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	_, err = store.Acquire("my-lock:3", "", 0, time.Now().UTC().Add(time.Second*1))
	require.NoError(t, err)

	locks := store.Locks()

	assert.Len(t, locks, 3)
}
