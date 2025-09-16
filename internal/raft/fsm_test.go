package raft

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/kgantsov/dlock/internal/storage"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

type MemorySnapshotSink struct {
	buf    *bytes.Buffer
	closed bool
	id     string
	mu     sync.Mutex
}

func NewMemorySnapshotSink(id string) *MemorySnapshotSink {
	return &MemorySnapshotSink{
		buf: &bytes.Buffer{},
		id:  id,
	}
}

func (m *MemorySnapshotSink) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.buf.Write(p)
}

func (m *MemorySnapshotSink) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MemorySnapshotSink) Cancel() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.buf.Reset()
	return nil
}

func (m *MemorySnapshotSink) ID() string {
	return m.id
}

func (m *MemorySnapshotSink) Read(p []byte) (n int, err error) {
	return m.buf.Read(p)
}
func TestFSM_Snapshot(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir)

	tmpRaftDir, _ := os.MkdirTemp("", "store_test*")
	defer os.RemoveAll(tmpRaftDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	// Initialize the FSM with a test store
	// store, err := badgerdb.New(db, badgerdb.Options{})

	require.NoError(t, err)
	defer os.RemoveAll("/tmp/testdb") // Clean up
	storage := storage.NewBadgerStorage(db)

	fsm := &FSM{storage: storage}

	// Apply some commands to the FSM
	fsm.Apply(&raft.Log{Data: []byte(`{"Op": "acquire", "Key": "key1", "Time": "2023-06-01T12:00:00Z"}`)})
	fsm.Apply(&raft.Log{Data: []byte(`{"Op": "release", "Key": "key1"}`)})

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Verify the snapshot is not nil
	require.NotNil(t, snapshot)
}

func TestFSM_Restore(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	// Initialize the FSM with a test store
	store := storage.NewBadgerStorage(db)
	require.NoError(t, err)

	fsm := &FSM{storage: store}

	err = store.Acquire([]byte("test-lock-1"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)

	err = store.Acquire([]byte("test-lock-2"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)

	err = store.Acquire([]byte("test-lock-3"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)

	err = store.Acquire([]byte("test-lock-4"), time.Now().UTC().Add(time.Millisecond))
	require.NoError(t, err)

	err = store.Release([]byte("test-lock-1"))
	require.NoError(t, err)

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Simulate saving the snapshot to a buffer
	sink := NewMemorySnapshotSink("test-snapshot-id")
	err = snapshot.Persist(sink)
	require.NoError(t, err)

	// Close the current store and restore from the snapshot
	err = db.Close()
	require.NoError(t, err)

	tmpRestoreDir, _ := os.MkdirTemp("", "dbrestore*")
	defer os.RemoveAll(tmpRestoreDir)

	db, err = badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	store = storage.NewBadgerStorage(db)

	tmpDir2, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir2)
	// Reinitialize the store for restore

	fsm.storage = store

	// Restore from the snapshot
	err = fsm.Restore(io.NopCloser(sink))
	require.NoError(t, err)

	// Check if the state is correctly restored
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	err = fsm.storage.Acquire([]byte("test-lock-1"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)

	err = fsm.storage.Acquire([]byte("test-lock-2"), time.Now().UTC().Add(time.Second*10))
	require.Error(t, err)

	err = fsm.storage.Acquire([]byte("test-lock-3"), time.Now().UTC().Add(time.Second*10))
	require.Error(t, err)

	err = fsm.storage.Acquire([]byte("test-lock-4"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)
}
