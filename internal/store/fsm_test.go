package store

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/internal/badger-store"
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
	// Initialize the FSM with a test store
	store, err := badgerdb.New(badgerdb.Options{
		Path: "/tmp/testdb",
	})
	require.NoError(t, err)
	defer os.RemoveAll("/tmp/testdb") // Clean up

	fsm := &FSM{store: store}

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
	// Initialize the FSM with a test store
	store, err := badgerdb.New(badgerdb.Options{
		Path: tmpDir,
	})
	require.NoError(t, err)

	fsm := &FSM{store: store}

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
	err = fsm.store.Close()
	require.NoError(t, err)

	tmpDir2, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir2)
	// Reinitialize the store for restore
	restoreStore, err := badgerdb.New(badgerdb.Options{
		Path: tmpDir2,
	})
	require.NoError(t, err)
	fsm.store = restoreStore

	// Restore from the snapshot
	err = fsm.Restore(io.NopCloser(sink))
	require.NoError(t, err)

	// Check if the state is correctly restored
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	err = fsm.store.Acquire([]byte("test-lock-1"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)

	err = fsm.store.Acquire([]byte("test-lock-2"), time.Now().UTC().Add(time.Second*10))
	require.Error(t, err)

	err = fsm.store.Acquire([]byte("test-lock-3"), time.Now().UTC().Add(time.Second*10))
	require.Error(t, err)

	err = fsm.store.Acquire([]byte("test-lock-4"), time.Now().UTC().Add(time.Second*10))
	require.NoError(t, err)
}
