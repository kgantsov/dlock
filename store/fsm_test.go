package store

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/badger-store"
	"github.com/stretchr/testify/assert"
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
	// Initialize the FSM with a test store
	store, err := badgerdb.New(badgerdb.Options{
		Path: "/tmp/testdb",
	})
	require.NoError(t, err)
	defer os.RemoveAll("/tmp/testdb") // Clean up

	fsm := &FSM{store: store}

	// Apply some commands to the FSM
	acquireCommand1 := &command{
		Op:   "acquire",
		Key:  "test-lock-1",
		Time: time.Now().UTC().Add(time.Second * time.Duration(10)).Format(time.RFC3339),
	}
	acquireCommand1Binary, _ := json.Marshal(acquireCommand1)
	releaseCommand1 := &command{
		Op:   "release",
		Key:  "test-lock-2",
		Time: time.Now().UTC().Add(time.Second * time.Duration(10)).Format(time.RFC3339),
	}
	releaseCommand1Binary, _ := json.Marshal(releaseCommand1)

	err = store.StoreLogs([]*raft.Log{
		{Index: 1, Term: 1, Data: releaseCommand1Binary},
		{Index: 2, Term: 1, Data: acquireCommand1Binary},
	})
	require.NoError(t, err)

	// Create a snapshot
	snapshot, err := fsm.Snapshot()
	require.NoError(t, err)

	// Simulate saving the snapshot to a buffer
	sink := NewMemorySnapshotSink("test-snapshot-id")
	err = snapshot.Persist(sink)
	require.NoError(t, err)

	assert.Equal(
		t, string(releaseCommand1Binary)+"\n"+string(acquireCommand1Binary)+"\n", sink.buf.String(),
	)

	// Close the current store and restore from the snapshot
	err = fsm.store.Close()
	require.NoError(t, err)

	// Reinitialize the store for restore
	restoreStore, err := badgerdb.New(badgerdb.Options{
		Path: "/tmp/newtestdb",
	})
	require.NoError(t, err)
	fsm.store = restoreStore

	// Restore from the snapshot
	err = fsm.Restore(io.NopCloser(sink))
	require.NoError(t, err)

	// Check if the state is correctly restored
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	err = fsm.store.Acquire([]byte(acquireCommand1.Key), time.Now().UTC().Add(time.Second*10))
	require.Error(t, err)
}
