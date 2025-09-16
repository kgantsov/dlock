package raft

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// TestStoreOpen tests that the store can be opened.
func TestStoreOpen(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	n := NewNode(db)

	n.RaftBind = "127.0.0.1:0"

	assert.NotNil(t, n)

	err = n.Open(false, "node0")
	require.NoError(t, err)
}

// TestStoreOpenSingleNode tests that a command can be applied to the log
func TestStoreOpenSingleNode(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	becomeLeader := false
	n := NewNode(db)
	n.SetLeaderChangeFunc(func(isLeader bool) {
		if isLeader {
			becomeLeader = true
		}
	})

	n.RaftBind = "127.0.0.1:0"
	n.RaftDir = tmpDir

	assert.NotNil(t, n)

	err = n.Open(true, "node0")
	require.NoError(t, err)

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)
	assert.True(t, becomeLeader)

	err = n.Acquire("foo", 60)
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = n.Acquire("foo", 60)
	require.Error(t, err)

	err = n.Release("foo")
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = n.Acquire("foo", 60)
	require.NoError(t, err)
}

// TestStoreOpenSingleNodeWithTTL tests that a command can be applied to the log
func TestStoreOpenSingleNodeWithTTL(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "db*")
	defer os.RemoveAll(tmpDir)

	opts := badger.DefaultOptions(tmpDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	n := NewNode(db)

	n.RaftBind = "127.0.0.1:0"
	n.RaftDir = tmpDir

	assert.NotNil(t, n)

	err = n.Open(true, "node0")
	require.NoError(t, err)

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	err = n.Acquire("foo", 2)
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = n.Acquire("foo", 2)
	assert.Error(t, err)

	time.Sleep(2 * time.Second)
	err = n.Acquire("foo", 2)
	require.NoError(t, err)
}

type MockStore struct {
	mock.Mock
}

func (m *MockStore) Close() error {
	args := m.Called()
	return args.Error(0)
}
func (m *MockStore) FirstIndex() (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}
func (m *MockStore) LastIndex() (uint64, error) {
	args := m.Called()
	return args.Get(0).(uint64), args.Error(1)
}
func (m *MockStore) GetLog(idx uint64, log *raft.Log) error {
	args := m.Called(idx, log)
	return args.Error(0)
}
func (m *MockStore) StoreLog(log *raft.Log) error {
	args := m.Called(log)
	return args.Error(0)
}
func (m *MockStore) StoreLogs(logs []*raft.Log) error {
	args := m.Called(logs)
	return args.Error(0)
}
func (m *MockStore) DeleteRange(min, max uint64) error {
	args := m.Called(min, max)
	return args.Error(0)
}
func (m *MockStore) PersistSnapshot(w io.Writer) error {
	args := m.Called(w)
	return args.Error(0)
}
func (m *MockStore) Set(k, v []byte) error {
	args := m.Called(k, v)
	return args.Error(0)
}
func (m *MockStore) Get(k []byte) ([]byte, error) {
	args := m.Called(k)
	return args.Get(0).([]byte), args.Error(0)
}
func (m *MockStore) Acquire(k []byte, expireAt time.Time) error {
	args := m.Called(k, expireAt)
	return args.Error(0)
}
func (m *MockStore) Release(k []byte) error {
	args := m.Called(k)
	return args.Error(0)
}
func (m *MockStore) SetUint64(key []byte, val uint64) error {
	args := m.Called(key, val)
	return args.Error(0)
}
func (m *MockStore) GetUint64(key []byte) (uint64, error) {
	args := m.Called(key)
	return args.Get(0).(uint64), args.Error(1)
}
func (m *MockStore) DBPath() string {
	args := m.Called()
	return args.String(0)
}
func (m *MockStore) RunValueLogGC(discardRatio float64) error {
	args := m.Called(discardRatio)
	return args.Error(0)
}
func (m *MockStore) Size() (lsm, vlog int64) {
	args := m.Called()
	return args.Get(0).(int64), args.Get(1).(int64)
}
func (m *MockStore) Locks() []string {
	args := m.Called()
	return args.Get(0).([]string)
}
