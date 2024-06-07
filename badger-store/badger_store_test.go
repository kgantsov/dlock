package badgerstore

import (
	"bytes"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBadgerStore(t testing.TB) *BadgerStore {
	dirname, err := os.MkdirTemp("", "store")
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	os.Remove(dirname)

	// Successfully creates and returns a store
	store, err := NewBadgerStore(&logrus.Logger{}, dirname)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestBadgerStore_Implements(t *testing.T) {
	var store interface{} = &BadgerStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("BadgerStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("BadgerStore does not implement raft.LogStore")
	}
}

func TestBadgerStore_FirstIndex(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBadgerStore_LastIndex(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

func TestBadgerStore_GetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	log := new(raft.Log)

	// Should return an error on non-existent log
	if err := store.GetLog(1, log); err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

func TestBadgerStore_SetLog(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

func TestBadgerStore_SetLogs(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
		testRaftLog(4, "log4"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(3, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[2], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

func TestBadgerStore_DeleteRange(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	log4 := testRaftLog(4, "log4")
	log5 := testRaftLog(5, "log5")
	logs := []*raft.Log{log1, log2, log3, log4, log5}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 3); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
	if err := store.GetLog(3, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
}

// TestCopyLogs tests that the copyLogs method works as expected
func TestCopyLogs(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
		testRaftLog(4, "log4"),
		testRaftLog(5, "log5"),
	}

	err := store.StoreLogs(logs)
	require.NoError(t, err)

	// Attempt to copy the logs
	buf := &bytes.Buffer{}
	err = store.CopyLogs(buf)
	require.NoError(t, err)

	assert.Equal(t, "log1\nlog2\nlog3\nlog4\nlog5\n", buf.String())
}

func TestBadgerStore_Set_Get(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

func TestBadgerStore_SetUint64_GetUint64(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

func TestBadgerStore_Acquire_Release(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != ErrNotAbleToAcquireLock {
		t.Fatalf("expected ErrNotAbleToAcquireLock error, got: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:2"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	if err := store.Release([]byte("my-lock:1")); err != nil {
		t.Fatalf("expected to release a lock, got: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}
}

func TestBadgerStore_Acquire_Release_WithTTL(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != ErrNotAbleToAcquireLock {
		t.Fatalf("expected ErrNotAbleToAcquireLock error, got: %q", err)
	}

	time.Sleep(1 * time.Second)

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}
}

// TestDBPath tests that the DBPath method returns the correct path
func TestDBPath(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	if store.DBPath() != store.path {
		t.Fatalf("bad path: %s", store.DBPath())
	}
}

func TestBadgerStore_Locks(t *testing.T) {
	store := testBadgerStore(t)
	defer store.Close()
	defer os.Remove(store.path)

	if err := store.Acquire([]byte("my-lock:1"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:2"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	if err := store.Acquire([]byte("my-lock:3"), time.Now().UTC().Add(time.Second*1)); err != nil {
		t.Fatalf("expected to acquire a lock, got error: %q", err)
	}

	locks := store.Locks()

	if len(locks) != 3 {
		t.Fatalf("expected to get 3 locks, got %d", len(locks))
	}
}
