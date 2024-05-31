package store

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/badger-store"
)

type FSM Store

type FSMResponse struct {
	key   string
	error error
}

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "acquire":
		return f.applyAcquire(c.Key, c.Time)
	case "release":
		return f.applyRelease(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapshot := &FSMSnapshot{path: f.store.DBPath(), store: f.store}
	return snapshot, nil
}

// Restore stores the key-value store to a previous state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the current database
	if err := f.store.Close(); err != nil {
		return err
	}

	newDB, err := badgerdb.New(badgerdb.Options{
		Path: f.store.DBPath(),
	})
	if err != nil {
		return err
	}

	f.store = newDB

	return nil
}

func (f *FSM) applyAcquire(key, expireAt string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	expire, err := time.Parse(time.RFC3339, expireAt)
	if err != nil {
		return &FSMResponse{
			key: key,
			error: fmt.Errorf(
				"Failed to parse expirition time for a lock for a key: %s %s", key, expireAt,
			),
		}
	}

	err = f.store.Acquire([]byte(key), expire)

	if err != nil {
		return &FSMResponse{
			key:   key,
			error: fmt.Errorf("Failed to acquire a lock for a key: %s", key),
		}
	}

	return &FSMResponse{key: key, error: nil}
}

func (f *FSM) applyRelease(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.store.Release([]byte(key))

	if err != nil {
		return &FSMResponse{
			key:   key,
			error: fmt.Errorf("Failed to release a lock for a key: %s", key),
		}
	}
	return &FSMResponse{key: key, error: nil}
}
