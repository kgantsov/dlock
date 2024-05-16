package store

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
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
		return f.applyAcquire(c.Key)
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

	// Clone the map.
	o := make(map[string]bool)
	for k, v := range f.m {
		o[k] = v
	}
	return &FSMSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	o := make(map[string]bool)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}

func (f *FSM) applyAcquire(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	val := f.m[key]

	if val {
		return &FSMResponse{
			key:   key,
			error: fmt.Errorf("Failed to acquire a lock for a key: %s", key),
		}
	}

	f.m[key] = true
	return &FSMResponse{key: key, error: nil}
}

func (f *FSM) applyRelease(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return &FSMResponse{key: key, error: nil}
}
