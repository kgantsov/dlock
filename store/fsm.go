package store

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
)

type FSM Store

type FSMResponse struct {
	key   string
	error error
}

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	f.logger.Debugf("Apply log: %v", l)

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

	snapshot := &FSMSnapshot{path: f.store.DBPath(), store: f.store, logger: f.logger}
	return snapshot, nil
}

// Restore stores the key-value store to a previous state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	scanner := bufio.NewScanner(rc)
	linesTotal := 0
	linesRestored := 0
	f.logger.Debugf("Restoring snapshot")
	for scanner.Scan() {
		line := scanner.Bytes()
		linesTotal++

		var c command
		if err := json.Unmarshal(line, &c); err != nil {
			f.logger.Warnf("Failed to unmarshal command: %v %v", err, line)
			continue
		}

		expire, err := time.Parse(time.RFC3339, c.Time)
		if err != nil {
			continue
		}

		if time.Now().UTC().Before(expire) {
			switch c.Op {
			case "acquire":
				f.store.Acquire([]byte(c.Key), expire)
				linesRestored++
			case "release":
				f.store.Release([]byte(c.Key))
				linesRestored++
			default:
				f.logger.Warnf("Unrecognized command op: %s", c.Op)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		f.logger.Infof("Error while reading snapshot: %v. Restored %d out of %d lines", err, linesRestored, linesTotal)
		return err
	}

	f.logger.Infof("Restored %d out of %d lines", linesRestored, linesTotal)

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
