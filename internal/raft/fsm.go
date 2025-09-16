package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/kgantsov/dlock/internal/storage"
	"github.com/rs/zerolog/log"
)

type FSM Node

type FSMResponse struct {
	key   string
	error error
}

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	log.Debug().Msgf("Apply log: %v", l)

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

	snapshot := &FSMSnapshot{storage: f.storage}
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
	log.Debug().Msgf("Restoring snapshot!@")
	for scanner.Scan() {
		line := scanner.Bytes()
		linesTotal++

		var lockEntry storage.LockEntry
		if err := json.Unmarshal(line, &lockEntry); err != nil {
			log.Warn().Msgf("Failed to unmarshal command: %v %v", err, string(line))
			continue
		}

		err := f.storage.Acquire([]byte(lockEntry.Key), lockEntry.ExpireAt)
		if err != nil {
			continue
		}
		linesRestored++
	}
	if err := scanner.Err(); err != nil {
		log.Info().Msgf(
			"Error while reading snapshot: %v. Restored %d out of %d lines",
			err,
			linesRestored,
			linesTotal,
		)
		return err
	}

	log.Info().Msgf("Restored %d out of %d lines", linesRestored, linesTotal)

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

	err = f.storage.Acquire([]byte(key), expire)

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

	err := f.storage.Release([]byte(key))

	if err != nil {
		return &FSMResponse{
			key:   key,
			error: fmt.Errorf("Failed to release a lock for a key: %s", key),
		}
	}
	return &FSMResponse{key: key, error: nil}
}
