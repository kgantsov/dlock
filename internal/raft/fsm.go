package raft

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	"github.com/kgantsov/dlock/internal/domain"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

type FSM Node

type FSMResponse struct {
	key   string
	error error
}

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(raftLog *raft.Log) interface{} {
	log.Debug().Msgf("Apply log: %v", raftLog)

	var c pb.RaftCommand
	if err := proto.Unmarshal(raftLog.Data, &c); err != nil {
		return &FSMResponse{error: err}
	}

	switch command := c.Cmd.(type) {
	case *pb.RaftCommand_Acquire:
		return f.applyAcquire(command)
	case *pb.RaftCommand_Release:
		return f.applyRelease(command)
	default:
		return fmt.Errorf("unknown command: %s", c.Cmd)
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

		lock, err := domain.LockEntryFromBytes(line)
		if err != nil {
			log.Warn().Msgf("Failed to unmarshal command: %v %v", err, string(line))
			continue
		}

		_, err = f.storage.Acquire(lock.Key, lock.Owner, lock.FencingToken, time.Unix(lock.ExpireAt, 0))
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

func (f *FSM) applyAcquire(payload *pb.RaftCommand_Acquire) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	lock, err := f.storage.Acquire(
		payload.Acquire.Key,
		payload.Acquire.Owner,
		payload.Acquire.FencingToken,
		time.Unix(payload.Acquire.ExpireAt, 0),
	)

	if err != nil {
		return &pb.AcquireResp{
			Key:   payload.Acquire.Key,
			Error: fmt.Errorf("Failed to acquire a lock for a key: %s", payload.Acquire.Key).Error(),
		}
	}

	return &pb.AcquireResp{
		Key:          payload.Acquire.Key,
		Owner:        lock.Owner,
		FencingToken: lock.FencingToken,
		ExpireAt:     lock.ExpireAt,
	}
}

func (f *FSM) applyRelease(payload *pb.RaftCommand_Release) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.storage.Release(
		payload.Release.Key,
		payload.Release.Owner,
		payload.Release.FencingToken,
	)

	if err != nil {
		return &pb.ReleaseResp{
			Success: false,
			Error:   fmt.Errorf("Failed to release a lock for a key: %s", payload.Release.Key).Error(),
		}
	}
	return &pb.ReleaseResp{Success: true}
}
