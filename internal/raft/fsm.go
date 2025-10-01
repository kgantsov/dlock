package raft

import (
	"fmt"
	"io"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/kgantsov/dlock/internal/storage"
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
	case *pb.RaftCommand_LeaderChangeConf:
		return f.applyLeaderChangeConf(command)
	case *pb.RaftCommand_Acquire:
		return f.applyAcquire(command)
	case *pb.RaftCommand_Release:
		return f.applyRelease(command)
	case *pb.RaftCommand_Renew:
		return f.applyRenew(command)
	default:
		return fmt.Errorf("unknown command: %s", c.Cmd)
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snapshot := &FSMSnapshot{storage: f.storage, leaderConfig: f.leaderConfig}
	return snapshot, nil
}

// Restore stores the key-value store to a previous state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	f.mu.Lock()
	defer f.mu.Unlock()

	linesTotal := 0
	linesRestored := 0

	log.Info().Msgf("Restoring snapshot...")

	for {
		item, err := storage.ReadSnapshotItem(rc)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		linesTotal++

		switch v := item.Item.(type) {
		case *pb.SnapshotItem_LeaderConf:
			log.Debug().
				Str("node_id", v.LeaderConf.NodeId).
				Str("raft_addr", v.LeaderConf.RaftAddr).
				Str("grpc_addr", v.LeaderConf.GrpcAddr).
				Msg("Restoring leader configuration")

			f.leaderConfig.Set(
				v.LeaderConf.NodeId,
				v.LeaderConf.RaftAddr,
				v.LeaderConf.GrpcAddr,
			)
		case *pb.SnapshotItem_Lock:
			log.Debug().
				Str("key", v.Lock.Key).
				Str("owner", v.Lock.Owner).
				Uint64("fencing_token", v.Lock.FencingToken).
				Str("expire_at", time.Unix(v.Lock.ExpireAt, 0).String()).
				Msg("Restoring lock")

			_, err = f.storage.Acquire(
				v.Lock.Key, v.Lock.Owner, v.Lock.FencingToken, time.Unix(v.Lock.ExpireAt, 0),
			)

			if err != nil {
				log.Error().Err(err).Msgf("failed to restore lock for key %s", v.Lock.Key)
				continue
			}

			linesRestored++
		default:
			return fmt.Errorf("unknown item in snapshot")
		}

		linesRestored++
	}

	log.Info().Msgf("Restored %d out of %d lines", linesRestored, linesTotal)

	return nil
}

func (f *FSM) applyLeaderChangeConf(payload *pb.RaftCommand_LeaderChangeConf) interface{} {
	log.Info().Msgf(
		"Leader changed: %s at %s (gRPC: %s)",
		payload.LeaderChangeConf.NodeId,
		payload.LeaderChangeConf.RaftAddr,
		payload.LeaderChangeConf.GrpcAddr,
	)

	f.leaderConfig.Set(
		payload.LeaderChangeConf.NodeId,
		payload.LeaderChangeConf.RaftAddr,
		payload.LeaderChangeConf.GrpcAddr,
	)
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
			Error: err.Error(),
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
			Error:   err.Error(),
		}
	}
	return &pb.ReleaseResp{Success: true}
}

func (f *FSM) applyRenew(payload *pb.RaftCommand_Renew) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	expireAt := time.Unix(payload.Renew.ExpireAt, 0)
	lock, err := f.storage.Renew(
		payload.Renew.Key,
		payload.Renew.Owner,
		payload.Renew.FencingToken,
		expireAt,
	)

	if err != nil {
		return &pb.RenewResp{
			Key:   payload.Renew.Key,
			Error: err.Error(),
		}
	}

	return &pb.RenewResp{
		Key:          payload.Renew.Key,
		Owner:        lock.Owner,
		FencingToken: lock.FencingToken,
		ExpireAt:     lock.ExpireAt,
	}
}
