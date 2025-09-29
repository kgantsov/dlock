package raft

import (
	"fmt"

	"github.com/hashicorp/raft"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/kgantsov/dlock/internal/storage"
	"github.com/rs/zerolog/log"
)

type FSMSnapshot struct {
	storage      storage.Storage
	leaderConfig *LeaderConfig
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	snapshotItem := &pb.SnapshotItem{
		Item: &pb.SnapshotItem_LeaderConf{
			LeaderConf: &pb.LeaderConfiguration{
				NodeId:   f.leaderConfig.Id,
				RaftAddr: f.leaderConfig.RaftAddr,
				GrpcAddr: f.leaderConfig.GrpcAddr,
			},
		},
	}
	log.Info().Msgf("Writing queue snapshot item for queue %#v", snapshotItem)

	if err := storage.WriteSnapshotItem(sink, snapshotItem); err != nil {
		return fmt.Errorf("failed to write message snapshot item: %v", err)
	}

	if err := f.storage.PersistSnapshot(sink); err != nil {
		log.Debug().Msg("Error copying logs to sink")
		sink.Cancel()
		return err
	}
	return nil
}

func (f *FSMSnapshot) Release() {}
