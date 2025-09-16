package raft

import (
	"github.com/hashicorp/raft"
	"github.com/kgantsov/dlock/internal/storage"
	"github.com/rs/zerolog/log"
)

type FSMSnapshot struct {
	storage storage.Storage
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := f.storage.PersistSnapshot(sink); err != nil {
		log.Debug().Msg("Error copying logs to sink")
		sink.Cancel()
		return err
	}
	return nil
}

func (f *FSMSnapshot) Release() {}
