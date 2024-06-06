package store

import (
	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/badger-store"
	"github.com/sirupsen/logrus"
)

type FSMSnapshot struct {
	path   string
	store  *badgerdb.BadgerStore
	logger *logrus.Logger
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := f.store.CopyLogs(sink); err != nil {
		f.logger.Debug("Error copying logs to sink")
		sink.Cancel()
		return err
	}
	return nil
}

func (f *FSMSnapshot) Release() {}
