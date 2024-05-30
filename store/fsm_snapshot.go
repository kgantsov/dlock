package store

import (
	"io"
	"os"

	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/badger-store"
)

type FSMSnapshot struct {
	path  string
	store *badgerdb.BadgerStore
}

func (f *FSMSnapshot) Persist(sink raft.SnapshotSink) error {
	// Create a temporary directory for the backup
	tmpDir, err := os.MkdirTemp("", "badger-snapshot-")
	if err != nil {
		sink.Cancel()
		return err
	}
	defer os.RemoveAll(tmpDir)

	backupPath := tmpDir + "/backup"
	if err := f.backup(backupPath); err != nil {
		sink.Cancel()
		return err
	}

	// Write the backup file to the snapshot sink
	file, err := os.Open(backupPath)
	if err != nil {
		sink.Cancel()
		return err
	}
	defer file.Close()

	if _, err := io.Copy(sink, file); err != nil {
		sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (f *FSMSnapshot) backup(backupPath string) error {

	file, err := os.Create(backupPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = f.store.Backup(file, 0)
	if err != nil {
		return err
	}

	return nil
}

func (f *FSMSnapshot) Release() {}
