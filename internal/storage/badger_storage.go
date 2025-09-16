package storage

import (
	"encoding/json"
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

type BadgerStorage struct {
	db *badger.DB
}

type LockEntry struct {
	Key      string
	ExpireAt time.Time
}

var dbLock = []byte("lock")

func NewBadgerStorage(db *badger.DB) *BadgerStorage {
	return &BadgerStorage{
		db: db,
	}
}

func (s *BadgerStorage) Acquire(key []byte, expireAt time.Time) error {
	log.Debug().Msgf("Acquiring lock for key: %s", key)

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get(addPrefix(dbLock, key))
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return err
		}
	} else {
		val, err := item.ValueCopy(nil)

		if err == nil {
			var valueExpireAt time.Time
			err = valueExpireAt.UnmarshalBinary(val)

			if err == nil {
				if time.Now().Before(valueExpireAt) {
					return ErrCouldNotAcquireLock
				}
			}
		}
	}

	expireInBytes, _ := expireAt.MarshalBinary()
	e := badger.NewEntry(addPrefix(dbLock, key), expireInBytes).WithTTL(expireAt.Sub(time.Now().UTC()))
	if err := txn.SetEntry(e); err != nil {
		return err
	}

	return txn.Commit()
}

func (s *BadgerStorage) Release(key []byte) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	_, err := txn.Get(addPrefix(dbLock, key))
	if err != nil {
		return ErrCouldNotReleaseLock
	}

	err = txn.Delete(addPrefix(dbLock, key))
	if err != nil {
		return ErrCouldNotReleaseLock
	}

	return txn.Commit()
}

func (s *BadgerStorage) Locks() []string {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	keys := []string{}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	opts.PrefetchValues = false

	it := txn.NewIterator(opts)
	defer it.Close()
	prefix := []byte(dbLock)

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		keys = append(keys, string(key))
	}

	return keys
}

func (s *BadgerStorage) PersistSnapshot(w io.Writer) error {
	txn := s.db.NewTransaction(false)
	defer txn.Discard()

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 100
	opts.PrefetchValues = true

	it := txn.NewIterator(opts)
	defer it.Close()
	prefix := []byte(dbLock)
	cnt := 0

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		log.Debug().Msgf("Copying key %s %d", key[:len(dbLock)], bytesToUint64(key[len(dbLock):]))

		val, err := item.ValueCopy(nil)

		if err != nil || val == nil {
			log.Debug().Msgf("Error reading key %s %d", key[:len(dbLock)], bytesToUint64(key[len(dbLock):]))
			continue
		}

		var valueExpireAt time.Time
		err = valueExpireAt.UnmarshalBinary(val)

		if err == nil {
			if time.Now().After(valueExpireAt) {
				// Skip expired locks
				continue
			}
		}

		lockEntry := LockEntry{
			Key:      string(key[len(dbLock):]),
			ExpireAt: valueExpireAt,
		}

		data, err := json.Marshal(lockEntry)
		if err != nil {
			log.Debug().Msgf("Error encoding key %s %v %v", key[:len(dbLock)], lockEntry, err)
			continue
		}

		if _, err := w.Write(data); err != nil {
			log.Debug().Msgf("Error writing key %s %v %v", key[:len(dbLock)], lockEntry, err)
			continue
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			log.Debug().Msgf("Error writing key %s %v", key[:len(dbLock)], lockEntry)
			continue
		}
		cnt += 1
	}

	log.Debug().Msgf("Total logs copied: %d", cnt)
	return nil
}

func (s *BadgerStorage) Close() error {
	return s.db.Close()
}
