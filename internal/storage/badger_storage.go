package storage

import (
	"io"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/kgantsov/dlock/internal/domain"
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

func (s *BadgerStorage) Acquire(key, owner string, fencingToken uint64, expireAt time.Time) (*domain.LockEntry, error) {
	log.Debug().Msgf("Acquiring lock for key: %s", key)

	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get(addPrefix(dbLock, []byte(key)))
	if err != nil {
		if err != badger.ErrKeyNotFound {
			return nil, err
		}
	} else {
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		lock, err := domain.LockEntryFromBytes(val)

		if err == nil {
			if err == nil {
				if time.Now().Before(time.Unix(lock.ExpireAt, 0)) {
					return nil, domain.ErrLockAlreadyAcquired
				}
			}
		}
	}

	lock := &domain.LockEntry{
		Key:          string(key),
		Owner:        owner,
		FencingToken: fencingToken,
		ExpireAt:     expireAt.Unix(),
	}

	lockBytes, err := lock.ToBytes()
	if err != nil {
		return nil, err
	}

	e := badger.NewEntry(
		addPrefix(dbLock, []byte(key)),
		lockBytes,
	).WithTTL(expireAt.Sub(time.Now().UTC()))

	if err := txn.SetEntry(e); err != nil {
		return nil, err
	}

	err = txn.Commit()
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func (s *BadgerStorage) Release(key, owner string, fencingToken uint64) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	item, err := txn.Get(addPrefix(dbLock, []byte(key)))
	if err != nil {
		return domain.ErrLockNotFound
	}

	val, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}
	lock, err := domain.LockEntryFromBytes(val)

	if err != nil {
		return err
	}

	if lock.Owner != owner {
		log.Info().Msgf("Owner mismatch: %s != %s", lock.Owner, owner)
		return domain.ErrOwnerMismatch
	}

	if lock.FencingToken != fencingToken {
		log.Info().Msgf("Fencing token mismatch: %d != %d", lock.FencingToken, fencingToken)
		return domain.ErrFencingTokenMismatch
	}

	err = txn.Delete(addPrefix(dbLock, []byte(key)))
	if err != nil {
		return err
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

		lock, err := domain.LockEntryFromBytes(val)
		if err != nil {
			log.Debug().Msgf("Error unmarshaling key %s %v", key[:len(dbLock)], err)
			continue
		}

		var valueExpireAt time.Time
		err = valueExpireAt.UnmarshalBinary(val)

		if err == nil {
			if time.Now().After(time.Unix(lock.ExpireAt, 0)) {
				// Skip expired locks
				continue
			}
		}

		if _, err := w.Write(val); err != nil {
			log.Debug().Msgf("Error writing key %s %v %v", key[:len(dbLock)], lock, err)
			continue
		}
		if _, err := w.Write([]byte("\n")); err != nil {
			log.Debug().Msgf("Error writing key %s %v", key[:len(dbLock)], lock)
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
