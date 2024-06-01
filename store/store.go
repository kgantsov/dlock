// Package store provides a simple distributed key-value store. The keys and
// associated values are changed via distributed consensus, meaning that the
// values are changed only when a majority of nodes in the cluster agree on
// the new value.
//
// Distributed consensus is provided via the Raft algorithm, specifically the
// Hashicorp implementation.
package store

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	badgerdb "github.com/kgantsov/dlock/badger-store"
	"github.com/sirupsen/logrus"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op   string `json:"op,omitempty"`
	Key  string `json:"name,omitempty"`
	Time string `json:"time,omitempty"`
}

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string
	inmemory bool
	ServerID raft.ServerID

	mu    sync.Mutex
	store *badgerdb.BadgerStore

	leaderChangeFn func(bool)

	raft *raft.Raft // The consensus mechanism

	logger *logrus.Logger
}

// New returns a new Store.
func New(logger *logrus.Logger, inmemory bool) *Store {
	return &Store{
		inmemory:       inmemory,
		logger:         logger,
		leaderChangeFn: func(bool) {},
	}
}

func (s *Store) SetLeaderChangeFunc(leaderChangeFn func(bool)) {
	s.leaderChangeFn = leaderChangeFn
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	s.ServerID = config.LocalID

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	if s.inmemory {
		logStore = raft.NewInmemStore()
		stableStore = raft.NewInmemStore()
	} else {
		badgerDB, err := badgerdb.New(badgerdb.Options{
			Path: s.RaftDir,
		})
		if err != nil {
			return fmt.Errorf("new store: %s", err)
		}
		logStore = badgerDB
		stableStore = badgerDB
		s.store = badgerDB
	}

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*FSM)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	go s.ListenToLeaderChanges()

	return nil
}

func (s *Store) ListenToLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		if isLeader {
			s.logger.Infof("Node %s has become a leader", s.ServerID)
		} else {
			s.logger.Infof("Node %s lost leadership", s.ServerID)
		}
		s.leaderChangeFn(isLeader)
	}
}

// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
func (s *Store) Acquire(key string, ttl int) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:   "acquire",
		Key:  key,
		Time: time.Now().UTC().Add(time.Second * time.Duration(ttl)).Format(time.RFC3339),
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	r := f.Response().(*FSMResponse)
	if r.error != nil {
		return r.error
	}
	return nil
}

// Release releases a lock for the given key.
func (s *Store) Release(key string) error {
	if s.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}

	c := &command{
		Op:  "release",
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	r := f.Response().(*FSMResponse)
	if r.error != nil {
		return r.error
	}
	return nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Infof("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		s.logger.Errorf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				s.logger.Infof("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, addr, err)
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}
	s.logger.Infof("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Store) RunValueLogGC() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		s.logger.Debug("Started running value GC")

		for range ticker.C {
			locks := s.store.Locks()
			s.logger.Debugf("Running value GC. Locks found: %d", len(locks))
		again:
			err := s.store.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}()
}
