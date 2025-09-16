package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"

	"github.com/hashicorp/raft"
	"github.com/kgantsov/dlock/internal/storage"
	badgerdb "github.com/kgantsov/raft-badgerstore"
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

// Node is a simple key-value store, where all changes are made via Raft consensus.
type Node struct {
	RaftDir  string
	RaftBind string
	ServerID raft.ServerID

	mu      sync.Mutex
	storage storage.Storage

	db *badger.DB

	leaderChangeFn func(bool)

	valueLogGCInterval time.Duration

	raft *raft.Raft // The consensus mechanism
}

// NewNode returns a new Store.
func NewNode(db *badger.DB) *Node {
	return &Node{
		leaderChangeFn:     func(bool) {},
		valueLogGCInterval: 5 * time.Minute,
		db:                 db,
	}
}

func (s *Node) SetLeaderChangeFunc(leaderChangeFn func(bool)) {
	s.leaderChangeFn = leaderChangeFn
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Node) Open(enableSingle bool, localID string) error {
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
	badgerDB, err := badgerdb.New(s.db, badgerdb.Options{})
	if err != nil {
		return fmt.Errorf("new store: %s", err)
	}
	logStore = badgerDB
	stableStore = badgerDB
	s.storage = storage.NewBadgerStorage(s.db)

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

func (s *Node) ListenToLeaderChanges() {
	for isLeader := range s.raft.LeaderCh() {
		if isLeader {
			log.Info().Msgf("Node %s has become a leader", s.ServerID)
		} else {
			log.Info().Msgf("Node %s lost leadership", s.ServerID)
		}
		s.leaderChangeFn(isLeader)
	}
}

// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
func (s *Node) Acquire(key string, ttl int) error {
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
func (s *Node) Release(key string) error {
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
func (s *Node) Join(nodeID, addr string) error {
	log.Info().Msgf("received join request for remote node %s at %s", nodeID, addr)

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Error().Msgf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				log.Info().Msgf("node %s at %s already member of cluster, ignoring join request", nodeID, addr)
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
	log.Info().Msgf("node %s at %s joined successfully", nodeID, addr)
	return nil
}

func (s *Node) RunValueLogGC() {
	ticker := time.NewTicker(s.valueLogGCInterval)
	defer ticker.Stop()

	log.Debug().Msgf("Started running value GC")

	for range ticker.C {
		log.Debug().Msg("Running value GC")
	again:
		err := s.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}
