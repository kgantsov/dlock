package raft

import (
	"fmt"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/dgraph-io/badger/v4"
	"github.com/kgantsov/dlock/internal/domain"
	"github.com/kgantsov/dlock/internal/grpc"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

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
	GrpcAddr string
	serverID raft.ServerID

	mu      sync.Mutex
	storage storage.Storage

	db *badger.DB

	leaderChangeFn func(bool)

	leaderConfig *LeaderConfig

	valueLogGCInterval time.Duration

	raft *raft.Raft // The consensus mechanism

	proxy *grpc.Proxy

	idGenerator *snowflake.Node
}

// NewNode returns a new Store.
func NewNode(db *badger.DB) *Node {
	return &Node{
		leaderChangeFn:     func(bool) {},
		valueLogGCInterval: 5 * time.Minute,
		mu:                 sync.Mutex{},
		db:                 db,
		proxy:              grpc.NewProxy(),
	}
}

func (n *Node) SetLeaderChangeFunc(leaderChangeFn func(bool)) {
	n.leaderChangeFn = leaderChangeFn
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (n *Node) Open(enableSingle bool, localID string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	n.serverID = config.LocalID

	n.leaderConfig = NewLeaderConfig(
		localID,
		n.RaftBind,
		n.GrpcAddr,
	)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", n.RaftBind)
	if err != nil {
		log.Error().Msgf("Error resolving TCP address: %s", err)
		return err
	}
	transport, err := raft.NewTCPTransport(n.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		log.Error().Msgf("Error creating TCP transport: %s", err)
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(n.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		log.Error().Msgf("Error creating file snapshot store: %s", err)
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	badgerDB, err := badgerdb.New(n.db, badgerdb.Options{})
	if err != nil {
		return fmt.Errorf("new store: %s", err)
	}
	logStore = badgerDB
	stableStore = badgerDB
	n.storage = storage.NewBadgerStorage(n.db)

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*FSM)(n), logStore, stableStore, snapshots, transport)
	if err != nil {
		log.Error().Msgf("Error creating new Raft: %s", err)
		return fmt.Errorf("new raft: %s", err)
	}
	n.raft = ra

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

	idGenerator, err := snowflake.NewNode(1)
	if err != nil {
		log.Warn().Err(err).Msg("failed to create snowflake node")
	}

	n.idGenerator = idGenerator

	go n.ListenToLeaderChanges()

	return nil
}

func (n *Node) ListenToLeaderChanges() {
	for isLeader := range n.raft.LeaderCh() {
		if isLeader {
			log.Info().Msgf("Node %s has become a leader", n.serverID)
			n.NotifyLeaderConfiguration()
		} else {
			log.Info().Msgf("Node %s lost leadership", n.serverID)
		}
		n.leaderChangeFn(isLeader)
	}
}
func (n *Node) InitIDGenerator() error {
	time.Sleep(2 * time.Second)
	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Info().Msgf("failed to get raft configuration: %v", err)
		return err
	}

	servers := configFuture.Configuration().Servers
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].ID < servers[j].ID
	})

	index := -1
	for i, srv := range servers {
		if srv.ID == n.serverID {
			index = i
			break
		}
	}

	log.Info().Msgf("Server configuration: %v Node index: %d", configFuture.Configuration().Servers, index)

	// Create a new snowflake Node with a Node number
	idGenerator, err := snowflake.NewNode(int64(index + 1))
	if err != nil {
		log.Warn().Err(err).Msg("failed to create snowflake node")
		return err
	}

	n.idGenerator = idGenerator

	return nil
}

func (n *Node) GenerateID() uint64 {
	return uint64(n.idGenerator.Generate().Int64())
}

// NotifyLeaderConfiguration notifies followers that a new leader has been elected and sends its address.
func (n *Node) NotifyLeaderConfiguration() error {
	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_LeaderChangeConf{
			LeaderChangeConf: &pb.LeaderChangeConfRaftCommand{
				NodeId:   string(n.serverID),
				RaftAddr: n.RaftBind,
				GrpcAddr: n.GrpcAddr,
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	f := n.raft.Apply(data, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	return nil
}

// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
func (n *Node) Acquire(key, owner string, ttl int64) (*domain.LockEntry, error) {

	if n.raft.State() != raft.Leader {
		leaderGrpcAddr := n.leaderConfig.GetLeaderGrpcAddress()

		return n.proxy.Acquire(leaderGrpcAddr, key, owner, ttl)
	}

	expireAt := time.Now().UTC().Add(time.Second * time.Duration(ttl))
	fencingToken := n.GenerateID()

	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_Acquire{
			Acquire: &pb.AcquireRaftCommand{
				Key:          key,
				Owner:        owner,
				FencingToken: fencingToken,
				ExpireAt:     expireAt.Unix(),
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := n.raft.Apply(data, raftTimeout)

	if f.Error() != nil {
		return nil, f.Error()
	}

	r := f.Response().(*pb.AcquireResp)
	if r.Error != "" {
		return nil, fmt.Errorf("%s", r.Error)
	}
	return &domain.LockEntry{
		Key:          r.Key,
		Owner:        r.Owner,
		FencingToken: r.FencingToken,
		ExpireAt:     r.ExpireAt,
	}, nil
}

// Release releases a lock for the given key.
func (n *Node) Release(key, owner string, fencingToken uint64) error {
	if n.raft.State() != raft.Leader {
		leaderGrpcAddr := n.leaderConfig.GetLeaderGrpcAddress()

		return n.proxy.Release(leaderGrpcAddr, key, owner, fencingToken)
	}

	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_Release{
			Release: &pb.ReleaseRaftCommand{
				Key:          key,
				Owner:        owner,
				FencingToken: fencingToken,
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return err
	}

	f := n.raft.Apply(data, raftTimeout)

	if f.Error() != nil {
		return f.Error()
	}

	r := f.Response().(*pb.ReleaseResp)
	if r.Error != "" {
		return fmt.Errorf("%s", r.Error)
	}
	return nil
}

func (n *Node) Renew(key, owner string, fencingToken uint64, ttl int64) (*domain.LockEntry, error) {
	if n.raft.State() != raft.Leader {
		leaderGrpcAddr := n.leaderConfig.GetLeaderGrpcAddress()

		return n.proxy.Renew(leaderGrpcAddr, key, owner, fencingToken, ttl)
	}

	expireAt := time.Now().UTC().Add(time.Second * time.Duration(ttl))

	cmd := &pb.RaftCommand{
		Cmd: &pb.RaftCommand_Renew{
			Renew: &pb.RenewRaftCommand{
				Key:          key,
				Owner:        owner,
				FencingToken: fencingToken,
				ExpireAt:     expireAt.Unix(),
			},
		},
	}

	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	f := n.raft.Apply(data, raftTimeout)

	if f.Error() != nil {
		return nil, f.Error()
	}

	r := f.Response().(*pb.RenewResp)
	if r.Error != "" {
		return nil, fmt.Errorf("%s", r.Error)
	}
	return &domain.LockEntry{
		Key:          r.Key,
		Owner:        r.Owner,
		FencingToken: r.FencingToken,
		ExpireAt:     r.ExpireAt,
	}, nil
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (n *Node) Join(nodeID, raftAddr string) error {
	log.Info().Msgf("received join request for remote node %s at %s", nodeID, raftAddr)

	configFuture := n.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		log.Error().Msgf("failed to get raft configuration: %v", err)
		return err
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(raftAddr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(raftAddr) && srv.ID == raft.ServerID(nodeID) {
				log.Info().Msgf("node %s at %s already member of cluster, ignoring join request", nodeID, raftAddr)
				return nil
			}

			future := n.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return fmt.Errorf("error removing existing node %s at %s: %s", nodeID, raftAddr, err)
			}
		}
	}

	f := n.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0)
	if f.Error() != nil {
		return f.Error()
	}

	log.Info().Msgf("node %s at %s joined successfully", nodeID, raftAddr)
	return nil
}

func (n *Node) NodeID() string {
	return string(n.serverID)
}

func (n *Node) RunValueLogGC() {
	ticker := time.NewTicker(n.valueLogGCInterval)
	defer ticker.Stop()

	log.Debug().Msgf("Started running value GC")

	for range ticker.C {
		log.Debug().Msg("Running value GC")
	again:
		err := n.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}
