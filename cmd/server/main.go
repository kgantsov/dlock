package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	cluster "github.com/kgantsov/dlock/internal/cluster"
	"github.com/kgantsov/dlock/internal/raft"
	server "github.com/kgantsov/dlock/internal/server"
)

// Command line defaults
const (
	DefaultHTTPAddr = "11000"
	DefaultRaftAddr = "localhost:12000"
)

// Command line parameters
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string
var ServiceName string

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.WarnLevel)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&nodeID, "id", "", "Node ID. If not set, same as Raft bind address")
	flag.StringVar(&ServiceName, "service-name", "", "Name of the service in Kubernetes")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	dataDir := flag.Arg(0)
	if dataDir == "" {
		log.Info().Msg("No Raft storage directory specified")
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		log.Fatal().Msgf("failed to create path for Raft storage: %s", err.Error())
	}

	opts := badger.DefaultOptions(dataDir)
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal().Msg(err.Error())
	}
	defer db.Close()

	node := raft.NewNode(db)
	node.RaftDir = dataDir
	go node.RunValueLogGC()

	var j *cluster.Joiner

	hosts := []string{}

	if ServiceName != "" {
		namespace := "default"
		serviceDiscovery := cluster.NewServiceDiscoverySRV(namespace, ServiceName)
		cl := cluster.NewCluster(serviceDiscovery, namespace, ServiceName, httpAddr)

		if err := cl.Init(); err != nil {
			log.Warn().Msgf("Error initialising a cluster: %s", err)
			os.Exit(1)
		}

		nodeID = cl.NodeID()
		raftAddr = cl.RaftAddr()
		hosts = cl.Hosts()

		node.SetLeaderChangeFunc(cl.LeaderChanged)
	} else {
		if joinAddr != "" {
			hosts = append(hosts, joinAddr)
		}
	}

	node.RaftBind = raftAddr

	if err := node.Open(true, nodeID); err != nil {
		log.Fatal().Msgf("failed to open store: %s", err.Error())
	}

	h := server.New(httpAddr, node)
	go func() {
		if err := h.Start(); err != nil {
			log.Error().Msgf("failed to start HTTP service: %s", err.Error())
		}
	}()

	// If join was specified, make the join request.
	j = cluster.NewJoiner(nodeID, raftAddr, hosts)

	if err := j.Join(); err != nil {
		log.Fatal().Msg(err.Error())
	}

	log.Info().Msgf("dlock started successfully, listening on http://%s", httpAddr)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info().Msg("dlock exiting")
}
