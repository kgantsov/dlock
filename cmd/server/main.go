package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	cluster "github.com/kgantsov/dlock/internal/cluster"
	"github.com/kgantsov/dlock/internal/grpc"
	"github.com/kgantsov/dlock/internal/raft"
	server "github.com/kgantsov/dlock/internal/server"
)

// Command line defaults
const (
	DefaultHTTPAddr = "8000"
	DefaultGRPCAddr = "localhost:9000"
	DefaultRaftAddr = "localhost:10000"
)

// Command line parameters
var httpAddr string
var grpcAddr string
var raftAddr string
var joinAddr string
var nodeID string
var ServiceName string

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	// Default level for this example is info, unless debug flag is present
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&grpcAddr, "gaddr", DefaultGRPCAddr, "Set the gRPC bind address")
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

	h := server.New(httpAddr, grpcAddr, raftAddr, node)
	go func() {
		if err := h.Start(); err != nil {
			log.Error().Msgf("failed to start HTTP service: %s", err.Error())
		}
	}()

	// If join was specified, make the join request.
	j = cluster.NewJoiner(node, nodeID, raftAddr, grpcAddr, hosts)

	if err := j.Join(); err != nil {
		log.Fatal().Msg(err.Error())
	}

	node.InitIDGenerator()

	log.Info().Msgf("dlock started successfully, listening on http://%s", httpAddr)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	grpcPort := lis.Addr().(*net.TCPAddr).Port
	grpcServer, err := grpc.NewGRPCServer(node, grpcPort)
	if err != nil {
		log.Fatal().Msgf("failed to create GRPC server: %v", err)
	}

	go grpcServer.Serve(lis)
	log.Info().Msgf("Started gRPC server on %s", grpcAddr)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info().Msg("dlock exiting")
}
