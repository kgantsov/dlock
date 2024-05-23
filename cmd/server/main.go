package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	cluster "github.com/kgantsov/dlock/cluster"
	server "github.com/kgantsov/dlock/server"
	"github.com/kgantsov/dlock/store"
	"github.com/sirupsen/logrus"
)

// Command line defaults
const (
	DefaultHTTPAddr = "11000"
	DefaultRaftAddr = "localhost:12000"
)

// Command line parameters
var inmemory bool
var httpAddr string
var raftAddr string
var joinAddr string
var nodeID string
var ServiceName string

func init() {
	flag.BoolVar(&inmemory, "inmemory", false, "Use in-memory storage for Raft")
	flag.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	flag.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.StringVar(&nodeID, "id", "", "Node ID. If not set, same as Raft bind address")
	flag.StringVar(&ServiceName, "service-name", "", "Name of the service in Kubernetes")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] <raft-data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
	log.SetLevel(logrus.DebugLevel)

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "No Raft storage directory specified\n")
		os.Exit(1)
	}

	// Ensure Raft storage exists.
	raftDir := flag.Arg(0)
	if raftDir == "" {
		log.Info("No Raft storage directory specified")
	}
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		log.Fatalf("failed to create path for Raft storage: %s", err.Error())
	}

	s := store.New(log, inmemory)
	s.RaftDir = raftDir

	var j *cluster.Joiner

	hosts := []string{}

	if ServiceName != "" {
		cl := cluster.NewCluster(log, "default", ServiceName, httpAddr)

		if err := cl.Init(); err != nil {
			log.Warningln("Error initialising a cluster:", err)
			os.Exit(1)
		}

		nodeID = cl.NodeID()
		joinAddr = cl.JoinAddr()
		raftAddr = cl.RaftAddr()
		hosts = cl.Hosts()

		s.SetLeaderChangeFunc(cl.LeaderChanged)
	} else {
		if joinAddr != "" {
			hosts = append(hosts, joinAddr)
		}
	}

	s.RaftBind = raftAddr

	if err := s.Open(true, nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := server.New(log, httpAddr, s)
	go func() {
		if err := h.Start(); err != nil {
			log.Errorf("failed to start HTTP service: %s", err.Error())
		}
	}()

	// If join was specified, make the join request.
	j = cluster.NewJoiner(log, nodeID, raftAddr, httpAddr, hosts)

	if err := j.Join(); err != nil {
		log.Fatal(err.Error())
	}

	// We're up and running!
	log.Infof("hraftd started successfully, listening on http://%s", httpAddr)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("hraftd exiting")
}
