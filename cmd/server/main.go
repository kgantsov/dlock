package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

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

	hosts := []string{}

	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			fmt.Println("Error getting hostname:", err)
			return
		}

		if ServiceName != "" {
			service := fmt.Sprintf("%s-internal.default.svc.cluster.local", ServiceName)
			_, addrs, err := net.LookupSRV("", "", service)
			if err != nil {
				log.Warningln("Error:", err)
			}

			for _, srv := range addrs {
				if strings.HasPrefix(srv.Target, hostname) {
					nodeID = srv.Target
				}
				hosts = append(hosts, srv.Target)
			}

			if !strings.HasPrefix(nodeID, fmt.Sprintf("%s-0", ServiceName)) {
				joinAddr = fmt.Sprintf(
					"%s-0.%s-internal.default.svc.cluster.local.:%s",
					ServiceName,
					ServiceName,
					httpAddr,
				)
			}
			raftAddr = fmt.Sprintf("%s:12000", nodeID)
		}
	}

	log.Debugf(
		"Current node is %s discovered hosts %+v joinAddr %s raftAddr %s",
		nodeID,
		hosts,
		joinAddr,
		raftAddr,
	)

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
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := server.New(log, httpAddr, s)
	go func() {
		if err := h.Start(); err != nil {
			log.Errorf("failed to start HTTP service: %s", err.Error())
		}
	}()

	// If join was specified, make the join request.
	if joinAddr != "" {
		if err := join(log, joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
	}

	// We're up and running!
	log.Infof("hraftd started successfully, listening on http://%s", httpAddr)

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("hraftd exiting")
}

func join(log *logrus.Logger, joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/join", joinAddr), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Infof("JOINED %+v %+v", resp.StatusCode, string(body))
	return nil
}
