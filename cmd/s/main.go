package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
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

		service := fmt.Sprintf("%s-internal.default.svc.cluster.local", ServiceName)
		_, addrs, err := net.LookupSRV("", "", service)
		if err != nil {
			fmt.Println("Error:", err)
		} else {

			for _, srv := range addrs {
				if strings.HasPrefix(srv.Target, hostname) {
					nodeID = srv.Target
				}
				hosts = append(hosts, srv.Target)
			}
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

	fmt.Printf(
		"Current node is %s discovered hosts %+v joinAddr %s raftAddr %s",
		nodeID,
		hosts,
		joinAddr,
		raftAddr,
	)
}
