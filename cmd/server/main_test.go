package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Helper function to set up command line arguments
func setUpArgs(args []string) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	os.Args = args
	// init()
}

func TestMainFunction(t *testing.T) {
	// Setup temporary directory for Raft storage
	tempDir, err := ioutil.TempDir("", "raft")
	if err != nil {
		t.Fatalf("failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tempDir) // clean up

	// Setup command line arguments
	setUpArgs([]string{"cmd", "-id=node1", tempDir})

	// Mock logrus logger
	log := logrus.New()
	log.SetOutput(ioutil.Discard)
	log.SetLevel(logrus.DebugLevel)

	// Run the main function in a goroutine to allow it to run asynchronously
	go main()

	// Allow some time for the server to start
	time.Sleep(3 * time.Second)

	// Perform HTTP request to verify the server is running
	jsonStr := []byte(`{"ttl": 60}`)
	resp, err := http.Post(
		"http://127.0.0.1:11000/API/v1/locks/:key", "application/json", bytes.NewBuffer(jsonStr),
	)
	if err != nil {
		t.Fatalf("failed to perform HTTP request: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}
}
