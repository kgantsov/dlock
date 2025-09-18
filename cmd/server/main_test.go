package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) // clean up

	// Setup command line arguments
	setUpArgs([]string{"cmd", "-id=node1", tempDir})

	// Run the main function in a goroutine to allow it to run asynchronously
	go main()

	// Allow some time for the server to start
	time.Sleep(3 * time.Second)

	// Perform HTTP request to verify the server is running
	jsonStr := []byte(`{"ttl": 60}`)
	resp, err := http.Post(
		"http://127.0.0.1:11000/API/v1/locks/:key/acquire", "application/json", bytes.NewBuffer(jsonStr),
	)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
