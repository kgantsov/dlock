package cluster

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/sirupsen/logrus"
)

// TestJoiner tests the Joiner.
func TestJoiner(t *testing.T) {
	// Start a local HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Test request parameters
		assert.Equal(t, "POST", req.Method)
		assert.Equal(t, "/join", req.URL.String())
		// Send response to be tested
		rw.Write([]byte(`OK`))
	}))
	// Close the server when test finishes
	defer server.Close()

	// get host name and port from server.URL
	host := server.URL[len("http://"):]

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	hosts := []string{host}
	j := NewJoiner(log, "node0", "raftAddr", hosts)

	if j == nil {
		t.Fatalf("failed to create Joiner")
	}

	if err := j.Join(); err != nil {
		t.Fatalf("failed to join hosts: %s", err)
	}
}

func TestJoinerRetry(t *testing.T) {
	attemptHost1 := 0
	attemptHost2 := 0

	// Start a local HTTP server
	server1 := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "POST", req.Method)
		assert.Equal(t, "/join", req.URL.String())

		if attemptHost1 < 2 {
			attemptHost1++
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		assert.Equal(t, 2, attemptHost1)
		rw.Write([]byte(`OK`))
	}))
	// Close the server when test finishes
	defer server1.Close()

	// Start a local HTTP server
	server2 := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		assert.Equal(t, "POST", req.Method)
		assert.Equal(t, "/join", req.URL.String())

		if attemptHost2 < 2 {
			attemptHost2++
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		assert.Equal(t, 2, attemptHost2)
		rw.Write([]byte(`OK`))
	}))
	// Close the server when test finishes
	defer server2.Close()

	// get host name and port from server.URL
	host1 := server1.URL[len("http://"):]
	host2 := server2.URL[len("http://"):]

	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	hosts := []string{host1, host2}
	j := NewJoiner(log, "node0", "raftAddr", hosts)

	if j == nil {
		t.Fatalf("failed to create Joiner")
	}

	if err := j.Join(); err != nil {
		t.Fatalf("failed to join hosts: %s", err)
	}
}

func TestJoinerNoHosts(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	hosts := []string{}
	j := NewJoiner(log, "node0", "raftAddr", hosts)

	if j == nil {
		t.Fatalf("failed to create Joiner")
	}

	err := j.Join()

	assert.Equal(t, nil, err)
}

func TestJoinerHostsUnavailable(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	hosts := []string{"host1", "host2"}
	j := NewJoiner(log, "node0", "raftAddr", hosts)

	assert.NotEqual(t, nil, j)

	err := j.Join()

	assert.Contains(t, err.Error(), "failed to join node at")
}
