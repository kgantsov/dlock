package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreOpen tests that the store can be opened.
func TestStoreOpen(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)
	s := New(log)
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	s.RaftBind = "127.0.0.1:0"
	s.RaftDir = tmpDir

	assert.NotNil(t, s)

	err := s.Open(false, "node0")
	require.NoError(t, err)
}

// TestStoreOpenSingleNode tests that a command can be applied to the log
func TestStoreOpenSingleNode(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	s := New(log)
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	s.RaftBind = "127.0.0.1:0"
	s.RaftDir = tmpDir

	assert.NotNil(t, s)

	err := s.Open(true, "node0")
	require.NoError(t, err)

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	err = s.Acquire("foo", 60)
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = s.Acquire("foo", 60)
	require.Error(t, err)

	err = s.Release("foo")
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = s.Acquire("foo", 60)
	require.NoError(t, err)
}

// TestStoreOpenSingleNodeWithTTL tests that a command can be applied to the log
func TestStoreOpenSingleNodeWithTTL(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.DebugLevel)

	s := New(log)
	tmpDir, _ := ioutil.TempDir("", "store_test")
	defer os.RemoveAll(tmpDir)

	s.RaftBind = "127.0.0.1:0"
	s.RaftDir = tmpDir

	assert.NotNil(t, s)

	err := s.Open(true, "node0")
	require.NoError(t, err)

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	err = s.Acquire("foo", 2)
	require.NoError(t, err)

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = s.Acquire("foo", 2)
	assert.Error(t, err)

	time.Sleep(2 * time.Second)
	err = s.Acquire("foo", 2)
	require.NoError(t, err)
}
