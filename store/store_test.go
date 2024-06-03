package store

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(false, "node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}
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
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(true, "node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Acquire("foo", 60); err != nil {
		t.Fatalf("failed to acquire a clock for a key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err := s.Acquire("foo", 60)
	if err == nil {
		t.Fatal("Managed to acquire already acquired lock")
	}

	if err := s.Release("foo"); err != nil {
		t.Fatalf("failed to relsease a lock for a key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err = s.Acquire("foo", 60)
	if err != nil {
		t.Fatalf("failed to acquire a clock for a key: %s", err.Error())
	}
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
	if s == nil {
		t.Fatalf("failed to create store")
	}

	if err := s.Open(true, "node0"); err != nil {
		t.Fatalf("failed to open store: %s", err)
	}

	// Simple way to ensure there is a leader.
	time.Sleep(3 * time.Second)

	if err := s.Acquire("foo", 2); err != nil {
		t.Fatalf("failed to acquire a clock for a key: %s", err.Error())
	}

	// Wait for committed log entry to be applied.
	time.Sleep(500 * time.Millisecond)
	err := s.Acquire("foo", 2)
	if err == nil {
		t.Fatal("Managed to acquire already acquired lock")
	}
	time.Sleep(2 * time.Second)
	if err := s.Acquire("foo", 2); err != nil {
		t.Fatalf("failed to acquire a clock for a key: %s", err.Error())
	}
}
