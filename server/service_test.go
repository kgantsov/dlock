package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	_, api := humatest.New(t)

	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	store := newTestStore()

	h := &Handler{
		store:  store,
		Logger: log,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		Status string `json:"status"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Post("/API/v1/locks/migration_lock", map[string]any{
		"ttl": 5,
	})

	successOutput := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", successOutput.Status)

	resp = api.Post("/API/v1/locks/another_lock", map[string]any{
		"ttl": 5,
	})

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", successOutput.Status)

	resp = api.Post("/API/v1/locks/migration_lock", map[string]any{
		"ttl": 5,
	})

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusConflict, resp.Code)
	assert.Equal(t, "Conflict", errorOutput.Title)
	assert.Equal(t, 409, errorOutput.Status)
	assert.Equal(t, "Failed to acquire a lock", errorOutput.Detail)

	resp = api.Delete("/API/v1/locks/migration_lock")

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "RELEASED", successOutput.Status)

	resp = api.Post("/API/v1/locks/migration_lock", map[string]any{
		"ttl": 0,
	})

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", successOutput.Status)
}

// TestRelease tests the release endpoint with the lock that is not qcuired.
func TestRelease(t *testing.T) {
	_, api := humatest.New(t)

	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	store := newTestStore()

	h := &Handler{
		store:  store,
		Logger: log,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		Status string `json:"status"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Delete("/API/v1/locks/non_existing_lock")

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Equal(t, "Bad Request", errorOutput.Title)
	assert.Equal(t, 400, errorOutput.Status)
	assert.Equal(t, "Failed to release a lock", errorOutput.Detail)
}

// TestJoin tests the join endpoint.
func TestJoin(t *testing.T) {
	_, api := humatest.New(t)

	log := logrus.New()
	log.SetLevel(logrus.InfoLevel)

	store := newTestStore()

	h := &Handler{
		store:  store,
		Logger: log,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		ID   string `json:"id"`
		Addr string `json:"addr"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Post("/join", map[string]any{
		"id":   "dlock-node-0",
		"addr": "localhost:12001",
	})

	successOutput := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, successOutput.ID, "dlock-node-0")
	assert.Equal(t, successOutput.Addr, "localhost:12001")
}

type testStore struct {
	m map[string]time.Time
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string]time.Time),
	}
}

func (t *testStore) Acquire(key string, ttl int) error {
	_, ok := t.m[key]

	if ok {
		return fmt.Errorf("Failed to acquire a lock for a key: %s", key)
	}

	t.m[key] = time.Now().UTC().Add(time.Second * time.Duration(ttl))
	return nil
}

func (t *testStore) Release(key string) error {
	_, ok := t.m[key]

	if !ok {
		return fmt.Errorf("Failed to release a lock for a key: %s", key)
	}

	delete(t.m, key)
	return nil
}

func (t *testStore) Join(nodeID, addr string) error {
	return nil
}
