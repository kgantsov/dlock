package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

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

	resp := api.Post("/API/v1/locks/migration_lock")

	successOutput := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, successOutput.Status, "ACQUIRED")

	resp = api.Post("/API/v1/locks/another_lock")

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, successOutput.Status, "ACQUIRED")

	resp = api.Post("/API/v1/locks/migration_lock")

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusConflict, resp.Code)
	assert.Equal(t, errorOutput.Title, "Conflict")
	assert.Equal(t, errorOutput.Status, 409)
	assert.Equal(t, errorOutput.Detail, "Failed to acquire a lock")

	resp = api.Delete("/API/v1/locks/migration_lock")

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, successOutput.Status, "RELEASED")

	resp = api.Post("/API/v1/locks/migration_lock")

	successOutput = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, successOutput.Status, "ACQUIRED")
}

type testStore struct {
	m map[string]bool
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string]bool),
	}
}

func (t *testStore) Acquire(key string) error {
	val := t.m[key]

	if val {
		return fmt.Errorf("Failed to acquire a lock for a key: %s", key)
	}

	t.m[key] = true
	return nil
}

func (t *testStore) Release(key string) error {
	delete(t.m, key)
	return nil
}

func (t *testStore) Join(nodeID, addr string) error {
	return nil
}
