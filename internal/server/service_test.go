package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2/humatest"
	"github.com/gofiber/fiber/v2"
	"github.com/kgantsov/dlock/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	httpAddr := "8080"
	store := newTestStore()

	service := New(httpAddr, store)

	assert.NotNil(t, service)
	assert.NotNil(t, service.router)
	assert.NotNil(t, service.api)
	assert.NotNil(t, service.h)
	assert.Equal(t, httpAddr, service.httpAddr)

	tests := []struct {
		description  string
		method       string
		url          string
		expectedCode int
	}{
		{"Healthcheck Middleware", "GET", "/readyz", fiber.StatusOK},
		{"Prometheus Middleware", "GET", "/metrics", fiber.StatusOK},
		{"Monitor Middleware", "GET", "/service/metrics", fiber.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.url, nil)
			resp, err := service.router.Test(req)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedCode, resp.StatusCode)
		})
	}

	// check that the correct headers are set by middlewares
	jsonBody := []byte(`{"owner": "owner-1", "ttl": 60}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest("POST", "/API/v1/locks/my-lock-1/acquire", bodyReader)
	resp, err := service.router.Test(req)

	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	require.Equal(t, "0", resp.Header.Get(fiber.HeaderXXSSProtection))
	require.Equal(t, "nosniff", resp.Header.Get(fiber.HeaderXContentTypeOptions))
	require.Equal(t, "SAMEORIGIN", resp.Header.Get(fiber.HeaderXFrameOptions))
	require.Equal(t, "", resp.Header.Get(fiber.HeaderContentSecurityPolicy))
	require.Equal(t, "no-referrer", resp.Header.Get(fiber.HeaderReferrerPolicy))
	require.Equal(t, "", resp.Header.Get(fiber.HeaderPermissionsPolicy))
	require.Equal(t, "require-corp", resp.Header.Get("Cross-Origin-Embedder-Policy"))
	require.Equal(t, "same-origin", resp.Header.Get("Cross-Origin-Opener-Policy"))
	require.Equal(t, "same-origin", resp.Header.Get("Cross-Origin-Resource-Policy"))
	require.Equal(t, "?1", resp.Header.Get("Origin-Agent-Cluster"))
	require.Equal(t, "off", resp.Header.Get("X-DNS-Prefetch-Control"))
	require.Equal(t, "noopen", resp.Header.Get("X-Download-Options"))
	require.Equal(t, "none", resp.Header.Get("X-Permitted-Cross-Domain-Policies"))
	require.NotEqual(t, "", resp.Header.Get("X-Request-Id"))
}

func TestLock(t *testing.T) {
	_, api := humatest.New(t)

	store := newTestStore()

	h := &Handler{
		node: store,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		Status       string `json:"status"`
		FencingToken string `json:"fencing_token,omitempty"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Post("/API/v1/locks/migration_lock/acquire", map[string]any{
		"owner": "owner-1",
		"ttl":   5,
	})

	lock1 := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), lock1)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", lock1.Status)

	resp = api.Post("/API/v1/locks/another_lock/acquire", map[string]any{
		"owner": "owner-2",
		"ttl":   5,
	})

	lock2 := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), lock2)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", lock2.Status)

	resp = api.Post("/API/v1/locks/migration_lock/acquire", map[string]any{
		"owner": "owner-1",
		"ttl":   5,
	})

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusConflict, resp.Code)
	assert.Equal(t, "Conflict", errorOutput.Title)
	assert.Equal(t, 409, errorOutput.Status)
	assert.Equal(t, "Failed to acquire a lock", errorOutput.Detail)

	resp = api.Post("/API/v1/locks/migration_lock/release", map[string]any{
		"owner":         "owner-1",
		"fencing_token": lock1.FencingToken,
	})

	lock1 = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), lock1)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "RELEASED", lock1.Status)

	resp = api.Post("/API/v1/locks/migration_lock/acquire", map[string]any{
		"owner": "owner-1",
		"ttl":   1,
	})

	lock1 = &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), lock1)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "ACQUIRED", lock1.Status)
}

// TestRelease tests the release endpoint with the lock that is not acquired.
func TestRelease(t *testing.T) {
	_, api := humatest.New(t)

	store := newTestStore()

	h := &Handler{
		node: store,
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

	resp := api.Post("/API/v1/locks/non_existing_lock/release", map[string]any{
		"owner":         "owner-1",
		"fencing_token": "123",
	})

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Equal(t, "Bad Request", errorOutput.Title)
	assert.Equal(t, 400, errorOutput.Status)
	assert.Equal(t, "Failed to release a lock", errorOutput.Detail)
}

func TestRenew(t *testing.T) {
	_, api := humatest.New(t)

	store := newTestStore()

	h := &Handler{
		node: store,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		Status       string `json:"status"`
		FencingToken string `json:"fencing_token,omitempty"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Post("/API/v1/locks/my_lock/renew", map[string]any{
		"owner":         "owner-1",
		"fencing_token": "123",
		"ttl":           5,
	})

	errorOutput := &ErrorOutput{}

	json.Unmarshal(resp.Body.Bytes(), errorOutput)

	assert.Equal(t, http.StatusBadRequest, resp.Code)
	assert.Equal(t, "Bad Request", errorOutput.Title)
	assert.Equal(t, 400, errorOutput.Status)
	assert.Equal(t, "Failed to renew a lock", errorOutput.Detail)
}

// TestJoin tests the join endpoint.
func TestJoin(t *testing.T) {
	_, api := humatest.New(t)

	store := newTestStore()

	h := &Handler{
		node: store,
	}
	h.RegisterRoutes(api)

	type SuccessOutput struct {
		ID       string `json:"id"`
		RaftAddr string `json:"raft_addr"`
	}
	type ErrorOutput struct {
		Title  string `json:"title"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
	}

	resp := api.Post("/join", map[string]any{
		"id":        "dlock-node-1",
		"raft_addr": "localhost:10001",
	})

	successOutput := &SuccessOutput{}

	json.Unmarshal(resp.Body.Bytes(), successOutput)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "dlock-node-1", successOutput.ID)
	assert.Equal(t, "localhost:10001", successOutput.RaftAddr)
}

type testStore struct {
	m map[string]*domain.LockEntry
}

func newTestStore() *testStore {
	return &testStore{
		m: make(map[string]*domain.LockEntry),
	}
}

func (t *testStore) Acquire(key, owner string, ttl int64) (*domain.LockEntry, error) {
	_, ok := t.m[key]

	if ok {
		return nil, fmt.Errorf("Failed to acquire a lock for a key: %s", key)
	}

	lock := &domain.LockEntry{
		Key:          key,
		Owner:        owner,
		FencingToken: uint64(time.Now().UnixNano()),
		ExpireAt:     time.Now().UTC().Add(time.Second * time.Duration(ttl)).Unix(),
	}

	t.m[key] = lock
	return lock, nil
}

func (t *testStore) Release(key, owner string, fencingToken uint64) error {
	lock, ok := t.m[key]
	if !ok {
		return fmt.Errorf("Failed to release a lock for a key: %s", key)
	}

	if lock.Owner != owner {
		return domain.ErrOwnerMismatch
	}

	if lock.FencingToken != fencingToken {
		return domain.ErrFencingTokenMismatch
	}

	delete(t.m, key)
	return nil
}

func (t *testStore) Renew(key, owner string, fencingToken uint64, ttl int64) (*domain.LockEntry, error) {
	lock, ok := t.m[key]
	if !ok {
		return nil, fmt.Errorf("Failed to renew a lock for a key: %s", key)
	}

	if lock.Owner != owner {
		return nil, domain.ErrOwnerMismatch
	}

	if lock.FencingToken != fencingToken {
		return nil, domain.ErrFencingTokenMismatch
	}

	lock.ExpireAt = time.Now().UTC().Add(time.Second * time.Duration(ttl)).Unix()
	t.m[key] = lock
	return lock, nil
}

func (t *testStore) Join(nodeID, addr string) error {
	return nil
}

func (t *testStore) NodeID() string {
	return "dlock-node-0"
}
