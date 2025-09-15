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
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	addr := "8080"
	store := newTestStore()

	service := New(addr, store)

	assert.NotNil(t, service)
	assert.NotNil(t, service.router)
	assert.NotNil(t, service.api)
	assert.NotNil(t, service.h)
	assert.Equal(t, addr, service.addr)

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
	jsonBody := []byte(`{"ttl": 60}`)
	bodyReader := bytes.NewReader(jsonBody)
	req := httptest.NewRequest("POST", "/API/v1/locks/my-lock-1", bodyReader)
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
	assert.Equal(t, "dlock-node-0", successOutput.ID)
	assert.Equal(t, "localhost:12001", successOutput.Addr)
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
