package server

import (
	"fmt"
	"net/http"

	"github.com/ansrivas/fiberprometheus/v2"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humafiber"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/healthcheck"
	"github.com/gofiber/fiber/v2/middleware/helmet"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/monitor"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/kgantsov/dlock/internal/domain"
)

// Node is the interface Raft-backed key-value stores must implement.
type Node interface {
	// Acquire acquires a lock the given key if it wasn't acquired by somebody else.
	Acquire(key, owner string, ttl int64) (*domain.LockEntry, error)

	// Release releases a lock for the given key.
	Release(key, owner string, fencingToken uint64) error

	// Join joins the node, identitifed by nodeID and reachable at raftAddr, to the cluster.
	Join(nodeID string, raftAddr, grpcAddr string) error

	NodeID() string
	SetNodeAddr(nodeID, raftAddr, grpcAddr string)
}

// Service provides HTTP service.
type Service struct {
	api      huma.API
	router   *fiber.App
	h        *Handler
	httpAddr string
}

// New returns an uninitialized HTTP service.
func New(httpAddr, grpcAddr, raftAddr string, node Node) *Service {

	router := fiber.New()
	api := humafiber.New(
		router, huma.DefaultConfig("DLock a distributed lock servie", "1.0.0"),
	)

	h := &Handler{
		node:     node,
		grpcAddr: grpcAddr,
		raftAddr: raftAddr,
	}
	h.ConfigureMiddleware(router)
	h.RegisterRoutes(api)

	return &Service{
		api:      api,
		router:   router,
		h:        h,
		httpAddr: httpAddr,
	}
}

func (h *Handler) ConfigureMiddleware(router *fiber.App) {
	router.Use(logger.New(logger.Config{
		TimeFormat: "2006-01-02T15:04:05.999Z0700",
		TimeZone:   "Local",
		Format:     "${time} [INFO] ${locals:requestid} ${method} ${path} ${status} ${latency} ${error}​\n",
	}))

	router.Use(healthcheck.New())
	router.Use(helmet.New())

	router.Use(requestid.New())

	prometheus := fiberprometheus.New("dlock")
	prometheus.RegisterAt(router, "/metrics")
	router.Use(prometheus.Middleware)

	router.Get("/service/metrics", monitor.New())
	router.Use(recover.New())
}

func (h *Handler) RegisterRoutes(api huma.API) {
	huma.Register(
		api,
		huma.Operation{
			OperationID: "raft-join",
			Method:      http.MethodPost,
			Path:        "/join",
			Summary:     "Join cluster",
			Description: "An endpoint for joining cluster used that by raft consensus protocol",
			Tags:        []string{"raft"},
		},
		h.Join,
	)
	huma.Register(
		api,
		huma.Operation{
			OperationID: "acquire-lock",
			Method:      http.MethodPost,
			Path:        "/API/v1/locks/{key}/acquire",
			Summary:     "Acquire lock",
			Description: "An endpoint that is used for acquiring a lock",
			Tags:        []string{"Locks"},
		},
		h.Acquire,
	)
	huma.Register(
		api,
		huma.Operation{
			OperationID: "release-lock",
			Method:      http.MethodPost,
			Path:        "/API/v1/locks/{key}/release",
			Summary:     "Release lock",
			Description: "An endpoint that is used for releasing a lock",
			Tags:        []string{"Locks"},
		},
		h.Release,
	)
}

// Start starts the service.
func (s *Service) Start() error {
	return s.router.Listen(fmt.Sprintf(":%s", s.httpAddr))
}

// Close closes the service.
func (s *Service) Close() {
	// s.e.Shutdown()
}
