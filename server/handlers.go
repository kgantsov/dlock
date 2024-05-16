package server

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
	"github.com/sirupsen/logrus"
)

type (
	Handler struct {
		Logger *logrus.Logger
		store  Store
	}
)

func (h *Handler) Join(ctx context.Context, input *JoinInput) (*JoinOutput, error) {

	if err := h.store.Join(input.Body.ID, input.Body.Addr); err != nil {
		return &JoinOutput{}, err
	}

	res := &JoinOutput{}
	res.Body.ID = input.Body.ID
	res.Body.Addr = input.Body.Addr

	return res, nil
}

func (h *Handler) Acquire(ctx context.Context, input *AcquireInput) (*AcquireOutput, error) {
	key := input.Key

	if err := h.store.Acquire(key); err != nil {
		return nil, huma.Error409Conflict("Failed to acquire a lock", err)
	}

	res := &AcquireOutput{Status: http.StatusOK}
	res.Body.Status = "ACQUIRED"
	return res, nil
}

func (h *Handler) Release(ctx context.Context, input *ReleaseInput) (*ReleaseOutput, error) {
	key := input.Key

	if err := h.store.Release(key); err != nil {
		return nil, huma.Error400BadRequest("Failed to release a lock", err)
	}

	res := &ReleaseOutput{Status: http.StatusOK}
	res.Body.Status = "RELEASED"
	return res, nil
}
