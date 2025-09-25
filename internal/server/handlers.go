package server

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/kgantsov/dlock/internal/domain"
)

type (
	Handler struct {
		node Node
	}
)

func (h *Handler) Join(ctx context.Context, input *JoinInput) (*JoinOutput, error) {
	if err := h.node.Join(input.Body.ID, input.Body.Addr); err != nil {
		return &JoinOutput{}, err
	}

	res := &JoinOutput{}
	res.Body.ID = input.Body.ID
	res.Body.Addr = input.Body.Addr

	return res, nil
}

func (h *Handler) Acquire(ctx context.Context, input *AcquireInput) (*AcquireOutput, error) {
	key := input.Key
	ttl := input.Body.TTL
	owner := input.Body.Owner

	if key == "" {
		return nil, huma.Error400BadRequest("Key is required", domain.ErrInvalidKey)
	}
	if owner == "" {
		return nil, huma.Error400BadRequest("Owner is required", domain.ErrInvalidOwner)
	}
	if ttl <= 0 {
		return nil, huma.Error400BadRequest("TTL must be greater than zero", domain.ErrInvalidTTL)
	}

	lock, err := h.node.Acquire(key, owner, ttl)
	if err != nil {
		return nil, huma.Error409Conflict("Failed to acquire a lock", err)
	}

	res := &AcquireOutput{
		Status: http.StatusOK,
		Body: AcquireOutputBody{
			Status:       "ACQUIRED",
			Owner:        lock.Owner,
			FencingToken: strconv.FormatUint(lock.FencingToken, 10),
			ExpireAt:     time.Unix(lock.ExpireAt, 0),
		},
	}
	return res, nil
}

func (h *Handler) Release(ctx context.Context, input *ReleaseInput) (*ReleaseOutput, error) {
	key := input.Key
	owner := input.Body.Owner

	if key == "" {
		return nil, huma.Error400BadRequest("Key is required", domain.ErrInvalidKey)
	}
	if owner == "" {
		return nil, huma.Error400BadRequest("Owner is required", domain.ErrInvalidOwner)
	}
	if input.Body.FencingToken == "" {
		return nil, huma.Error400BadRequest("Fencing token is required", domain.ErrInvalidFencingToken)
	}

	fencingToken, err := strconv.ParseUint(input.Body.FencingToken, 10, 64)
	if err != nil {
		return nil, huma.Error500InternalServerError(
			"Failed to parse fencing token", domain.ErrInvalidFencingToken,
		)
	}

	if err := h.node.Release(key, owner, fencingToken); err != nil {
		return nil, huma.Error400BadRequest("Failed to release a lock", err)
	}

	res := &ReleaseOutput{
		Status: http.StatusOK,
		Body:   ReleaseOutputBody{Status: "RELEASED"},
	}
	return res, nil
}
