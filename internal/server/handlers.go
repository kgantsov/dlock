package server

import (
	"context"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"
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

	lock, err := h.node.Acquire(key, owner, ttl)
	if err != nil {
		return nil, huma.Error409Conflict("Failed to acquire a lock", err)
	}

	res := &AcquireOutput{
		Status: http.StatusOK,
		Body: AcquireOutputBody{
			Status:       "ACQUIRED",
			Owner:        lock.Owner,
			FencingToken: lock.FencingToken,
			ExpireAt:     time.Unix(lock.ExpireAt, 0),
		},
	}
	return res, nil
}

func (h *Handler) Release(ctx context.Context, input *ReleaseInput) (*ReleaseOutput, error) {
	key := input.Key
	owner := input.Body.Owner
	fencingToken := input.Body.FencingToken

	if err := h.node.Release(key, owner, fencingToken); err != nil {
		return nil, huma.Error400BadRequest("Failed to release a lock", err)
	}

	res := &ReleaseOutput{
		Status: http.StatusOK,
		Body:   ReleaseOutputBody{Status: "RELEASED"},
	}
	return res, nil
}
