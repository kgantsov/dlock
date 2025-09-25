package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/kgantsov/dlock/internal/domain"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockNode struct {
	mock.Mock
}

func (m *MockNode) Acquire(key, owner string, ttl int64) (*domain.LockEntry, error) {
	args := m.Called(key, owner, ttl)
	if args.Get(0) != nil {
		return args.Get(0).(*domain.LockEntry), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockNode) Release(key, owner string, fencingToken uint64) error {
	args := m.Called(key, owner, fencingToken)
	return args.Error(0)
}

func TestSyncroLockServer_Acquire(t *testing.T) {
	node := new(MockNode)
	server := NewSyncroLockServer(node, 1234)
	ctx := context.Background()

	// Invalid key
	req := &pb.AcquireReq{Key: "", Owner: "owner", Ttl: 10}
	resp, err := server.Acquire(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidKey)

	// Invalid owner
	req = &pb.AcquireReq{Key: "key", Owner: "", Ttl: 10}
	resp, err = server.Acquire(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidOwner)

	// Invalid TTL
	req = &pb.AcquireReq{Key: "key", Owner: "owner", Ttl: 0}
	resp, err = server.Acquire(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidTTL)

	// Acquire error from node
	req = &pb.AcquireReq{Key: "key-1", Owner: "owner", Ttl: 10}
	node.On("Acquire", "key-1", "owner", int64(10)).Return(nil, errors.New("acquire failed"))
	resp, err = server.Acquire(ctx, req)
	assert.NotNil(t, resp)
	assert.False(t, resp.Success)
	assert.Error(t, err)
	node.AssertCalled(t, "Acquire", "key-1", "owner", int64(10))

	// Successful acquire
	req = &pb.AcquireReq{Key: "key-2", Owner: "owner", Ttl: 10}
	lock := &domain.LockEntry{Owner: "owner", FencingToken: 42, ExpireAt: 100}
	node.On("Acquire", "key-2", "owner", int64(10)).Return(lock, nil)
	resp, err = server.Acquire(ctx, req)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Equal(t, "key-2", resp.Key)
	assert.Equal(t, "owner", resp.Owner)
	assert.Equal(t, uint64(42), resp.FencingToken)
	assert.Equal(t, int64(100), resp.ExpireAt)
	assert.Equal(t, int64(10), resp.ExpiresIn)
}

func TestSyncroLockServer_Release(t *testing.T) {
	node := new(MockNode)
	server := NewSyncroLockServer(node, 1234)
	ctx := context.Background()

	// Invalid key
	req := &pb.ReleaseReq{Key: "", Owner: "owner", FencingToken: 1}
	resp, err := server.Release(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidKey)

	// Invalid owner
	req = &pb.ReleaseReq{Key: "key", Owner: "", FencingToken: 1}
	resp, err = server.Release(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidOwner)

	// Invalid fencing token
	req = &pb.ReleaseReq{Key: "key", Owner: "owner", FencingToken: 0}
	resp, err = server.Release(ctx, req)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, domain.ErrInvalidFencingToken)

	// Release error from node
	req = &pb.ReleaseReq{Key: "key-1", Owner: "owner", FencingToken: 123}
	node.On("Release", "key-1", "owner", uint64(123)).Return(domain.ErrFencingTokenMismatch)
	resp, err = server.Release(ctx, req)
	assert.NotNil(t, resp)
	assert.False(t, resp.Success)
	assert.Error(t, err)
	node.AssertCalled(t, "Release", "key-1", "owner", uint64(123))

	// Successful release
	req = &pb.ReleaseReq{Key: "key-2", Owner: "owner", FencingToken: 124}
	node.On("Release", "key-2", "owner", uint64(124)).Return(nil)
	resp, err = server.Release(ctx, req)
	assert.NoError(t, err)
	assert.True(t, resp.Success)
	node.AssertCalled(t, "Release", "key-2", "owner", uint64(124))
}
