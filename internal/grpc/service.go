package grpc

import (
	"context"

	"github.com/kgantsov/dlock/internal/domain"
	pb "github.com/kgantsov/dlock/internal/proto"
	"github.com/kgantsov/dlock/internal/server"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
)

type SyncroLockServer struct {
	pb.UnimplementedLockServiceServer
	node server.Node
	port int
}

func NewSyncroLockServer(node server.Node, port int) *SyncroLockServer {
	return &SyncroLockServer{
		node: node,
		port: port,
	}
}

func NewGRPCServer(node server.Node, port int) (*grpc.Server, error) {
	grpcServer := grpc.NewServer()
	pb.RegisterLockServiceServer(grpcServer, NewSyncroLockServer(node, port))
	return grpcServer, nil
}

func (s *SyncroLockServer) Acquire(ctx context.Context, req *pb.AcquireReq) (*pb.AcquireResp, error) {
	if req.Key == "" {
		return nil, domain.ErrInvalidKey
	}
	if req.Owner == "" {
		return nil, domain.ErrInvalidOwner
	}
	if req.Ttl <= 0 {
		return nil, domain.ErrInvalidTTL
	}

	lock, err := s.node.Acquire(req.Key, req.Owner, req.Ttl)
	if err != nil {
		return &pb.AcquireResp{
			Success: false,
		}, err
	}

	resp := &pb.AcquireResp{
		Success:      true,
		Key:          req.Key,
		Owner:        lock.Owner,
		FencingToken: lock.FencingToken,
		ExpireAt:     lock.ExpireAt,
		ExpiresIn:    req.Ttl,
	}

	log.Info().Msgf("Acquire: key=%s, owner=%s, ttl=%d, acquired=%v", req.Key, req.Owner, req.Ttl, lock)

	return resp, nil
}

func (s *SyncroLockServer) Release(ctx context.Context, req *pb.ReleaseReq) (*pb.ReleaseResp, error) {
	if req.Key == "" {
		return nil, domain.ErrInvalidKey
	}
	if req.Owner == "" {
		return nil, domain.ErrInvalidOwner
	}
	if req.FencingToken == 0 {
		return nil, domain.ErrInvalidFencingToken
	}

	err := s.node.Release(req.Key, req.Owner, req.FencingToken)
	if err != nil {
		return &pb.ReleaseResp{
			Success: false,
		}, err
	}
	resp := &pb.ReleaseResp{
		Success: true,
	}

	log.Info().Msgf("Release: key=%s, owner=%s", req.Key, req.Owner)
	return resp, nil
}
