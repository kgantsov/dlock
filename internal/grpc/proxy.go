package grpc

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/kgantsov/dlock/internal/domain"
	pb "github.com/kgantsov/dlock/internal/proto"
	"google.golang.org/grpc"
)

type Proxy struct {
	client pb.LockServiceClient
	leader string
}

func NewProxy() *Proxy {
	return &Proxy{}
}

func (p *Proxy) initClient(leader string) error {
	if leader == "" {
		return fmt.Errorf("leader address is empty")
	}

	p.leader = leader

	host := leader

	// if p.port != 0 {
	// 	host = fmt.Sprintf("%s:%d", host, p.port)
	// }

	conn, err := grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		log.Fatal().Msgf("Failed to connect: %v", err)
	}
	// defer conn.Close()

	p.client = pb.NewLockServiceClient(conn)

	return nil
}

func (p *Proxy) Acquire(host string, key, owner string, ttl int64) (*domain.LockEntry, error) {
	log.Info().Msgf("PROXY Acquire to the leader node: %s %s %s %d", host, key, owner, ttl)

	if p.leader != host || p.client == nil {
		if err := p.initClient(host); err != nil {
			return nil, err
		}
	}

	req := &pb.AcquireReq{
		Key:   key,
		Owner: owner,
		Ttl:   ttl,
	}

	resp, err := p.client.Acquire(context.Background(), req)
	if err != nil {
		log.Error().Msgf("Failed to acquire lock: %v", err)
		return nil, err
	}

	return &domain.LockEntry{
		Key:          resp.Key,
		Owner:        resp.Owner,
		ExpireAt:     resp.ExpireAt,
		FencingToken: resp.FencingToken,
	}, nil
}

func (p *Proxy) Release(host string, key, owner string, fencingToken uint64) error {
	log.Info().Msgf("PROXY Release to the leader node: %s %s %s %d", host, key, owner, fencingToken)

	if p.leader != host || p.client == nil {
		if err := p.initClient(host); err != nil {
			return err
		}
	}

	req := &pb.ReleaseReq{
		Key:          key,
		Owner:        owner,
		FencingToken: fencingToken,
	}

	_, err := p.client.Release(context.Background(), req)
	if err != nil {
		log.Error().Msgf("Failed to release lock: %v", err)
		return err
	}

	return nil
}
