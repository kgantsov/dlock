package server

import "time"

type JoinInput struct {
	Body struct {
		ID       string `json:"id" example:"dlock-node-0" doc:"ID of a node"`
		RaftAddr string `json:"raft_addr" example:"localhost:12001" doc:"IP address and a port of a service"`
		GrpcAddr string `json:"grpc_addr" example:"localhost:12002" doc:"gRPC address and a port of a service"`
	}
}

type JoinOutput struct {
	Body struct {
		ID       string `json:"id" example:"dlock-node-0" doc:"ID of a node"`
		RaftAddr string `json:"raft_addr" example:"localhost:12001" doc:"IP address and a port of a service"`
		GrpcAddr string `json:"grpc_addr" example:"localhost:12002" doc:"gRPC address and a port of a service"`
	}
}

type AcquireInput struct {
	Key  string `path:"key" maxLength:"1024" example:"migration_lock:1" doc:"Key for the lock"`
	Body struct {
		Owner string `json:"owner,omitempty" example:"owner-1" doc:"Owner of the lock"`
		TTL   int64  `json:"ttl" default:"60" minimum:"1" example:"60" doc:"Time to live for the lock in seconds"`
	}
}

type AcquireOutputBody struct {
	Status       string    `json:"status" example:"ACQUIRED" doc:"Status of the acquire operation"`
	Owner        string    `json:"owner,omitempty" example:"owner-1" doc:"Owner of the lock"`
	FencingToken string    `json:"fencing_token,omitempty" example:"1968688872162332700" doc:"Fencing token of the lock"`
	ExpireAt     time.Time `json:"expire_at,omitempty" example:"2025-09-18T16:50:38+02:00" doc:"Expiration time of the lock"`
}

type AcquireOutput struct {
	Status int
	Body   AcquireOutputBody
}
type ReleaseInputBody struct {
	Owner        string `json:"owner" example:"owner-1" doc:"Owner of the lock"`
	FencingToken string `json:"fencing_token" example:"1968688872162332700" doc:"Fencing token of the lock"`
}
type ReleaseInput struct {
	Key  string `path:"key" maxLength:"1024" example:"migration_lock:1" doc:"Key for the lock"`
	Body ReleaseInputBody
}

type ReleaseOutputBody struct {
	Status string `json:"status" example:"RELEASED" doc:"Status of the release operation"`
}
type ReleaseOutput struct {
	Status int
	Body   ReleaseOutputBody
}
