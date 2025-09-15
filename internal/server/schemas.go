package server

type JoinInput struct {
	Body struct {
		ID   string `json:"id" example:"dlock-node-0" doc:"ID of a node"`
		Addr string `json:"addr" example:"localhost:12001" doc:"IP address and a port of a service"`
	}
}

type JoinOutput struct {
	Body struct {
		ID   string `json:"id" example:"dlock-node-0" doc:"ID of a node"`
		Addr string `json:"addr" example:"localhost:12001" doc:"IP address and a port of a service"`
	}
}

type AcquireInput struct {
	Key  string `path:"key" maxLength:"1024" example:"migration_lock:1" doc:"Key for the lock"`
	Body struct {
		TTL int `json:"ttl" minimum:1 example:60 doc:"Time to live for the lock in seconds"`
	}
}

type AcquireOutput struct {
	Status int
	Body   struct {
		Status string `json:"status" example:"ACQUIRED" doc:"Status of the acquire operation"`
	}
}

type ReleaseInput struct {
	Key string `path:"key" maxLength:"1024" example:"migration_lock:1" doc:"Key for the lock"`
}

type ReleaseOutput struct {
	Status int
	Body   struct {
		Status string `json:"status" example:"RELEASED" doc:"Status of the release operation"`
	}
}
