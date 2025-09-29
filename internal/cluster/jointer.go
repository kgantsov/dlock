package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kgantsov/dlock/internal/domain"
	"github.com/rs/zerolog/log"
)

type Joiner struct {
	nodeID   string
	raftAddr string
	hosts    []string
}

func NewJoiner(nodeID, raftAddr string, hosts []string) *Joiner {
	log.Debug().Msgf("Creating new joiner: %s %s %v", nodeID, raftAddr, hosts)
	j := &Joiner{
		nodeID:   nodeID,
		raftAddr: raftAddr,
		hosts:    hosts,
	}

	return j
}

func (j *Joiner) Join() error {
	if len(j.hosts) == 0 {
		log.Debug().Msgf("There is no hosts to join: %d", len(j.hosts))
		return nil
	}

	var host string
	var err error

	for i := 0; i < 3; i++ {
		for _, host = range j.hosts {
			log.Debug().Msgf("Trying to join: %s", host)

			if err = j.join(j.nodeID, host, j.raftAddr); err == nil {
				return nil
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}

	return domain.ErrFailedToJoinNode
}

func (j *Joiner) join(nodeID, joinAddr, raftAddr string) error {
	b, err := json.Marshal(map[string]string{
		"id":        nodeID,
		"raft_addr": raftAddr,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/join", joinAddr), bytes.NewReader(b))
	if err != nil {
		log.Error().Msgf("Error creating a join request: %s", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Error().Msgf("Error sending a join request: %s", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Error().Msgf("Error joining a node, status code: %d", resp.StatusCode)
		return domain.ErrFailedToJoinNode
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error().Msgf("Error reading a join response: %s", err)
		return err
	}

	var joinResp struct {
		ID       string `json:"id"`
		RaftAddr string `json:"raft_addr"`
	}
	if err := json.Unmarshal(body, &joinResp); err != nil {
		return err
	}

	log.Info().Msgf("JOINED %+v %+v", resp.StatusCode, joinResp)

	return nil
}
