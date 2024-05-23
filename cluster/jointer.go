package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/sirupsen/logrus"
)

type Joiner struct {
	nodeID   string
	joinAddr string
	raftAddr string

	logger *logrus.Logger
}

func NewJoiner(logger *logrus.Logger, nodeID, joinAddr, raftAddr string) *Joiner {
	j := &Joiner{
		nodeID:   nodeID,
		joinAddr: joinAddr,
		raftAddr: raftAddr,

		logger: logger,
	}

	return j
}

func (j *Joiner) Join() error {
	if j.joinAddr == "" {
		return nil
	}

	if err := j.join(j.joinAddr, j.raftAddr, j.nodeID); err != nil {
		return errors.New(fmt.Sprintf("failed to join node at %s: %s", j.joinAddr, err.Error()))
	}

	return nil
}

func (j *Joiner) join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/join", joinAddr), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	j.logger.Infof("JOINED %+v %+v", resp.StatusCode, string(body))
	return nil
}
