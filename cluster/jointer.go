package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

type Joiner struct {
	nodeID   string
	raftAddr string
	httpAddr string
	hosts    []string

	logger *logrus.Logger
}

func NewJoiner(logger *logrus.Logger, nodeID, raftAddr, httpAddr string, hosts []string) *Joiner {
	j := &Joiner{
		nodeID:   nodeID,
		raftAddr: raftAddr,
		httpAddr: httpAddr,
		hosts:    hosts,

		logger: logger,
	}

	return j
}

func (j *Joiner) Join() error {
	if len(j.hosts) == 0 {
		j.logger.Debugf("There is no hosts to join: %d", len(j.hosts))
		return nil
	}

	var host string
	var err error

	for i := 0; i < 3; i++ {
		for _, host = range j.hosts {
			host = fmt.Sprintf("%s:%s", host, j.httpAddr)

			j.logger.Debugf("Trying to join: %s", host)

			if err = j.join(host, j.raftAddr, j.nodeID); err == nil {
				return nil
			}
		}
		time.Sleep(time.Duration(1) * time.Second)
	}

	return errors.New(fmt.Sprintf("failed to join node at %s: %s", host, err.Error()))
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

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Non-OK HTTP status:", resp.StatusCode)
		return errors.New(fmt.Sprintf("Failed to join: %s", joinAddr))
	}

	body, err := ioutil.ReadAll(resp.Body)
	j.logger.Infof("JOINED %+v %+v", resp.StatusCode, string(body))
	return nil
}
