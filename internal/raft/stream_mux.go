package raft

import (
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

type MuxStreamLayer struct {
	listener net.Listener
	conns    chan net.Conn
	groupID  string
}

func NewMuxStreamLayer(listener net.Listener, groupID string) *MuxStreamLayer {
	return &MuxStreamLayer{
		listener: listener,
		groupID:  groupID,
	}
}

func (m *MuxStreamLayer) Accept() (net.Conn, error) {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			return nil, err
		}
		// Read group ID prefix
		groupBuf := make([]byte, 2)
		if _, err := io.ReadFull(conn, groupBuf); err != nil {
			conn.Close()
			continue
		}
		group := string(groupBuf)
		if group == m.groupID {
			return conn, nil
		}
		conn.Close() // or forward elsewhere
	}
}

func (m *MuxStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", string(address), timeout)
	if err != nil {
		return nil, err
	}
	// Write group ID prefix
	_, err = conn.Write([]byte(m.groupID))
	return conn, err
}

func (m *MuxStreamLayer) Addr() net.Addr {
	return m.listener.Addr()
}

func (m *MuxStreamLayer) Close() error {
	return m.listener.Close()
}
