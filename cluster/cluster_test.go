package cluster

import (
	"errors"
	"net"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestCluster_Init tests the Init method of the Cluster
func TestCluster_Init(t *testing.T) {
	serviceDiscovery := createMockServiceDiscoverySRV()

	c := NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err := c.Init()

	assert.Nil(t, err)
	assert.Equal(t, "dlock-2", c.hostname)
	assert.Equal(t, "51.11.1.2", c.ip)
	assert.Equal(t, []string{"dlock-0:8000", "dlock-1:8000"}, c.hosts)
}

// TestCluster_InitError tests the Init method of the Cluster with an error
func TestCluster_InitError(t *testing.T) {
	SRVError := errors.New("Error getting SRV records")
	IPError := errors.New("Error getting IP")
	HostnameError := errors.New("Error getting Hostname")

	serviceDiscovery := createMockServiceDiscoverySRV()
	serviceDiscovery.lookupSRVFn = func(service, proto, name string) (string, []*net.SRV, error) {
		return "", nil, SRVError
	}

	c := NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err := c.Init()

	assert.Error(t, SRVError, err)

	serviceDiscovery = createMockServiceDiscoverySRV()
	serviceDiscovery.lookupIPFn = func(host string) ([]string, error) {
		return nil, errors.New("Error getting IP")
	}

	c = NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err = c.Init()

	assert.Error(t, IPError, err)

	serviceDiscovery = createMockServiceDiscoverySRV()
	serviceDiscovery.lookupHostnameFn = func() (string, error) {
		return "", HostnameError
	}

	c = NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err = c.Init()

	assert.Error(t, HostnameError, err)
}

// TestCluster_NodeID tests the NodeID method of the Cluster
func TestCluster_NodeID(t *testing.T) {
	serviceDiscovery := createMockServiceDiscoverySRV()
	serviceDiscovery.lookupHostnameFn = func() (string, error) {
		return "dlock-0", nil
	}

	c := NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err := c.Init()
	assert.Nil(t, err)

	assert.Equal(t, "dlock-0:8000", c.NodeID())

	serviceDiscovery.lookupHostnameFn = func() (string, error) {
		return "dlock-2", nil
	}

	c = NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err = c.Init()
	assert.Nil(t, err)

	assert.Equal(t, "dlock-2:8000", c.NodeID())
}

// TestCluster_RaftAddr tests the RaftAddr method of the Cluster
func TestCluster_RaftAddr(t *testing.T) {
	serviceDiscovery := createMockServiceDiscoverySRV()
	serviceDiscovery.lookupHostnameFn = func() (string, error) {
		return "dlock-0", nil
	}

	c := NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err := c.Init()
	assert.Nil(t, err)

	assert.Equal(t, "dlock-0:12000", c.RaftAddr())

	serviceDiscovery.lookupHostnameFn = func() (string, error) {
		return "dlock-2", nil
	}

	c = NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err = c.Init()
	assert.Nil(t, err)

	assert.Equal(t, "dlock-2:12000", c.RaftAddr())
}

// TestCluster_Hosts tests the Hosts method of the Cluster
func TestCluster_Hosts(t *testing.T) {
	serviceDiscovery := createMockServiceDiscoverySRV()

	c := NewCluster(
		&logrus.Logger{}, serviceDiscovery, "test-namespace", "dlock", "8000",
	)

	err := c.Init()
	assert.Nil(t, err)

	assert.Equal(t, []string{"dlock-0:8000", "dlock-1:8000"}, c.Hosts())
}

func createMockServiceDiscoverySRV() *ServiceDiscoverySRV {
	return &ServiceDiscoverySRV{
		namespace:   "test-namespace",
		serviceName: "dlock",
		lookupSRVFn: func(service, proto, name string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "dlock-0", Port: 8000},
				{Target: "dlock-1", Port: 8000},
				{Target: "dlock-2", Port: 8000},
			}, nil
		},
		lookupIPFn: func(host string) ([]string, error) {
			return []string{"51.11.1.2"}, nil
		},
		lookupHostnameFn: func() (string, error) {
			return "dlock-2", nil
		},
	}
}
