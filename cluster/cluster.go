package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Cluster struct {
	namespace   string
	serviceName string
	hostname    string
	ip          string
	nodeID      string
	hosts       []string
	httpAddr    string
	joinAddr    string
	raftAddr    string

	logger *logrus.Logger
}

func NewCluster(logger *logrus.Logger, namespace, ServiceName, httpAddr string) *Cluster {
	c := &Cluster{
		namespace:   namespace,
		serviceName: ServiceName,
		httpAddr:    httpAddr,
		logger:      logger,
	}

	return c
}

func (c *Cluster) getIP() (string, error) {
	addrs, err := net.LookupHost(c.hostname)
	if err != nil {
		return "", err
	}

	for _, a := range addrs {
		return a, nil
	}
	return "", errors.New("Couldn't locat the IP")
}

func (c *Cluster) Init() error {
	var err error
	c.hostname, err = os.Hostname()
	if err != nil {
		c.logger.Warnf("Error getting hostname: %s", err)
		return err
	}

	c.ip, err = c.getIP()
	if err != nil {
		c.logger.Errorf("Couldn't lookup the IP: %v\n", err)
		return err
	}

	service := fmt.Sprintf("%s-internal.default.svc.cluster.local", c.serviceName)
	_, addrs, err := net.LookupSRV("", "", service)
	if err != nil {
		c.logger.Warningln("Error:", err)
	}

	for _, srv := range addrs {
		if strings.HasPrefix(srv.Target, c.hostname) {
			c.nodeID = srv.Target
		} else {
			c.hosts = append(c.hosts, srv.Target)
		}

	}

	if strings.HasPrefix(c.nodeID, fmt.Sprintf("%s-0", c.serviceName)) {
		if len(c.hosts) >= 1 {
			c.joinAddr = fmt.Sprintf("%s:%s", c.hosts[0], c.httpAddr)
		}
	} else {
		c.joinAddr = fmt.Sprintf(
			"%s-0.%s-internal.default.svc.cluster.local.:%s",
			c.serviceName,
			c.serviceName,
			c.httpAddr,
		)
	}

	c.raftAddr = fmt.Sprintf("%s:12000", c.nodeID)

	c.logger.Debugf(
		"Current node is %s discovered hosts %+v joinAddr %s raftAddr %s",
		c.nodeID,
		c.hosts,
		c.joinAddr,
		c.raftAddr,
	)

	return nil
}

func (c *Cluster) NodeID() string {
	return c.nodeID
}

func (c *Cluster) JoinAddr() string {
	return c.joinAddr
}

func (c *Cluster) RaftAddr() string {
	return c.raftAddr
}

func (c *Cluster) Hosts() []string {
	return c.hosts
}

func (c *Cluster) LeaderChanged(isLeader bool) {
	if isLeader {
		err := c.UpdateServiceEndpointSlice()
		if err != nil {
			c.logger.Errorf("Failed to update service edpoint sclice: %s", err.Error())
		}
	}
}

func (c *Cluster) UpdateServiceEndpointSlice() error {
	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	// Create Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	err = clientset.DiscoveryV1().EndpointSlices(c.namespace).Delete(
		context.TODO(), c.serviceName, metav1.DeleteOptions{},
	)
	if err != nil {
		return err
	}

	name := "http"
	ready := true
	var port int32 = 80

	i, err := strconv.ParseInt(c.httpAddr, 10, 32)
	if err != nil {
		return err
	}
	port = int32(i)

	// Define the new EndpointSlice object
	newEndpointSlice := &discoveryv1.EndpointSlice{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "discovery.k8s.io/v1",
			Kind:       "EndpointSlice",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.serviceName,
			Namespace: c.namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name": c.serviceName,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{c.ip},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &ready,
				},
				Hostname: &c.hostname,
			},
		},

		Ports: []discoveryv1.EndpointPort{
			{
				Name: &name,
				Port: &port,
			},
		},
	}

	// Create the new EndpointSlice
	createdEndpointSlice, err := clientset.DiscoveryV1().EndpointSlices(c.namespace).Create(
		context.TODO(), newEndpointSlice, metav1.CreateOptions{},
	)
	if err != nil {
		return err
	}

	c.logger.Infof("EndpointSlice %s created successfully!\n", createdEndpointSlice.Name)

	return nil
}
