package cluster

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type Cluster struct {
	namespace        string
	serviceName      string
	serviceDiscovery ServiceDiscovery
	hostname         string
	ip               string
	nodeID           string
	hosts            []string
	httpAddr         string
	raftAddr         string

	logger *logrus.Logger
}

func NewCluster(logger *logrus.Logger, serviceDiscovery ServiceDiscovery, namespace, ServiceName, httpAddr string) *Cluster {
	c := &Cluster{
		namespace:        namespace,
		serviceName:      ServiceName,
		httpAddr:         httpAddr,
		logger:           logger,
		serviceDiscovery: serviceDiscovery,
	}

	return c
}

func (c *Cluster) Init() error {
	var err error
	c.hostname, err = c.serviceDiscovery.Hostname()
	if err != nil {
		c.logger.Warnf("Error getting hostname: %s", err)
		return err
	}

	c.ip, err = c.serviceDiscovery.IP()
	if err != nil {
		c.logger.Errorf("Couldn't lookup the IP: %v\n", err)
		return err
	}

	addrs, err := c.serviceDiscovery.Lookup()
	if err != nil {
		c.logger.Warningln("Error:", err)
		return err
	}

	for _, addr := range addrs {
		if strings.HasPrefix(addr, c.hostname) {
			c.nodeID = addr
		} else {
			c.hosts = append(c.hosts, addr)
		}
	}

	host, _, err := net.SplitHostPort(c.nodeID)
	if err != nil {
		c.logger.Warningf("Error splitting host and port: %s %v\n", c.nodeID, err)
	}
	c.raftAddr = fmt.Sprintf("%s:12000", host)

	c.logger.Debugf(
		"Current node is %s discovered hosts %+v raftAddr %s",
		c.nodeID,
		c.hosts,
		c.raftAddr,
	)

	return nil
}

func (c *Cluster) NodeID() string {
	return c.nodeID
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
