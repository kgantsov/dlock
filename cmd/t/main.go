package main

import (
	"context"
	"fmt"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func main() {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}

	// Create Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// Specify the namespace where you want to create the EndpointSlice
	namespace := "default"

	err = clientset.DiscoveryV1().EndpointSlices(namespace).Delete(context.TODO(), "dlock", metav1.DeleteOptions{})
	if err != nil {
		panic(err.Error())
	}

	name := "http"
	var port int32 = 11000
	ready := true
	hostname := "dlock-0"

	// Define the new EndpointSlice object
	newEndpointSlice := &discoveryv1.EndpointSlice{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "discovery.k8s.io/v1",
			Kind:       "EndpointSlice",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dlock", // Provide a unique name for your EndpointSlice
			Namespace: namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name": "dlock",
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.42.0.43"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: &ready,
				},
				Hostname: &hostname,
			},

			// Add more endpoints as needed
		},

		Ports: []discoveryv1.EndpointPort{
			{
				Name: &name,
				Port: &port,
			},
		},
	}

	// Create the new EndpointSlice
	createdEndpointSlice, err := clientset.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), newEndpointSlice, metav1.CreateOptions{})
	if err != nil {
		panic(err.Error())
	}

	fmt.Printf("EndpointSlice %s created successfully!\n", createdEndpointSlice.Name)

	// // Define the new EndpointSlice object
	// newEndpointSlice := &v1beta1.EndpointSlice{
	// 	TypeMeta: metav1.TypeMeta{
	// 		APIVersion: "discovery.k8s.io/v1beta1",
	// 		Kind:       "EndpointSlice",
	// 	},
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		Name:      "dlock", // Provide a unique name for your EndpointSlice
	// 		Namespace: namespace,
	// 	},
	// 	AddressType: "IPv4",
	// 	Endpoints: []v1beta1.Endpoint{
	// 		{
	// 			Addresses: []string{"dlock-0.dlock-internal.default.svc.cluster.local"},
	// 		},
	// 		// Add more endpoints as needed
	// 	},
	// 	Ports: []v1beta1.EndpointPort{
	// 		{
	// 			Name: &name,
	// 			Port: &port,
	// 		},
	// 	},
	// }

	// // Create the new EndpointSlice
	// createdEndpointSlice, err := clientset.DiscoveryV1beta1().EndpointSlices(namespace).Create(context.TODO(), newEndpointSlice, metav1.CreateOptions{})
	// if err != nil {
	// 	panic(err.Error())
	// }

	// fmt.Printf("EndpointSlice %s created successfully!\n", createdEndpointSlice.Name)
}
