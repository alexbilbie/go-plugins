package awsdiscovery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/servicediscovery"
	"github.com/aws/aws-sdk-go/service/servicediscovery/servicediscoveryiface"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/registry"
)

type awsDiscovery struct {
	client servicediscoveryiface.ServiceDiscoveryAPI
}

func init() {
	cmd.DefaultRegistries["awsdiscovery"] = NewRegistry
}

func (r *awsDiscovery) Register(s *registry.Service, opts ...registry.RegisterOption) error {

	if len(s.Nodes) == 0 {
		return errors.New("require at least one node")
	}

	for _, node := range s.Nodes {
		attrs := map[string]*string{
			"AWS_INSTANCE_PORT": aws.String(string(node.Port)),
		}

		ip := net.ParseIP(node.Address)
		if ip.To4() != nil {
			attrs["AWS_INSTANCE_IPV4"] = aws.String(node.Address)
		} else if ip.To16() != nil {
			attrs["AWS_INSTANCE_IPV6"] = aws.String(node.Address)
		} else {
			return errors.New(fmt.Sprintf("%s could not be parsed as an IPv4 or IPv6 address", node.Address))
		}

		r.client.RegisterInstance(&servicediscovery.RegisterInstanceInput{
			InstanceId: aws.String(node.Id),
			ServiceId:  aws.String(s.Name),
			Attributes: attrs,
		})
	}

	return nil
}

func (r *awsDiscovery) Deregister(s *registry.Service) error {
	for _, node := range s.Nodes {
		_, err := r.client.DeregisterInstance(&servicediscovery.DeregisterInstanceInput{
			InstanceId: aws.String(node.Id),
			ServiceId:  aws.String(s.Name),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *awsDiscovery) GetService(serviceName string) ([]*registry.Service, error) {
	result, err := r.client.ListInstances(&servicediscovery.ListInstancesInput{
		ServiceId: aws.String(serviceName),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == servicediscovery.ErrCodeServiceNotFound {
				return nil, registry.ErrNotFound
			}
		}

		return nil, err
	}

	var nodes []*registry.Node

	for _, instance := range result.Instances {
		nodes = append(nodes,
			&registry.Node{
				Id:      aws.StringValue(instance.Id),
				Address: aws.StringValue(instance.Attributes["AWS_INSTANCE_IP"]),
				Port: func(s string) int {
					i, _ := strconv.Atoi(s)
					return i
				}(aws.StringValue(instance.Attributes["AWS_INSTANCE_PORT"])),
			},
		)
	}

	return []*registry.Service{
		{
			Name:  serviceName,
			Nodes: nodes,
		},
	}, nil
}

func (r *awsDiscovery) ListServices() ([]*registry.Service, error) {
	var services []*registry.Service

	err := r.client.ListServicesPages(
		&servicediscovery.ListServicesInput{},
		func(result *servicediscovery.ListServicesOutput, lastPage bool) bool {
			for _, s := range result.Services {
				services = append(
					services,
					&registry.Service{
						Name: aws.StringValue(s.Name),
					},
				)
			}
			return lastPage
		},
	)

	if err != nil {
		return nil, err
	}

	return services, nil
}

func (r *awsDiscovery) Watch() (registry.Watcher, error) {
	return nil, errors.New("watch is not supported for route53 registry")
}

func (r *awsDiscovery) String() string {
	return "route53"
}

func NewRegistry(opts ...registry.Option) registry.Registry {

	options := registry.Options{
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := servicediscovery.New(sess)

	return &awsDiscovery{
		svc,
	}
}
