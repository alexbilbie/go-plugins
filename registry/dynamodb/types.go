package dynamodb

import "github.com/micro/go-micro/registry"

type DDBService struct {
	Type      string
	Name      string
	Version   string
	Metadata  map[string]string
	Endpoints []DDBEndpoint
	TTL       int64
}

func NewDDBService(service *registry.Service) DDBService {
	return DDBService{
		Type:     typeSrv,
		Name:     service.Name,
		Version:  service.Version,
		Metadata: service.Metadata,
		Endpoints: func(endpoints []*registry.Endpoint) []DDBEndpoint {
			var ddbEndpoints []DDBEndpoint
			for _, e := range service.Endpoints {
				ddbEndpoints = append(ddbEndpoints, NewDDBEndpoint(e))
			}
			return ddbEndpoints
		}(service.Endpoints),
	}
}

func (s DDBService) ToRegistryService() *registry.Service {
	var endpoints []*registry.Endpoint
	for _, e := range s.Endpoints {
		endpoints = append(
			endpoints,
			&registry.Endpoint{
				Name: e.Name,
				Request: &registry.Value{
					Name: e.Request.Name,
					Type: e.Request.Type,
					Values: func(dv []DDBValue) []*registry.Value {
						var values []*registry.Value
						for _, v := range dv {
							values = append(values, v.ToRegistryValue())
						}
						return values
					}(e.Request.Values),
				},
				Response: &registry.Value{
					Name: e.Response.Name,
					Type: e.Response.Type,
					Values: func(dv []DDBValue) []*registry.Value {
						var values []*registry.Value
						for _, v := range dv {
							values = append(values, v.ToRegistryValue())
						}
						return values
					}(e.Response.Values),
				},
				Metadata: e.Metadata,
			},
		)
	}

	return &registry.Service{
		Name:      s.Name,
		Version:   s.Version,
		Metadata:  s.Metadata,
		Endpoints: endpoints,
	}
}

type DDBNode struct {
	Type           string
	Name           string
	ServiceName    string
	ServiceVersion string
	Address        string
	Port           int
	Metadata       map[string]string
	TTL            int64
}

func NewDDBNode(node *registry.Node) DDBNode {
	return DDBNode{
		Type:    typeNode,
		Name:    node.Id,
		Address: node.Address,
		Port:    node.Port,
	}
}

func (n DDBNode) ToRegistryValue() *registry.Node {
	return &registry.Node{
		Id:       n.ServiceName,
		Address:  n.Address,
		Port:     n.Port,
		Metadata: n.Metadata,
	}
}

type DDBEndpoint struct {
	Name     string
	Request  DDBValue
	Response DDBValue
	Metadata map[string]string
}

func NewDDBEndpoint(e *registry.Endpoint) DDBEndpoint {
	return DDBEndpoint{
		e.Name,
		NewDBBValue(e.Request),
		NewDBBValue(e.Response),
		e.Metadata,
	}
}

type DDBValue struct {
	Name   string
	Type   string
	Values []DDBValue
}

func NewDBBValue(n *registry.Value) DDBValue {
	var values []DDBValue
	for _, vs := range n.Values {
		values = append(values, NewDBBValue(vs))
	}
	return DDBValue{
		Name:   n.Name,
		Type:   n.Type,
		Values: values,
	}
}

func (v DDBValue) ToRegistryValue() *registry.Value {
	var values []*registry.Value
	for _, vs := range v.Values {
		values = append(values, vs.ToRegistryValue())
	}

	return &registry.Value{
		Name:   v.Name,
		Type:   v.Type,
		Values: values,
	}
}
