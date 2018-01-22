package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/registry"
	"github.com/pkg/errors"
)

const (
	registryName = "dynamodb"
	typeNode     = "node"
	typeSrv      = "service"
	keyType      = "Type"
	keyName      = "Name"
)

func init() {
	cmd.DefaultRegistries[registryName] = NewRegistry
}

type dynamoDBRegistry struct {
	client  dynamodbiface.DynamoDBAPI
	options registry.Options
}

func (r dynamoDBRegistry) Register(service *registry.Service, opts ...registry.RegisterOption) error {
	if len(service.Nodes) == 0 {
		return errors.New("Require at least one node to register service")
	}

	var options registry.RegisterOptions
	for _, o := range opts {
		o(&options)
	}

	var expiry = int64(options.TTL) + time.Now().Unix()
	var items []map[string]*dynamodb.AttributeValue

	s := NewDDBService(service)
	if options.TTL > time.Duration(0) {
		s.TTL = expiry
	}
	av, _ := dynamodbattribute.MarshalMap(s)
	items = append(items, av)

	for _, node := range service.Nodes {
		n := NewDDBNode(node)
		if options.TTL > time.Duration(0) {
			n.TTL = expiry
		}
		av, _ := dynamodbattribute.MarshalMap(n)
		items = append(items, av)
	}

	//fmt.Printf("%+v\n", items)
	//os.Exit(1)

	var writeRequests []*dynamodb.WriteRequest
	for _, item := range items {
		writeRequests = append(writeRequests, &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		})
	}

	result, err := r.client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			r.getTableName(): writeRequests,
		},
	})

	if err != nil {
		return err
	}

	if len(result.UnprocessedItems) > 0 {
		return errors.New(fmt.Sprintf("%d items were not registered\n", len(result.UnprocessedItems)))
	}

	return nil
}

func (r dynamoDBRegistry) Deregister(service *registry.Service) error {

	if len(service.Nodes) == 0 {
		return errors.New("Require at least one node to deregister")
	}

	_, err := r.client.DeleteItem(&dynamodb.DeleteItemInput{
		TableName: aws.String(r.getTableName()),
		Key: map[string]*dynamodb.AttributeValue{
			keyType: {S: aws.String(typeNode)},
			keyName: {S: aws.String(service.Nodes[0].Id)},
		},
	})
	if err != nil {
		return errors.Wrap(err, "DynamoDB registry deregister node error")
	}

	srv, err := r.GetService(service.Name)
	if err != nil {
		panic(err)
		return err
	}

	if len(srv) == 0 {
		return nil
	}

	if len(srv[0].Nodes) == 0 {
		_, err = r.client.DeleteItem(&dynamodb.DeleteItemInput{
			TableName: aws.String(r.getTableName()),
			Key: map[string]*dynamodb.AttributeValue{
				keyType: {S: aws.String(typeSrv)},
				keyName: {S: aws.String(service.Name)},
			},
		})
		if err != nil {
			panic(errors.Wrap(err, "DynamoDB registry deregister service error"))
			return errors.Wrap(err, "DynamoDB registry deregister service error")
		}
	}

	return nil
}

func (r dynamoDBRegistry) GetService(serviceName string) ([]*registry.Service, error) {
	fmt.Printf("Finding service called %s\n", serviceName)

	getItemResult, err := r.client.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(r.getTableName()),
		Key: map[string]*dynamodb.AttributeValue{
			keyType: {S: aws.String(typeSrv)},
			keyName: {S: aws.String(serviceName)},
		},
	})

	if err != nil {
		panic(err)
		return nil, err
	}

	if len(getItemResult.Item) == 0 {
		fmt.Println("not found")
		return nil, registry.ErrNotFound
	}

	var ddbService DDBService
	if dynamodbattribute.UnmarshalMap(getItemResult.Item, &ddbService) != nil {
		return nil, errors.Wrap(err, "DynamoDB registry GetService DynamoDB unmarshal error of service")
	}

	queryResult, err := r.client.Query(&dynamodb.QueryInput{
		TableName:      aws.String(r.getTableName()),
		ConsistentRead: aws.Bool(true),
		KeyConditions: map[string]*dynamodb.Condition{
			keyType: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(typeNode),
					},
				},
				ComparisonOperator: aws.String("EQ"),
			},
			keyName: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(serviceName),
					},
				},
				ComparisonOperator: aws.String("BEGINS_WITH"),
			},
		},
	})

	if err != nil {
		return nil, errors.Wrap(err, "DynamoDB registry GetService error")
	}

	var ddbNodes []DDBNode
	for _, item := range queryResult.Items {
		var node DDBNode
		if dynamodbattribute.UnmarshalMap(item, &node) != nil {
			return nil, errors.Wrap(err, "DynamoDB registry GetService DynamoDB unmarshal node error")
		}
		ddbNodes = append(ddbNodes, node)
	}

	service := ddbService.ToRegistryService()
	service.Nodes = func(ns []DDBNode) []*registry.Node {
		var nodes []*registry.Node
		for _, n := range ns {
			nodes = append(nodes, n.ToRegistryValue())
		}
		return nodes
	}(ddbNodes)

	return []*registry.Service{service}, nil
}

func (r dynamoDBRegistry) ListServices() ([]*registry.Service, error) {

	queryResult, err := r.client.Query(&dynamodb.QueryInput{
		TableName:      aws.String(r.getTableName()),
		ConsistentRead: aws.Bool(true),
		KeyConditions: map[string]*dynamodb.Condition{
			keyType: {
				AttributeValueList: []*dynamodb.AttributeValue{
					{
						S: aws.String(typeSrv),
					},
				},
				ComparisonOperator: aws.String("EQ"),
			},
		},
	})

	if err != nil {
		return nil, errors.Wrap(err, "DynamoDB registry ListServices error")
	}

	if len(queryResult.Items) == 0 {
		return nil, nil
	}

	var services []*registry.Service
	for _, item := range queryResult.Items {
		var srv DDBService
		if dynamodbattribute.UnmarshalMap(item, &srv) != nil {
			return nil, errors.Wrap(err, "DynamoDB registry ListServices DynamoDB unmarshal error")
		}
		services = append(services, srv.ToRegistryService())
	}

	return services, nil
}

func (r dynamoDBRegistry) Watch() (registry.Watcher, error) {
	panic("implement me")
}

func (r dynamoDBRegistry) String() string {
	return registryName
}

func (r dynamoDBRegistry) getTableName() string {
	raw := r.options.Context.Value(tableNameKey{})
	if raw != nil {
		return raw.(string)
	}
	return ""
}

func NewRegistry(opts ...registry.Option) registry.Registry {
	options := registry.Options{
		Context: context.Background(),
	}

	for _, o := range opts {
		o(&options)
	}

	return dynamoDBRegistry{
		newClient(),
		options,
	}
}

func newClient() dynamodbiface.DynamoDBAPI {
	s := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	//s.Config = s.Config.WithLogLevel(aws.LogDebugWithHTTPBody)

	return dynamodb.New(s)
}
