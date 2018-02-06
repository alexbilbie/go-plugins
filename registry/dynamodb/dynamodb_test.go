package dynamodb

import (
	"testing"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/micro/go-micro/registry"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pkg/errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

var (
	testData = map[string][]*registry.Service{
		"foo": {
			{
				Name:    "foo",
				Version: "1.0.0",
				Nodes: []*registry.Node{
					{
						Id:      "foo-1.0.0-123",
						Address: "localhost",
						Port:    9999,
					},
					{
						Id:      "foo-1.0.0-321",
						Address: "localhost",
						Port:    9999,
					},
				},
			},
			{
				Name:    "foo",
				Version: "1.0.1",
				Nodes: []*registry.Node{
					{
						Id:      "foo-1.0.1-321",
						Address: "localhost",
						Port:    6666,
					},
				},
			},
			{
				Name:    "foo",
				Version: "1.0.3",
				Nodes: []*registry.Node{
					{
						Id:      "foo-1.0.3-345",
						Address: "localhost",
						Port:    8888,
					},
				},
			},
		},
		"bar": {
			{
				Name:    "bar",
				Version: "default",
				Nodes: []*registry.Node{
					{
						Id:      "bar-1.0.0-123",
						Address: "localhost",
						Port:    9999,
					},
					{
						Id:      "bar-1.0.0-321",
						Address: "localhost",
						Port:    9999,
					},
				},
			},
			{
				Name:    "bar",
				Version: "latest",
				Nodes: []*registry.Node{
					{
						Id:      "bar-1.0.1-321",
						Address: "localhost",
						Port:    6666,
					},
				},
			},
		},
	}
)

type mockDynamoClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m mockDynamoClient) BatchWriteItem(*dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	return &dynamodb.BatchWriteItemOutput{}, nil
}

func (m mockDynamoClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {

	if aws.StringValue(input.Key[keyName].S) == "foo" {
		item, _ := dynamodbattribute.MarshalMap(NewDDBService(testData["foo"][0]))
		return &dynamodb.GetItemOutput{Item: item}, nil
	}

	if aws.StringValue(input.Key[keyName].S) == "bar" {
		item, _ := dynamodbattribute.MarshalMap(NewDDBService(testData["bar"][0]))
		return &dynamodb.GetItemOutput{Item: item}, nil
	}

	return nil, errors.New(fmt.Sprintf("Unknown service: %s", aws.StringValue(input.Key[keyName].S)))
}

func (m mockDynamoClient) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	var ddbNodes []map[string]*dynamodb.AttributeValue
	for _, node := range testData[aws.StringValue(input.KeyConditions[keyName].AttributeValueList[0].S)][0].Nodes {
		d, _ := dynamodbattribute.MarshalMap(NewDDBNode(node))
		ddbNodes = append(ddbNodes, d)
	}

	return &dynamodb.QueryOutput{Items: ddbNodes}, nil
}

func TestDynamoDBRegistry(t *testing.T) {

	r := NewRegistryWithDynamoDBClient(&mockDynamoClient{})

	fn := func(k string, v []*registry.Service) {
		services, err := r.GetService(k)
		if err != nil {
			t.Errorf("Unexpected error getting service %s: %v", k, err)
		}

		if len(services) != len(v) {
			t.Errorf("Expected %d services for %s, got %d", len(v), k, len(services))
		}

		for _, service := range v {
			var seen bool
			for _, s := range services {
				if s.Version == service.Version {
					seen = true
					break
				}
			}
			if !seen {
				t.Errorf("expected to find version %s", service.Version)
			}
		}
	}

	// register data
	for _, v := range testData {
		for _, service := range v {
			if err := r.Register(service); err != nil {
				t.Errorf("Unexpected register error: %v", err)
			}
		}
	}

	// using test data
	for k, v := range testData {
		fn(k, v)
	}

	fn("foo", nil)

	// deregister
	/*for _, v := range testData {
		for _, service := range v {
			if err := r.Deregister(service); err != nil {
				t.Errorf("Unexpected deregister error: %v", err)
			}
		}
	}*/

}
