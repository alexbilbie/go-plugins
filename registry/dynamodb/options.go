package dynamodb

import (
	"context"

	"github.com/micro/go-micro/registry"
)

type tableNameKey struct{}

// TableName sets the DynamoDB table name as a registry option
func TableName(tableName string) registry.Option {
	return func(o *registry.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, tableNameKey{}, tableName)
	}
}
