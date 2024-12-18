package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Item is a type alias (and shorthand) for items stored in DynamoDB, which are represented in Golang
// as maps of attribute values.
type Item = map[string]types.AttributeValue

// Querier implements the dynamodb Query API.
type Querier interface {
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// TransactionWriter implements the dynamodb Transact Write API.
type TransactionWriter interface {
	TransactWriteItems(context.Context,
		*dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// TransactionGetter implements the dynamodb Transact Get API.
type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// Updater implements the dynamodb Update API.
type Updater interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Scanner implements the dynamodb Scan API.
type Scanner interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// Putter implements the dynamodb Put API.
type Putter interface {
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// Getter implements the dynamodb Get API.
type Getter interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

// Deleter implements the dynamodb Delete API.
type Deleter interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// BatchWriter implements the dynamodb Batch Write API.
type BatchWriter interface {
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// BatchGetter implements the dynamodb Batch Get API.
type BatchGetter interface {
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}
