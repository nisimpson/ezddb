package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Item = map[string]types.AttributeValue

// BatchGetter implements the dynamodb Batch Get API.
//
//go:generate mockery --name BatchGetter
type BatchGetter interface {
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

// BatchWriter implements the dynamodb Batch Write API.
//
//go:generate mockery --name BatchWriter
type BatchWriter interface {
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// Deleter implements the dynamodb Delete API.
//
//go:generate mockery --name Deleter
type Deleter interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Getter implements the dynamodb Get API.
//
//go:generate mockery --name Getter
type Getter interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

// Putter implements the dynamodb Put API.
//
//go:generate mockery --name Putter
type Putter interface {
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// Querier implements the dynamodb Query API.
//
//go:generate mockery --name Querier
type Querier interface {
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Scanner implements the dynamodb Scan API.
//
//go:generate mockery --name Scanner
type Scanner interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// TransactionGetter implements the dynamodb Transact Get API.
//
//go:generate mockery --name TransactionGetter
type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// TransactionWriter implements the dynamodb Transact Write API.
//
//go:generate mockery --name TransactionWriter
type TransactionWriter interface {
	TransactWriteItems(context.Context,
		*dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// Updater implements the dynamodb Update API.
//
//go:generate mockery --name Updater
type Updater interface {
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}
