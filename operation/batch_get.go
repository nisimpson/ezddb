package operation

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// BatchGetOperation functions generate dynamodb put input data given some context.
type BatchGetOperation func(context.Context) (*dynamodb.BatchGetItemInput, error)

// NewBatchGetOperation creates a new batch get Operation instance.
func NewBatchGetOperation() BatchGetOperation {
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]types.KeysAndAttributes),
		}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchGetOperation) Invoke(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
	return g(ctx)
}

// BatchGetModifier makes modifications to the input before the Operation is executed.
type BatchGetModifier interface {
	// ModifyBatchGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchGetItemInput(context.Context, *dynamodb.BatchGetItemInput) error
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (b BatchGetOperation) Modify(modifiers ...BatchGetModifier) BatchGetOperation {
	mapper := func(ctx context.Context, input *dynamodb.BatchGetItemInput, mod BatchGetModifier) error {
		return mod.ModifyBatchGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return modify[dynamodb.BatchGetItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (b BatchGetOperation) Execute(ctx context.Context,
	getter ezddb.BatchGetter, options ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.BatchGetItem(ctx, input, options...)
	}
}
