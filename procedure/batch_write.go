package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// BatchWriter implements the dynamodb Batch Write API.
type BatchWriter interface {
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

// BatchWriteProcedure functions generate dynamodb put input data given some context.
type BatchWriteProcedure func(context.Context) (*dynamodb.BatchWriteItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchWriteProcedure) Invoke(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
	return g(ctx)
}

// BatchWriteModifier makes modifications to the input before the procedure is executed.
type BatchWriteModifier interface {
	// ModifyBatchWriteItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchWriteItemInput(context.Context, *dynamodb.BatchWriteItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (b BatchWriteProcedure) Modify(modifiers ...BatchWriteModifier) BatchWriteProcedure {
	mapper := func(ctx context.Context, input *dynamodb.BatchWriteItemInput, mod BatchWriteModifier) error {
		return mod.ModifyBatchWriteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return modify[dynamodb.BatchWriteItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (b BatchWriteProcedure) Execute(ctx context.Context,
	writer BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.BatchWriteItem(ctx, input, options...)
	}
}
