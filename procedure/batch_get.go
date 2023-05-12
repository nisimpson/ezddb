package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)


// BatchGetter implements the dynamodb Batch Get API.
type BatchGetter interface {
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

// BatchGetProcedure functions generate dynamodb put input data given some context.
type BatchGetProcedure func(context.Context) (*dynamodb.BatchGetItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchGetProcedure) Invoke(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
	return g(ctx)
}

// BatchGetModifier makes modifications to the input before the procedure is executed.
type BatchGetModifier interface {
	// ModifyBatchGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchGetItemInput(context.Context, *dynamodb.BatchGetItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (b BatchGetProcedure) Modify(modifiers ...BatchGetModifier) BatchGetProcedure {
	mapper := func(ctx context.Context, input *dynamodb.BatchGetItemInput, mod BatchGetModifier) error {
		return mod.ModifyBatchGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return modify[dynamodb.BatchGetItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (b BatchGetProcedure) Execute(ctx context.Context,
	getter BatchGetter, options ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.BatchGetItem(ctx, input, options...)
	}
}
