package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TransactionGetter implements the dynamodb Transact Get API.
type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// TransactionGetProcedure functions generate dynamodb input data given some context.
type TransactionGetProcedure func(context.Context) (*dynamodb.TransactGetItemsInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionGetProcedure) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

// TransactionGetModifier makes modifications to the input before the procedure is executed.
type TransactionGetModifier interface {
	// ModifyTransactGetItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (t TransactionGetProcedure) Modify(modifiers ...TransactionGetModifier) TransactionGetProcedure {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactionGetModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify[dynamodb.TransactGetItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (t TransactionGetProcedure) Execute(ctx context.Context,
	getter TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}
