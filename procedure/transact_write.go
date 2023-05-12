package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TransactionWriter implements the dynamodb Transact Write API.
type TransactionWriter interface {
	TransactWriteItems(context.Context,
		*dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// TransactionWriteProcedure functions generate dynamodb input data given some context.
type TransactionWriteProcedure func(context.Context) (*dynamodb.TransactWriteItemsInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionWriteProcedure) Invoke(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
	return t(ctx)
}

// TransactionWriteModifier makes modifications to the input before the procedure is executed.
type TransactionWriteModifier interface {
	// ModifyTransactWriteItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactWriteItemsInput(context.Context, *dynamodb.TransactWriteItemsInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (t TransactionWriteProcedure) Modify(modifiers ...TransactionWriteModifier) TransactionWriteProcedure {
	mapper := func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, mod TransactionWriteModifier) error {
		return mod.ModifyTransactWriteItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return modify[dynamodb.TransactWriteItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (t TransactionWriteProcedure) Execute(ctx context.Context,
	writer TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.TransactWriteItems(ctx, input, options...)
	}
}
