package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TransactionWriter implements the dynamodb Transact Write API.
type TransactionWriter interface {
	TransactWriteItems(context.Context,
		*dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// TransactionWriteOperation functions generate dynamodb input data given some context.
type TransactionWriteOperation func(context.Context) (*dynamodb.TransactWriteItemsInput, error)

// NewTransactionWriteOperation returns a new transaction write Operation instance.
func NewTransactionWriteOperation() TransactionWriteOperation {
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return &dynamodb.TransactWriteItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionWriteOperation) Invoke(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
	return t(ctx)
}

// TransactionWriteModifier makes modifications to the input before the Operation is executed.
type TransactionWriteModifier interface {
	// ModifyTransactWriteItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactWriteItemsInput(context.Context, *dynamodb.TransactWriteItemsInput) error
}

// TransactionWriteModifierFunc is a function that implements TransactionWriteModifier.
type TransactionWriteModifierFunc modifier[dynamodb.TransactWriteItemsInput]

func (t TransactionWriteModifierFunc) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactionWriteOperation) Modify(modifiers ...TransactionWriteModifier) TransactionWriteOperation {
	mapper := func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, mod TransactionWriteModifier) error {
		return mod.ModifyTransactWriteItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return modify[dynamodb.TransactWriteItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactionWriteOperation) Execute(ctx context.Context,
	writer TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.TransactWriteItems(ctx, input, options...)
	}
}
