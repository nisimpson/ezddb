package operation

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TransactionGetter implements the dynamodb Transact Get API.
type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// TransactionGetOperation functions generate dynamodb input data given some context.
type TransactionGetOperation func(context.Context) (*dynamodb.TransactGetItemsInput, error)

// NewTransactionGetOperation returns a new transaction get Operation instance.
func NewTransactionGetOperation() TransactionGetOperation {
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return &dynamodb.TransactGetItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionGetOperation) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

// TransactionGetModifier makes modifications to the input before the Operation is executed.
type TransactionGetModifier interface {
	// ModifyTransactGetItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

// TransactionGetModifierFunc is a function that implements TransactionGetModifier.
type TransactionGetModifierFunc modifier[dynamodb.TransactGetItemsInput]

func (t TransactionGetModifierFunc) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactionGetOperation) Modify(modifiers ...TransactionGetModifier) TransactionGetOperation {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactionGetModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify[dynamodb.TransactGetItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactionGetOperation) Execute(ctx context.Context,
	getter TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}
