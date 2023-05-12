package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

type TransactionGetProcedure func(context.Context) (*dynamodb.TransactGetItemsInput, error)

func (t TransactionGetProcedure) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

type TransactionGetModifier interface {
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

func (t TransactionGetProcedure) Modify(modifiers ...TransactionGetModifier) TransactionGetProcedure {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactionGetModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify[dynamodb.TransactGetItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

func (t TransactionGetProcedure) Execute(ctx context.Context,
	getter TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}
