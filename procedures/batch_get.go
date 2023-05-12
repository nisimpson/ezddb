package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type BatchGetter interface {
	BatchGetItem(context.Context, *dynamodb.BatchGetItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

type BatchGetProcedure func(context.Context) (*dynamodb.BatchGetItemInput, error)

func (g BatchGetProcedure) Invoke(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
	return g(ctx)
}

type BatchGetModifier interface {
	ModifyBatchGetItemInput(context.Context, *dynamodb.BatchGetItemInput) error
}

func (b BatchGetProcedure) Modify(modifiers ...BatchGetModifier) BatchGetProcedure {
	mapper := func(ctx context.Context, input *dynamodb.BatchGetItemInput, mod BatchGetModifier) error {
		return mod.ModifyBatchGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return modify[dynamodb.BatchGetItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

func (b BatchGetProcedure) Execute(ctx context.Context,
	getter BatchGetter, options ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.BatchGetItem(ctx, input, options...)
	}
}
