package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type BatchWriter interface {
	BatchWriteItem(context.Context, *dynamodb.BatchWriteItemInput, ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error)
}

type BatchWriteProcedure func(context.Context) (*dynamodb.BatchWriteItemInput, error)

func (g BatchWriteProcedure) Invoke(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
	return g(ctx)
}

type BatchWriteModifier interface {
	ModifyBatchWriteItemInput(context.Context, *dynamodb.BatchWriteItemInput) error
}

func (b BatchWriteProcedure) Modify(modifiers ...BatchWriteModifier) BatchWriteProcedure {
	mapper := func(ctx context.Context, input *dynamodb.BatchWriteItemInput, mod BatchWriteModifier) error {
		return mod.ModifyBatchWriteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return modify[dynamodb.BatchWriteItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

func (b BatchWriteProcedure) Execute(ctx context.Context,
	writer BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.BatchWriteItem(ctx, input, options...)
	}
}
