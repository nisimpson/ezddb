package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Querier interface {
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

type QueryProcedure func(context.Context) (*dynamodb.QueryInput, error)

func (q QueryProcedure) Invoke(ctx context.Context) (*dynamodb.QueryInput, error) {
	return q(ctx)
}

type QueryModifier interface {
	ModifyQueryInput(context.Context, *dynamodb.QueryInput) error
}

func (p QueryProcedure) Modify(modifiers ...QueryModifier) QueryProcedure {
	mapper := func(ctx context.Context, input *dynamodb.QueryInput, mod QueryModifier) error {
		return mod.ModifyQueryInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return modify[dynamodb.QueryInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

func (p QueryProcedure) Execute(ctx context.Context,
	querier Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return querier.Query(ctx, input, options...)
	}
}

func (p QueryProcedure) WithPagination(callback PageQueryCallback) QueryExecutor {
	return func(ctx context.Context, q Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
		input, err := p.Invoke(ctx)
		if err != nil {
			return nil, err
		}
		for {
			if output, err := q.Query(ctx, input, options...); err != nil {
				return nil, err
			} else if ok := callback(ctx, output); !ok {
				return output, nil
			} else if output.LastEvaluatedKey == nil {
				return output, nil
			} else {
				input.ExclusiveStartKey = output.LastEvaluatedKey
			}
		}
	}
}

type PageQueryCallback = func(context.Context, *dynamodb.QueryOutput) bool

type QueryExecutor func(context.Context, Querier, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)

func (q QueryExecutor) Execute(ctx context.Context, querier Querier, options ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return q(ctx, querier, options...)
}
