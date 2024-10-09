package graph

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

type searcherFunc[T any] func(context.Context, *Options) (*dynamodb.QueryInput, error)

func (f searcherFunc[T]) search(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
	return f(ctx, o)
}

type collection[T Data] struct{ item T }

func CollectionOf[T Data](item T) collection[T] {
	return collection[T]{item: item}
}

func (c collection[T]) Filter(e filter.Expression, mods ...operation.QueryModifier) searcher[T] {
	return searcherFunc[T](
		func(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
			builder := expression.NewBuilder()
			itemTypeEquals := expression.Key(AttributePartitionKey).Equal(expression.Value(c.item.DynamoItemType()))
			builder = builder.WithKeyCondition(itemTypeEquals)
			var errs []error = make([]error, 0)
			var condition expression.ConditionBuilder
			var input dynamodb.QueryInput
			if e != nil {
				var err error
				condition, err = filter.Evaluate(e)
				builder = builder.WithCondition(condition)
				errs = append(errs, err)
			}
			mods = append(mods, operation.WithExpressionBuilderFunc(builder, o.BuildExpression))
			for _, mod := range mods {
				errs = append(errs, mod.ModifyQueryInput(ctx, &input))
			}
			input.TableName = &o.TableName
			input.IndexName = &o.CollectionQueryIndexName
			return &input, errors.Join(errs...)
		},
	)
}

func (c collection[T]) Get(mods ...operation.QueryModifier) searcher[T] {
	return c.Filter(nil, mods...)
}

type partition[T Data] struct{ item T }

func PartitionOf[T Data](item T) partition[T] {
	return partition[T]{item: item}
}

func (p partition[T]) Filter(e filter.Expression, mods ...operation.QueryModifier) searcher[T] {
	return searcherFunc[T](
		func(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
			builder := expression.NewBuilder()
			pk, sk := p.item.DynamoHashKey(), p.item.DynamoSortKey()

			builder = builder.WithKeyCondition(itemTypeEquals)
			var errs []error = make([]error, 0)
			var condition expression.ConditionBuilder
			var input dynamodb.QueryInput
			if e != nil {
				var err error
				condition, err = filter.Evaluate(e)
				builder = builder.WithCondition(condition)
				errs = append(errs, err)
			}
			mods = append(mods, operation.WithExpressionBuilderFunc(builder, o.BuildExpression))
			for _, mod := range mods {
				errs = append(errs, mod.ModifyQueryInput(ctx, &input))
			}
			input.TableName = &o.TableName
			input.IndexName = &o.CollectionQueryIndexName
			return &input, errors.Join(errs...)
		},
	)
}
