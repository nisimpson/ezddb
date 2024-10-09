package graph

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

type listsearch[T NodeIdentifier] struct{ item T }

func ListOf[T NodeIdentifier](item T) listsearch[T] {
	return listsearch[T]{item: item}
}

func (s listsearch[T]) Filter(e filter.Expression, mods ...operation.QueryModifier) searcher[T] {
	builder := expression.NewBuilder()
	builder = builder.WithKeyCondition(itemTypeEquals(s.item.DynamoItemType()))
	return newSearcher[T](e, builder, indexTypeCollection, mods)
}

func (s listsearch[T]) Get(mods ...operation.QueryModifier) searcher[T] {
	return s.Filter(nil, mods...)
}

type nodesearch[T NodeIdentifier] struct{ item T }

func NodeOf[T NodeIdentifier](item T) nodesearch[T] {
	return nodesearch[T]{}
}

func (s nodesearch[T]) Filter(e filter.Expression, mods ...operation.QueryModifier) searcher[T] {
	item := NewEdge(s.item)
	builder := expression.NewBuilder()
	builder = builder.WithKeyCondition(skEquals(item.SK))
	return newSearcher[T](e, builder, indexTypeReverseLookup, mods)
}

func (s nodesearch[T]) Get(mods ...operation.QueryModifier) searcher[T] {
	return s.Filter(nil, mods...)
}

type edgesearch[T NodeIdentifier, U NodeIdentifier] struct{ item T }

func EdgesOf[T NodeIdentifier, U NodeIdentifier](item T) edgesearch[T, U] {
	return edgesearch[T, U]{item: item}
}

func (s edgesearch[T, U]) Filter(e filter.Expression, mods ...operation.QueryModifier) searcher[T] {
	var edge U
	nodeSK := NewEdge(s.item).SK
	edgePrefix := edge.DynamoPrefix()
	builder := expression.NewBuilder().WithKeyCondition(skEquals(nodeSK).And(gsi1SkBeginsWith(edgePrefix)))
	return newSearcher[T](e, builder, indexTypeReverseLookup, mods)
}

func (s edgesearch[T, U]) Get(mods ...operation.QueryModifier) searcher[T] {
	return s.Filter(nil, mods...)
}

type searchIndexType int

const (
	indexTypeCollection searchIndexType = iota
	indexTypeReverseLookup
)

func newSearcher[T any](e filter.Expression, b expression.Builder, indexType searchIndexType, mods []operation.QueryModifier) searcher[T] {
	return searcherFunc[T](
		func(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
			var errs []error = make([]error, 0)
			var condition expression.ConditionBuilder
			var input dynamodb.QueryInput
			if e != nil {
				var err error
				condition, err = filter.Evaluate(e)
				b = b.WithCondition(condition)
				errs = append(errs, err)
			}
			mods = append(mods, operation.WithExpressionBuilderFunc(b, o.BuildExpression))
			for _, mod := range mods {
				errs = append(errs, mod.ModifyQueryInput(ctx, &input))
			}
			input.TableName = &o.TableName
			if indexType == indexTypeCollection {
				input.IndexName = &o.CollectionQueryIndexName
			}
			if indexType == indexTypeReverseLookup {
				input.IndexName = &o.ReverseLookupIndexName
			}
			return &input, errors.Join(errs...)
		},
	)
}

type searcherFunc[T any] func(context.Context, *Options) (*dynamodb.QueryInput, error)

func (f searcherFunc[T]) search(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
	return f(ctx, o)
}

func keyEquals(key, value string) expression.KeyConditionBuilder {
	return expression.Key(key).Equal(expression.Value(value))
}

func keyBeginsWith(key, prefix string) expression.KeyConditionBuilder {
	return expression.Key(key).BeginsWith(prefix)
}

func itemTypeEquals(value string) expression.KeyConditionBuilder {
	return keyEquals(AttributeItemType, value)
}

func pkEquals(value string) expression.KeyConditionBuilder {
	return keyEquals(AttributePartitionKey, value)
}

func skEquals(value string) expression.KeyConditionBuilder {
	return keyEquals(AttributeSortKey, value)
}

func gsi1SkBeginsWith(prefix string) expression.KeyConditionBuilder {
	return keyBeginsWith(AttributeReverseLookupSortKey, prefix)
}

func gsi2SkBetween(start, end time.Time) expression.KeyConditionBuilder {
	return expression.Key(AttributeCollectionQuerySortKey).Between(
		expression.Value(start.UTC().Format(time.RFC3339)),
		expression.Value(end.UTC().Format(time.RFC3339)),
	)
}
