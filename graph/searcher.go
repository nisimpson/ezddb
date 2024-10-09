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

type listsearch[T Node] struct{ item T }

func ListOf[T Node](item T) listsearch[T] {
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

func (listsearch[T]) Result(g Graph[T], output *dynamodb.QueryOutput, opts ...OptionsFunc) (nodes []T, cursor string, err error) {
	nodes = make([]T, 0, len(output.Items))
	for _, item := range output.Items {
		node, nodeerr := g.Result(item, opts...)
		if nodeerr != nil {
			err = nodeerr
			return
		}
		nodes = append(nodes, node)
	}
	provider := g.options.StartKeyTokenProvider
	cursor, err = provider.GetStartKeyToken(context.TODO(), output.LastEvaluatedKey)
	return
}

type nodesearch[T Node] struct{ item T }

func NodeOf[T Node](item T) nodesearch[T] {
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

func (s nodesearch[T]) Result(g Graph[T], output *dynamodb.QueryOutput, opts ...OptionsFunc) (node T, cursor string, err error) {
	g.options.apply(opts)
	node = s.item
	refGraph := Graph[nodeRef](g)
	for _, item := range output.Items {
		if ok := isTypeOf(node, item); ok {
			node, err = g.Result(item)
			if err != nil {
				return
			}
			continue
		}
		ref, referr := refGraph.Result(item)
		if referr != nil {
			err = referr
			return
		}
		referr = node.DynamoUnmarshalRef(ref.Relation, ref.DynamoID())
		if referr != nil {
			err = referr
			return
		}
	}
	provider := g.options.StartKeyTokenProvider
	cursor, err = provider.GetStartKeyToken(context.TODO(), output.LastEvaluatedKey)
	return
}

type edgesearch[T Node, U Node] struct{ item T }

func EdgesOf[T Node, U Node](item T) edgesearch[T, U] {
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

func (e edgesearch[T, U]) Result(g Graph[T], output *dynamodb.QueryOutput, opts ...OptionsFunc) (node T, cursor string, err error) {
	node = e.item
	refGraph := Graph[nodeRef](g)
	refGraph.options.apply(opts)
	for _, item := range output.Items {
		ref, referr := refGraph.Result(item)
		if referr != nil {
			err = referr
			return
		}
		if referr := node.DynamoUnmarshalRef(ref.Relation, ref.DynamoID()); err != nil {
			err = referr
			return
		}
	}
	provider := g.options.StartKeyTokenProvider
	cursor, err = provider.GetStartKeyToken(context.TODO(), output.LastEvaluatedKey)
	return
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
