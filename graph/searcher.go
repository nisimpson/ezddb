package graph

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/filter"
	"github.com/nisimpson/ezddb/operation"
)

type listsearch[T Node] struct {
	itemType      string
	createdBefore time.Time
	createdAfter  time.Time
	filter        filter.Expression
}

func ListOf[T Node](template T) listsearch[T] {
	return listsearch[T]{itemType: template.DynamoItemType()}
}

func (s listsearch[T]) WithDateFilter(start, end time.Time) listsearch[T] {
	s.createdAfter = start
	s.createdBefore = end
	return s
}

func (s listsearch[T]) WithFilter(e filter.Expression, and ...filter.Expression) listsearch[T] {
	s.filter = e
	for _, item := range and {
		s.filter = filter.And(s.filter, item)
	}
	return s
}

func (s listsearch[T]) Criteria(mods ...operation.QueryModifier) QueryBuilder[T] {
	builder := expression.NewBuilder()
	keyCondition := itemTypeEquals(s.itemType)
	if !(s.createdAfter.IsZero() || s.createdBefore.IsZero()) {
		keyCondition = keyCondition.And(gsi2SkBetween(s.createdAfter, s.createdBefore))
	}
	builder = builder.WithKeyCondition(keyCondition)
	return newQueryBuilder[T](s.filter, builder, indexTypeCollection, mods)
}

func (listsearch[T]) Scan(g Graph[T], output *dynamodb.QueryOutput, opts ...OptionsFunc) (nodes []T, cursor string, err error) {
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

type nodesearch[T Node] struct {
	item       T
	edgePrefix string
	filter     filter.Expression
}

func NodeOf[T Node](node T) nodesearch[T] {
	return nodesearch[T]{item: node}
}

func EdgesOf[T Node, U Node](source T, target U) nodesearch[T] {
	prefix := target.DynamoPrefix()
	return nodesearch[T]{item: source, edgePrefix: prefix}
}

func (s nodesearch[T]) WithFilter(e filter.Expression, and ...filter.Expression) nodesearch[T] {
	s.filter = e
	for _, item := range and {
		s.filter = filter.And(s.filter, item)
	}
	return s
}

func (s nodesearch[T]) Criteria(e filter.Expression, mods ...operation.QueryModifier) QueryBuilder[T] {
	item := NewEdge(s.item)
	builder := expression.NewBuilder()
	keyCond := skEquals(item.SK)
	if s.edgePrefix != "" {
		keyCond = keyCond.And(gsi1SkBeginsWith(s.edgePrefix))
	}
	builder = builder.WithKeyCondition(keyCond)
	return newQueryBuilder[T](s.filter, builder, indexTypeReverseLookup, mods)
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

type searchIndexType int

const (
	indexTypeCollection searchIndexType = iota
	indexTypeReverseLookup
)

func newQueryBuilder[T any](e filter.Expression, b expression.Builder, indexType searchIndexType, mods []operation.QueryModifier) QueryBuilder[T] {
	return queryBuilderFunc[T](
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

type queryBuilderFunc[T any] func(context.Context, *Options) (*dynamodb.QueryInput, error)

func (f queryBuilderFunc[T]) queryInput(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
	return f(ctx, o)
}

func isTypeOf[T Node](node T, item ezddb.Item) bool {
	if attr, ok := item[AttributeItemType].(*types.AttributeValueMemberS); !ok {
		return false
	} else {
		return attr.Value == node.DynamoItemType()
	}
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
