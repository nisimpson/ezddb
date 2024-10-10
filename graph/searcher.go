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

func (s listsearch[T]) WithCreationDateBetween(start, end time.Time) listsearch[T] {
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

func (s listsearch[T]) Build(mods ...operation.QueryModifier) QueryBuilder[T] {
	builder := expression.NewBuilder()
	keyCondition := itemTypeEquals(s.itemType)
	if !(s.createdAfter.IsZero() || s.createdBefore.IsZero()) {
		keyCondition = keyCondition.And(gsi2SkBetween(s.createdAfter, s.createdBefore))
	}
	builder = builder.WithKeyCondition(keyCondition)
	return newQueryBuilder[T](s.filter, builder, indexTypeCollection, mods)
}

type nodesearch[T Node] struct {
	item       T
	edgePrefix string
	filter     filter.Expression
	reverse    bool
}

func EdgesOf[T Node](node T) nodesearch[T] {
	return nodesearch[T]{item: node}
}

func (s nodesearch[T]) WithEdgeTarget(target Node) nodesearch[T] {
	s.edgePrefix = target.DynamoPrefix()
	return s
}

func (s nodesearch[T]) WithReverseLookup() nodesearch[T] {
	s.reverse = true
	return s
}

func (s nodesearch[T]) WithFilter(e filter.Expression, and ...filter.Expression) nodesearch[T] {
	s.filter = e
	for _, item := range and {
		s.filter = filter.And(s.filter, item)
	}
	return s
}

func (s nodesearch[T]) pkCondition() expression.KeyConditionBuilder {
	item := NewEdge(s.item)
	keyCondition := pkEquals(item.PK)
	if s.reverse {
		keyCondition = skEquals(item.SK)
	}
	return keyCondition
}

func (s nodesearch[T]) skCondition() expression.KeyConditionBuilder {
	keyCondition := skBeginsWith(s.edgePrefix)
	if s.reverse {
		keyCondition = gsi1SkBeginsWith(s.edgePrefix)
	}
	return keyCondition
}

func (s nodesearch[T]) Build(mods ...operation.QueryModifier) QueryBuilder[T] {
	builder := expression.NewBuilder()
	key := s.pkCondition()
	if s.edgePrefix != "" {
		key = key.And(s.skCondition())
	}
	builder = builder.WithKeyCondition(key)
	var index searchIndexType
	if s.reverse {
		index = indexTypeReverseLookup
	}
	return newQueryBuilder[T](s.filter, builder, index, mods)
}

type searchIndexType int

const (
	indexTypeNone searchIndexType = iota
	indexTypeCollection
	indexTypeReverseLookup
)

func newQueryBuilder[T any](e filter.Expression, b expression.Builder, indexType searchIndexType, mods []operation.QueryModifier) QueryBuilder[T] {
	return queryBuilderFunc[T](
		func(ctx context.Context, o *Options) (*dynamodb.QueryInput, error) {
			var errs []error = make([]error, 0)
			var input dynamodb.QueryInput
			if e != nil {
				b = b.WithCondition(filter.Condition(e))
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

func itemIsTypeOf[T Node](node T, item ezddb.Item) bool {
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

func pkEquals(value string) expression.KeyConditionBuilder {
	return keyEquals(AttributePartitionKey, value)
}

func skEquals(value string) expression.KeyConditionBuilder {
	return keyEquals(AttributeSortKey, value)
}

func skBeginsWith(prefix string) expression.KeyConditionBuilder {
	return keyBeginsWith(AttributeSortKey, prefix)
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
