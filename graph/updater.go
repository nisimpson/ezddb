package graph

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
)

type nodeupdater[T Node] struct {
	item  T
	attrs []attributeUpdater[T]
}

func (u nodeupdater[T]) update(ub expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder) {
	edge := NewEdge(u.item)
	for _, attr := range u.attrs {
		ub = attr.update(ub)
	}
	return edge.Key(), ub
}

func (nodeupdater[T]) Result(g Graph[T], output *dynamodb.UpdateItemOutput, opts ...OptionsFunc) (node T, err error) {
	edge := Edge[T]{}
	err = g.options.UnmarshalItem(output.Attributes, &edge)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal type: '%s': %w", edge.ItemType, err)
		return node, err
	}
	err = edge.Data.DynamoUnmarshal(edge.CreatedAt, edge.UpdatedAt)
	return edge.Data, err
}

func Updater[T Node](data T, attrs ...attributeUpdater[T]) nodeupdater[T] {
	return nodeupdater[T]{item: data, attrs: attrs}
}

type attributeUpdater[T any] interface {
	update(expression.UpdateBuilder) expression.UpdateBuilder
}

type attributeUpdaterFunc[T any] func(expression.UpdateBuilder) expression.UpdateBuilder

func (f attributeUpdaterFunc[T]) update(b expression.UpdateBuilder) expression.UpdateBuilder {
	return f(b)
}

type attributeUpdate[T any, U any] struct{ key string }

func UpdateFor[T Node, U any](key string) attributeUpdate[T, U] {
	return attributeUpdate[T, U]{key: key}
}

func (a attributeUpdate[T, U]) name() expression.NameBuilder {
	return expression.Name(fmt.Sprintf("data.%s", a.key))
}

func (a attributeUpdate[T, U]) Set(value T) attributeUpdater[T] {
	return attributeUpdaterFunc[T](func(ub expression.UpdateBuilder) expression.UpdateBuilder {
		return ub.Set(a.name(), expression.Value(value))
	})
}

func (a attributeUpdate[T, U]) Increment(value T) attributeUpdater[T] {
	return attributeUpdaterFunc[T](func(ub expression.UpdateBuilder) expression.UpdateBuilder {
		return ub.Add(a.name(), expression.Value(value))
	})
}
