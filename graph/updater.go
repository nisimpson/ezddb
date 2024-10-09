package graph

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/nisimpson/ezddb"
)

type updaterFunc[T any] func(expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder)

func (f updaterFunc[T]) update(e expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder) {
	return f(e)
}

func Updater[T Data](data T, attrs ...attributeUpdater[T]) updater[T] {
	return updaterFunc[T](func(ub expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder) {
		pk, sk := data.DynamoHashKey(), data.DynamoSortKey()
		for _, attr := range attrs {
			ub = attr.update(ub)
		}
		return newItemKey(pk, sk), ub
	})
}

type attributeUpdater[T any] interface {
	update(expression.UpdateBuilder) expression.UpdateBuilder
}

type attributeUpdaterFunc[T any] func(expression.UpdateBuilder) expression.UpdateBuilder

func (f attributeUpdaterFunc[T]) update(b expression.UpdateBuilder) expression.UpdateBuilder {
	return f(b)
}

type attributeUpdate[T any, U any] struct{ key string }

func UpdateFor[T Data, U any](key string) attributeUpdate[T, U] {
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
