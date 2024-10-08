package graph

import "github.com/nisimpson/ezddb/operation"

type searcherFunc[T any] func() []operation.QueryModifier

func (f searcherFunc[T]) search() []operation.QueryModifier {
	return f()
}
