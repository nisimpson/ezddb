package collection

type Mapper[T any, U any] func(T) U

func Map[T any, U any](items []T, mapper Mapper[T,U]) []U {
	mapped := make([]U, 0, len(items))
	for _, item := range items {
		mapped = append(mapped, mapper(item))
	}
	return mapped
}
