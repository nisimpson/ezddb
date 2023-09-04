package xslices

func MapSlice[T any, U any](items []T, mapper func(T) U) []U {
	results := make([]U, 0, len(items))
	for _, item := range items {
		results = append(results, mapper(item))
	}
	return results
}
