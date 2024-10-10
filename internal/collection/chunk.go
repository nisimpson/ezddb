package collection

// Chunk splits a slice into smaller slices of a specified size.
// The last slice may have a smaller length than the specified size.
//
// Example:
//
//	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
//	chunks := Chunk(items, 3)
//	// chunks: [[1 2 3] [4 5 6] [7 8 9] [10]]
func Chunk[T any](s []T, size int) [][]T {
	var chunks [][]T
	for i := 0; i < len(s); i += size {
		end := i + size
		if end > len(s) {
			end = len(s)
		}
		chunks = append(chunks, s[i:end])
	}
	return chunks
}
