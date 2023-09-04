package xslices

func Chunk[T any](items []T, size int) [][]T {
	chunks := make([][]T, 0)
	if size >= len(items) {
		chunks = append(chunks, items)
		return chunks
	}
	chunkCount := len(items) / size
	offset := 0
	for chunkCount > 0 {
		chunks = append(chunks, items[offset:offset+size])
		offset = offset + size
		chunkCount = chunkCount - 1
	}
	rem := len(items) % size
	if rem != 0 {
		chunks = append(chunks, items[offset:offset+rem])
	}
	return chunks
}
