package stored

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/internal/collection"
)

const (
	MaxBatchWriteSize = 25
)

// BatchWriteItem functions generate dynamodb put input data given some context.
type BatchWriteItem func(context.Context) (*dynamodb.BatchWriteItemInput, error)

// newBatchWriteOperation creates a new BatchWriteItem operation with default settings.
func newBatchWriteOperation() BatchWriteItem {
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return &dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]types.WriteRequest),
		}, nil
	}
}

// BatchWriteItemCollection is a collection of modifiers that can be applied to a batch write operation.
// It provides methods for joining, modifying, and executing batch write operations.
type BatchWriteItemCollection []BatchWriteItemModifier

// Join combines all modifiers in the collection into a slice of BatchWriteItem operations.
func (c BatchWriteItemCollection) Join() []BatchWriteItem {
	batches := collection.Chunk(c, MaxBatchWriteSize)
	ops := make([]BatchWriteItem, 0, len(batches))
	for _, batch := range batches {
		op := newBatchWriteOperation()
		op = op.Modify(batch...)
		ops = append(ops, op)
	}
	return ops
}

// Execute runs the operation on each [BatchWriteItem] sequentially,
// merging the output.
func (c BatchWriteItemCollection) Execute(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	ops := c.Join()
	output := make([]*dynamodb.BatchWriteItemOutput, len(ops))
	errs := make([]error, len(ops))
	for idx, op := range ops {
		if out, err := op.Execute(ctx, writer, options...); err != nil {
			errs[idx] = err
		} else {
			output[idx] = out
		}
	}
	return c.mergeOutput(output), errors.Join(errs...)
}

// Modify applies additional modifiers to the collection and returns the modified collection.
func (c BatchWriteItemCollection) Modify(modifiers ...BatchWriteItemModifier) BatchWriteItemCollection {
	return append(c, modifiers...)
}

// ExecuteConcurrently executes all batch write operations in the collection concurrently.
func (c BatchWriteItemCollection) ExecuteConcurrently(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	ops := c.Join()
	wg := &sync.WaitGroup{}
	output := make([]*dynamodb.BatchWriteItemOutput, len(ops))
	errs := make([]error, len(ops))
	for idx, op := range ops {
		wg.Add(1)
		go func(idx int, op BatchWriteItem) {
			defer wg.Done()
			if out, err := op.Execute(ctx, writer, options...); err != nil {
				errs[idx] = err
			} else {
				output[idx] = out
			}
		}(idx, op)
	}
	wg.Wait()
	return c.mergeOutput(output), errors.Join(errs...)
}

// mergeOutput combines multiple BatchWriteItemOutput instances into a single output.
func (BatchWriteItemCollection) mergeOutput(items []*dynamodb.BatchWriteItemOutput) *dynamodb.BatchWriteItemOutput {
	output := &dynamodb.BatchWriteItemOutput{
		ItemCollectionMetrics: make(map[string][]types.ItemCollectionMetrics),
		UnprocessedItems:      make(map[string][]types.WriteRequest),
		ConsumedCapacity:      make([]types.ConsumedCapacity, 0),
	}
	for _, item := range items {
		if item == nil {
			continue
		}
		output.ConsumedCapacity = append(output.ConsumedCapacity, item.ConsumedCapacity...)
		for k, v := range item.ItemCollectionMetrics {
			output.ItemCollectionMetrics[k] = append(output.ItemCollectionMetrics[k], v...)
		}
		for k, v := range item.UnprocessedItems {
			output.UnprocessedItems[k] = append(output.UnprocessedItems[k], v...)
		}
	}
	return output
}

// Invoke is a wrapper around the function invocation for semantic purposes.
func (g BatchWriteItem) Invoke(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
	return g(ctx)
}

// BatchWriteItemModifier makes modifications to the input before the Operation is executed.
// BatchWriteItemModifier defines the interface for types that can modify BatchWriteItem operations.
type BatchWriteItemModifier interface {
	// ModifyBatchWriteItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchWriteItemInput(context.Context, *dynamodb.BatchWriteItemInput) error
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (b BatchWriteItem) Modify(modifiers ...BatchWriteItemModifier) BatchWriteItem {
	mapper := func(ctx context.Context, input *dynamodb.BatchWriteItemInput, mod BatchWriteItemModifier) error {
		return mod.ModifyBatchWriteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return modify(ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (b BatchWriteItem) Execute(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.BatchWriteItem(ctx, input, options...)
	}
}
