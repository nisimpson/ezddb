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

// NewBatchWriteOperation creates a new batch write Operation instance.
func newBatchWriteOperation() BatchWriteItem {
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return &dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]types.WriteRequest),
		}, nil
	}
}

type BatchWriteItemCollection []BatchWriteItemModifier

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

func (c BatchWriteItemCollection) Modify(modifiers ...BatchWriteItemModifier) BatchWriteItemCollection {
	return append(c, modifiers...)
}

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
