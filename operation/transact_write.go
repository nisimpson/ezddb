package operation

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
	MaxTransactionGetSize = 100
)

// TransactionWriteOperation functions generate dynamodb input data given some context.
type TransactionWriteOperation func(context.Context) (*dynamodb.TransactWriteItemsInput, error)

// NewTransactionWriteOperation returns a new transaction write Operation instance.
func NewTransactionWriteOperation() TransactionWriteOperation {
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return &dynamodb.TransactWriteItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionWriteOperation) Invoke(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
	return t(ctx)
}

type TransactionWriteCollection []TransactionWriteModifier

func (c TransactionWriteCollection) Join() []TransactionWriteOperation {
	batches := collection.Chunk(c, MaxTransactionGetSize)
	ops := make([]TransactionWriteOperation, 0, len(batches))
	for _, batch := range batches {
		op := NewTransactionWriteOperation()
		op.Modify(batch...)
		ops = append(ops, op)
	}
	return ops
}

func (c TransactionWriteCollection) Execute(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	ops := c.Join()
	output := make([]*dynamodb.TransactWriteItemsOutput, len(ops))
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

func (c TransactionWriteCollection) ExecuteConcurrently(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	ops := c.Join()
	wg := &sync.WaitGroup{}
	output := make([]*dynamodb.TransactWriteItemsOutput, len(ops))
	errs := make([]error, len(ops))
	for idx, op := range ops {
		wg.Add(1)
		go func(idx int, op TransactionWriteOperation) {
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

func (TransactionWriteCollection) mergeOutput(items []*dynamodb.TransactWriteItemsOutput) *dynamodb.TransactWriteItemsOutput {
	output := &dynamodb.TransactWriteItemsOutput{
		ItemCollectionMetrics: make(map[string][]types.ItemCollectionMetrics),
	}
	for _, item := range items {
		output.ConsumedCapacity = append(output.ConsumedCapacity, item.ConsumedCapacity...)
		for k, v := range item.ItemCollectionMetrics {
			output.ItemCollectionMetrics[k] = append(output.ItemCollectionMetrics[k], v...)
		}
	}
	return output
}

// TransactionWriteModifier makes modifications to the input before the Operation is executed.
type TransactionWriteModifier interface {
	// ModifyTransactWriteItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactWriteItemsInput(context.Context, *dynamodb.TransactWriteItemsInput) error
}

// TransactionWriteModifierFunc is a function that implements TransactionWriteModifier.
type TransactionWriteModifierFunc modifier[dynamodb.TransactWriteItemsInput]

func (t TransactionWriteModifierFunc) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactionWriteOperation) Modify(modifiers ...TransactionWriteModifier) TransactionWriteOperation {
	mapper := func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, mod TransactionWriteModifier) error {
		return mod.ModifyTransactWriteItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return modify(ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactionWriteOperation) Execute(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.TransactWriteItems(ctx, input, options...)
	}
}
