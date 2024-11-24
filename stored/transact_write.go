package stored

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/internal/collection"
)

const (
	// The maximum number of transactions in a single [TransactWriteItems] request.
	MaxTransactionGetSize = 100
)

// TransactWriteItems functions generate dynamodb input data given some context.
type TransactWriteItems func(context.Context) (*dynamodb.TransactWriteItemsInput, error)

// newTransactionWriteOperation returns a new transaction write Operation instance.
func newTransactionWriteOperation() TransactWriteItems {
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return &dynamodb.TransactWriteItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for semantic purposes.
func (t TransactWriteItems) Invoke(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
	return t(ctx)
}

// TransactWriteItemsCollection is a collection of TransactionWriteItems modifiers
// that can be chunked into multiple [TransactWriteItems] operations if the total number of
// transctions execeeds [MaxTransactionGetSize].
type TransactWriteItemsCollection []TransactionWriteItemsModifier

// Join creates a new TransactionWriteItemsCollection by chunking the original
// collection into batches of size [MaxTransactionGetSize].
func (c TransactWriteItemsCollection) Join() []TransactWriteItems {
	batches := collection.Chunk(c, MaxTransactionGetSize)
	ops := make([]TransactWriteItems, 0, len(batches))
	for _, batch := range batches {
		op := newTransactionWriteOperation()
		ops = append(ops, op.Modify(batch...))
	}
	return ops
}

// Invoke joins, chunks, and generates multiple [dynamodb.TransactWriteItemsInput] requests.
func (c TransactWriteItemsCollection) Invoke(ctx context.Context) ([]*dynamodb.TransactWriteItemsInput, error) {
	ops := c.Join()
	output := make([]*dynamodb.TransactWriteItemsInput, len(ops))
	for idx, op := range ops {
		var err error
		if output[idx], err = op.Invoke(ctx); err != nil {
			return nil, err
		}
	}
	return output, nil
}

// Execute executes the [TransactWriteItems] operations sequentially,
// merging the output and errors into a single output.
func (c TransactWriteItemsCollection) Execute(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactWriteItemsOutput, error) {
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
	return output, errors.Join(errs...)
}

// ExecuteConcurrently executes the [TransactWriteItems] operations concurrently,
// merging the output and errors into a single output.
func (c TransactWriteItemsCollection) ExecuteConcurrently(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactWriteItemsOutput, error) {
	ops := c.Join()
	wg := &sync.WaitGroup{}
	output := make([]*dynamodb.TransactWriteItemsOutput, len(ops))
	errs := make([]error, len(ops))
	for idx, op := range ops {
		wg.Add(1)
		go func(idx int, op TransactWriteItems) {
			defer wg.Done()
			if out, err := op.Execute(ctx, writer, options...); err != nil {
				errs[idx] = err
			} else {
				output[idx] = out
			}
		}(idx, op)
	}
	wg.Wait()
	return output, errors.Join(errs...)
}

// TransactionWriteItemsModifier makes modifications to the input before the Operation is executed.
type TransactionWriteItemsModifier interface {
	// ModifyTransactWriteItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactWriteItemsInput(context.Context, *dynamodb.TransactWriteItemsInput) error
}

// TransactionWriteItemsModifierFunc is a function that implements TransactionWriteModifier.
type TransactionWriteItemsModifierFunc modifier[dynamodb.TransactWriteItemsInput]

func (t TransactionWriteItemsModifierFunc) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactWriteItems) Modify(modifiers ...TransactionWriteItemsModifier) TransactWriteItems {
	mapper := func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, mod TransactionWriteItemsModifier) error {
		return mod.ModifyTransactWriteItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return modify(ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactWriteItems) Execute(ctx context.Context,
	writer ezddb.TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.TransactWriteItems(ctx, input, options...)
	}
}
