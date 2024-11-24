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
	// MaxTransactGetSize is the maximum number of items that can be retrieved in a single TransactGetItems operation.
	// The maximum number of items that can be retrieved in a single call to TransactGetItems.
	// This value is used to chunk the operations into batches of a maximum size.
	MaxTransactGetOperations = 25
)

// TransactGetItems is a function that generates a [dynamodb.TransactGetItemsInput] given a context.
// It represents a transactional get operation that can be modified and executed.
type TransactGetItems func(context.Context) (*dynamodb.TransactGetItemsInput, error)

// newTransactionGetOperation returns a new transaction get Operation instance.
func newTransactionGetOperation() TransactGetItems {
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return &dynamodb.TransactGetItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for semantic purposes.
func (t TransactGetItems) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

// TransactGetItemsCollection is a collection of TransactGetItems operations.
// It provides methods to join and execute the operations.
// TransactGetItemsCollection is a collection of modifiers that can be applied to a transactional get operation.
type TransactGetItemsCollection []TransactGetItemsModifier

// Join joins the TransactGetItems operations in the collection into batches of the maximum size.
func (c TransactGetItemsCollection) Join() []TransactGetItems {
	batches := collection.Chunk(c, MaxTransactGetOperations)
	ops := make([]TransactGetItems, 0, len(batches))
	for _, batch := range batches {
		op := newTransactionGetOperation()
		op = op.Modify(batch...)
		ops = append(ops, op)
	}
	return ops
}

// Invoke joins, chunks, and generates multiple [dynamodb.TransactGetItemsInput] requests.
func (c TransactGetItemsCollection) Invoke(ctx context.Context) ([]*dynamodb.TransactGetItemsInput, error) {
	ops := c.Join()
	inps := make([]*dynamodb.TransactGetItemsInput, 0, len(ops))
	for _, op := range ops {
		inp, err := op.Invoke(ctx)
		if err != nil {
			return nil, err
		}
		inps = append(inps, inp)
	}
	return inps, nil
}

// Execute executes the [TransactGetItems] operations sequentially,
// merging the output and errors into a single output.
func (c TransactGetItemsCollection) Execute(ctx context.Context,
	getter ezddb.TransactionGetter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactGetItemsOutput, error) {
	ops := c.Join()
	outs := make([]*dynamodb.TransactGetItemsOutput, 0, len(ops))
	errs := make([]error, 0, len(ops))
	for _, op := range ops {
		out, err := op.Execute(ctx, getter, options...)
		if err != nil {
			errs = append(errs, err)
		} else {
			outs = append(outs, out)
		}
	}
	return outs, errors.Join(errs...)
}

// ExecuteConcurrently executes the collection of operations concurrently, awaiting and returning
// the resulting outputs.
func (c TransactGetItemsCollection) ExecuteConcurrently(ctx context.Context,
	getter ezddb.TransactionGetter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactGetItemsOutput, error) {
	ops := c.Join()
	outs := make([]*dynamodb.TransactGetItemsOutput, len(ops))
	errs := make([]error, len(ops))
	wg := &sync.WaitGroup{}

	// execute operations
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op TransactGetItems) {
			defer wg.Done()
			out, err := op.Execute(ctx, getter, options...)
			if err != nil {
				errs[i] = err
			} else {
				outs[i] = out
			}
		}(i, op)
	}

	return outs, errors.Join(errs...)
}

// TransactGetItemsModifier makes modifications to the input before the operation is executed.
type TransactGetItemsModifier interface {
	// ModifyTransactGetItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

// TransactGetModifierFunc is a function type that implements the TransactGetItemsModifier interface.
// It provides a convenient way to create TransactGetItemsModifiers from simple functions.
type TransactGetModifierFunc modifier[dynamodb.TransactGetItemsInput]

func (t TransactGetModifierFunc) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactGetItems) Modify(modifiers ...TransactGetItemsModifier) TransactGetItems {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactGetItemsModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify(ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactGetItems) Execute(ctx context.Context,
	getter ezddb.TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}
