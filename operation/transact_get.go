package operation

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/internal/collection"
)

const (
	MaxTransactionGetOperations = 25
)

// TransactionGetter implements the dynamodb Transact Get API.
type TransactionGetter interface {
	TransactGetItems(context.Context, *dynamodb.TransactGetItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error)
}

// TransactionGetOperation functions generate dynamodb input data given some context.
type TransactionGetOperation func(context.Context) (*dynamodb.TransactGetItemsInput, error)

// NewTransactionGetOperation returns a new transaction get Operation instance.
func NewTransactionGetOperation() TransactionGetOperation {
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return &dynamodb.TransactGetItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionGetOperation) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

type TransactionGetCollection []TransactionGetModifier

func (c TransactionGetCollection) Join() []TransactionGetOperation {
	batches := collection.Chunk(c, MaxTransactionGetOperations)
	ops := make([]TransactionGetOperation, 0, len(batches))
	for _, batch := range batches {
		op := NewTransactionGetOperation()
		op = op.Modify(batch...)
		ops = append(ops, op)
	}
	return ops
}

func (TransactionGetCollection) visit(outs []*dynamodb.TransactGetItemsOutput, v ezddb.ItemVisitor) error {
	errs := make([]error, 0)
	for _, out := range outs {
		if out == nil {
			continue
		}
		for _, response := range out.Responses {
			errs = append(errs, ezddb.VisitItem(response.Item, v))
		}
	}
	return errors.Join(errs...)
}

func (c TransactionGetCollection) Invoke(ctx context.Context) ([]*dynamodb.TransactGetItemsInput, error) {
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

func (c TransactionGetCollection) Execute(ctx context.Context,
	getter TransactionGetter, visitor ezddb.ItemVisitor, options ...func(*dynamodb.Options)) error {
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
	errs = append(errs, c.visit(outs, visitor))
	return errors.Join(errs...)
}

func (c TransactionGetCollection) ExecuteConcurrently(ctx context.Context,
	getter TransactionGetter, visitor ezddb.ItemVisitor, options ...func(*dynamodb.Options)) error {
	ops := c.Join()
	outs := make([]*dynamodb.TransactGetItemsOutput, len(ops))
	errs := make([]error, len(ops))
	wg := &sync.WaitGroup{}

	// execute operations
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op TransactionGetOperation) {
			defer wg.Done()
			out, err := op.Execute(ctx, getter, options...)
			if err != nil {
				errs[i] = err
			} else {
				outs[i] = out
			}
		}(i, op)
	}

	errs = append(errs, c.visit(outs, visitor))
	return errors.Join(errs...)
}

// TransactionGetModifier makes modifications to the input before the Operation is executed.
type TransactionGetModifier interface {
	// ModifyTransactGetItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

// TransactionGetModifierFunc is a function that implements TransactionGetModifier.
type TransactionGetModifierFunc modifier[dynamodb.TransactGetItemsInput]

func (t TransactionGetModifierFunc) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (t TransactionGetOperation) Modify(modifiers ...TransactionGetModifier) TransactionGetOperation {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactionGetModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify(ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (t TransactionGetOperation) Execute(ctx context.Context,
	getter TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}
