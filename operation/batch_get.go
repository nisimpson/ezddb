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
	MaxBatchReadSize = 100
)

// BatchGetItem functions generate dynamodb put input data given some context.
type BatchGetItem func(context.Context) (*dynamodb.BatchGetItemInput, error)

// newBatchGetOperation creates a new batch get Operation instance.
func newBatchGetOperation() BatchGetItem {
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]types.KeysAndAttributes),
		}, nil
	}
}

type BatchGetItemCollection []BatchGetItemModifier

func (c BatchGetItemCollection) Join() []BatchGetItem {
	batches := collection.Chunk(c, MaxBatchReadSize)
	ops := make([]BatchGetItem, 0, len(batches))
	for _, batch := range batches {
		opt := newBatchGetOperation()
		for _, mod := range batch {
			opt = opt.Modify(mod)
		}
		ops = append(ops, opt)
	}
	return ops
}

func (c BatchGetItemCollection) Modify(modifiers ...BatchGetItemModifier) BatchGetItemCollection {
	return append(c, modifiers...)
}

func (BatchGetItemCollection) readOutput(v ezddb.ItemVisitor, outs []*dynamodb.BatchGetItemOutput) error {
	for _, o := range outs {
		if o == nil {
			continue
		}
		for _, items := range o.Responses {
			for _, item := range items {
				if err := ezddb.VisitItem(item, v); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c BatchGetItemCollection) Invoke(ctx context.Context) ([]*dynamodb.BatchGetItemInput, error) {
	ops := c.Join()
	inputs := make([]*dynamodb.BatchGetItemInput, 0, len(ops))
	for _, op := range ops {
		input, err := op.Invoke(ctx)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, input)
	}
	return inputs, nil
}

func (c BatchGetItemCollection) Execute(ctx context.Context,
	getter ezddb.BatchGetter, visitor ezddb.ItemVisitor, options ...func(*dynamodb.Options)) error {
	ops := c.Join()
	outs := make([]*dynamodb.BatchGetItemOutput, 0, len(ops))
	errs := make([]error, 0, len(ops))
	for _, op := range ops {
		out, err := op.Execute(ctx, getter, options...)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		outs = append(outs, out)
	}
	errs = append(errs, c.readOutput(visitor, outs))
	return errors.Join(errs...)
}

func (c BatchGetItemCollection) ExecuteConcurrently(ctx context.Context,
	getter ezddb.BatchGetter, visitor ezddb.ItemVisitor, options ...func(*dynamodb.Options)) error {
	ops := c.Join()
	outs := make([]*dynamodb.BatchGetItemOutput, len(ops))
	errs := make([]error, len(ops))
	wg := &sync.WaitGroup{}
	for i, op := range ops {
		wg.Add(1)
		go func(i int, op BatchGetItem) {
			defer wg.Done()
			out, err := op.Execute(ctx, getter, options...)
			if err != nil {
				errs[i] = err
				return
			}
			outs[i] = out
		}(i, op)
	}
	wg.Wait()
	errs = append(errs, c.readOutput(visitor, outs))
	return errors.Join(errs...)
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchGetItem) Invoke(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
	return g(ctx)
}

// BatchGetItemModifier makes modifications to the input before the Operation is executed.
type BatchGetItemModifier interface {
	// ModifyBatchGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchGetItemInput(context.Context, *dynamodb.BatchGetItemInput) error
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (b BatchGetItem) Modify(modifiers ...BatchGetItemModifier) BatchGetItem {
	mapper := func(ctx context.Context, input *dynamodb.BatchGetItemInput, mod BatchGetItemModifier) error {
		return mod.ModifyBatchGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return modify(ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (b BatchGetItem) Execute(ctx context.Context,
	getter ezddb.BatchGetter, options ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.BatchGetItem(ctx, input, options...)
	}
}
