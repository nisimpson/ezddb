package procedure

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// BatchWrite functions generate dynamodb put input data given some context.
type BatchWrite func(context.Context) (*dynamodb.BatchWriteItemInput, error)

// NewBatchWriteProcedure creates a new batch write procedure instance.
func NewBatchWriteProcedure() BatchWrite {
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return &dynamodb.BatchWriteItemInput{
			RequestItems: make(map[string][]types.WriteRequest),
		}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchWrite) Invoke(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
	return g(ctx)
}

// BatchWriteModifier makes modifications to the input before the procedure is executed.
type BatchWriteModifier interface {
	// ModifyBatchWriteItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchWriteItemInput(context.Context, *dynamodb.BatchWriteItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (b BatchWrite) Modify(modifiers ...BatchWriteModifier) BatchWrite {
	mapper := func(ctx context.Context, input *dynamodb.BatchWriteItemInput, mod BatchWriteModifier) error {
		return mod.ModifyBatchWriteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchWriteItemInput, error) {
		return modify[dynamodb.BatchWriteItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (b BatchWrite) Execute(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) (*dynamodb.BatchWriteItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.BatchWriteItem(ctx, input, options...)
	}
}

type MultiBatchWrite []BatchWrite

func (m MultiBatchWrite) Invoke(ctx context.Context) ([]*dynamodb.BatchWriteItemInput, error) {
	inputs := make([]*dynamodb.BatchWriteItemInput, 0, len(m))
	for _, fn := range m {
		if input, err := fn.Invoke(ctx); err != nil {
			return nil, err
		} else {
			inputs = append(inputs, input)
		}
	}
	return inputs, nil
}

func (m MultiBatchWrite) Execute(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) ([]*dynamodb.BatchWriteItemOutput, error) {
	outputs := make([]*dynamodb.BatchWriteItemOutput, 0)
	errs := make([]error, 0)

	for _, proc := range m {
		out, err := proc.Execute(ctx, writer, options...)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		outputs = append(outputs, out)
	}

	if len(errs) > 0 {
		return outputs, errors.Join(errs...)
	}

	return outputs, nil
}

type MultiBatchWriteResult struct {
	awaiter *sync.WaitGroup
	outputs []*dynamodb.BatchWriteItemOutput
	errors  []error
}

func (m *MultiBatchWriteResult) Wait() ([]*dynamodb.BatchWriteItemOutput, error) {
	m.awaiter.Wait()
	if len(m.errors) > 0 {
		return nil, errors.Join(m.errors...)
	}
	return m.outputs, nil
}

func (m MultiBatchWrite) ExecuteAsync(ctx context.Context,
	writer ezddb.BatchWriter, options ...func(*dynamodb.Options)) *MultiBatchWriteResult {

	runner := func(proc BatchWrite, result *MultiBatchWriteResult) {
		defer result.awaiter.Done()
		output, err := proc.Execute(ctx, writer, options...)
		if err != nil {
			result.errors = append(result.errors, err)
		} else {
			result.outputs = append(result.outputs, output)
		}
	}

	result := &MultiBatchWriteResult{}

	for _, proc := range m {
		result.awaiter.Add(1)
		go runner(proc, result)
	}

	return result
}
