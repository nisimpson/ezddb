package procedure

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// BatchGet functions generate dynamodb put input data given some context.
type BatchGet func(context.Context) (*dynamodb.BatchGetItemInput, error)

// NewBatchGetProcedure creates a new batch get procedure instance.
func NewBatchGetProcedure() BatchGet {
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return &dynamodb.BatchGetItemInput{
			RequestItems: make(map[string]types.KeysAndAttributes),
		}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g BatchGet) Invoke(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
	return g(ctx)
}

// BatchGetModifier makes modifications to the input before the procedure is executed.
type BatchGetModifier interface {
	// ModifyBatchGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyBatchGetItemInput(context.Context, *dynamodb.BatchGetItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (b BatchGet) Modify(modifiers ...BatchGetModifier) BatchGet {
	mapper := func(ctx context.Context, input *dynamodb.BatchGetItemInput, mod BatchGetModifier) error {
		return mod.ModifyBatchGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.BatchGetItemInput, error) {
		return modify[dynamodb.BatchGetItemInput](ctx, b, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (b BatchGet) Execute(ctx context.Context,
	getter ezddb.BatchGetter, options ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if input, err := b.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.BatchGetItem(ctx, input, options...)
	}
}

type MultiBatchGet []BatchGet

func (m MultiBatchGet) Invoke(ctx context.Context) ([]*dynamodb.BatchGetItemInput, error) {
	inputs := make([]*dynamodb.BatchGetItemInput, 0, len(m))
	for _, fn := range m {
		if input, err := fn.Invoke(ctx); err != nil {
			return nil, err
		} else {
			inputs = append(inputs, input)
		}
	}
	return inputs, nil
}

func (m MultiBatchGet) Execute(ctx context.Context,
	writer ezddb.BatchGetter, options ...func(*dynamodb.Options)) ([]*dynamodb.BatchGetItemOutput, error) {
	outputs := make([]*dynamodb.BatchGetItemOutput, 0)
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

type MultiBatchGetResult struct {
	awaiter *sync.WaitGroup
	outputs []*dynamodb.BatchGetItemOutput
	errors  []error
}

func (m *MultiBatchGetResult) Wait() ([]*dynamodb.BatchGetItemOutput, error) {
	m.awaiter.Wait()
	if len(m.errors) > 0 {
		return nil, errors.Join(m.errors...)
	}
	return m.outputs, nil
}

func (m MultiBatchGet) ExecuteAsync(ctx context.Context,
	writer ezddb.BatchGetter, options ...func(*dynamodb.Options)) *MultiBatchGetResult {

	runner := func(proc BatchGet, result *MultiBatchGetResult) {
		defer result.awaiter.Done()
		output, err := proc.Execute(ctx, writer, options...)
		if err != nil {
			result.errors = append(result.errors, err)
		} else {
			result.outputs = append(result.outputs, output)
		}
	}

	result := &MultiBatchGetResult{}

	for _, proc := range m {
		result.awaiter.Add(1)
		go runner(proc, result)
	}

	return result
}
