package procedure

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
)

// TransactionGet functions generate dynamodb input data given some context.
type TransactionGet func(context.Context) (*dynamodb.TransactGetItemsInput, error)

// NewTransactionGetProcedure returns a new transaction get procedure instance.
func NewTransactionGetProcedure() TransactionGet {
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return &dynamodb.TransactGetItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionGet) Invoke(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
	return t(ctx)
}

// TransactionGetModifier makes modifications to the input before the procedure is executed.
type TransactionGetModifier interface {
	// ModifyTransactGetItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactGetItemsInput(context.Context, *dynamodb.TransactGetItemsInput) error
}

// TransactionGetModifierFunc is a function that implements TransactionGetModifier.
type TransactionGetModifierFunc modifier[dynamodb.TransactGetItemsInput]

func (t TransactionGetModifierFunc) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (t TransactionGet) Modify(modifiers ...TransactionGetModifier) TransactionGet {
	mapper := func(ctx context.Context, input *dynamodb.TransactGetItemsInput, mod TransactionGetModifier) error {
		return mod.ModifyTransactGetItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactGetItemsInput, error) {
		return modify[dynamodb.TransactGetItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (t TransactionGet) Execute(ctx context.Context,
	getter ezddb.TransactionGetter, options ...func(*dynamodb.Options)) (*dynamodb.TransactGetItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.TransactGetItems(ctx, input, options...)
	}
}

type MultiTransactionGet []TransactionGet

func (m MultiTransactionGet) Invoke(ctx context.Context) ([]*dynamodb.TransactGetItemsInput, error) {
	inputs := make([]*dynamodb.TransactGetItemsInput, 0, len(m))
	for _, fn := range m {
		if input, err := fn.Invoke(ctx); err != nil {
			return nil, err
		} else {
			inputs = append(inputs, input)
		}
	}
	return inputs, nil
}

func (m MultiTransactionGet) Execute(ctx context.Context,
	writer ezddb.TransactionGetter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactGetItemsOutput, error) {
	outputs := make([]*dynamodb.TransactGetItemsOutput, 0)
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

type MultiTransactionGetResult struct {
	awaiter *sync.WaitGroup
	outputs []*dynamodb.TransactGetItemsOutput
	errors  []error
}

func (m *MultiTransactionGetResult) Wait() ([]*dynamodb.TransactGetItemsOutput, error) {
	m.awaiter.Wait()
	if len(m.errors) > 0 {
		return nil, errors.Join(m.errors...)
	}
	return m.outputs, nil
}

func (m MultiTransactionGet) ExecuteAsync(ctx context.Context,
	writer ezddb.TransactionGetter, options ...func(*dynamodb.Options)) *MultiTransactionGetResult {

	runner := func(proc TransactionGet, result *MultiTransactionGetResult) {
		defer result.awaiter.Done()
		output, err := proc.Execute(ctx, writer, options...)
		if err != nil {
			result.errors = append(result.errors, err)
		} else {
			result.outputs = append(result.outputs, output)
		}
	}

	result := &MultiTransactionGetResult{}

	for _, proc := range m {
		result.awaiter.Add(1)
		go runner(proc, result)
	}

	return result
}
