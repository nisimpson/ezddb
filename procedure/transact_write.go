package procedure

import (
	"context"
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// TransactionWriter implements the dynamodb Transact Write API.
type TransactionWriter interface {
	TransactWriteItems(context.Context,
		*dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// TransactionWrite functions generate dynamodb input data given some context.
type TransactionWrite func(context.Context) (*dynamodb.TransactWriteItemsInput, error)

// NewTransactionWriteProcedure returns a new transaction write procedure instance.
func NewTransactionWriteProcedure() TransactionWrite {
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return &dynamodb.TransactWriteItemsInput{}, nil
	}
}

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (t TransactionWrite) Invoke(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
	return t(ctx)
}

// TransactionWriteModifier makes modifications to the input before the procedure is executed.
type TransactionWriteModifier interface {
	// ModifyTransactWriteItemsInput is invoked when this modifier is applied to the provided input.
	ModifyTransactWriteItemsInput(context.Context, *dynamodb.TransactWriteItemsInput) error
}

// TransactionWriteModifierFunc is a function that implements TransactionWriteModifier.
type TransactionWriteModifierFunc modifier[dynamodb.TransactWriteItemsInput]

func (t TransactionWriteModifierFunc) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	return t(ctx, input)
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (t TransactionWrite) Modify(modifiers ...TransactionWriteModifier) TransactionWrite {
	mapper := func(ctx context.Context, input *dynamodb.TransactWriteItemsInput, mod TransactionWriteModifier) error {
		return mod.ModifyTransactWriteItemsInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.TransactWriteItemsInput, error) {
		return modify[dynamodb.TransactWriteItemsInput](ctx, t, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (t TransactionWrite) Execute(ctx context.Context,
	writer TransactionWriter, options ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	if input, err := t.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return writer.TransactWriteItems(ctx, input, options...)
	}
}

type MultiTransactionWrite []TransactionWrite

func (m MultiTransactionWrite) Invoke(ctx context.Context) ([]*dynamodb.TransactWriteItemsInput, error) {
	inputs := make([]*dynamodb.TransactWriteItemsInput, 0, len(m))
	for _, fn := range m {
		if input, err := fn.Invoke(ctx); err != nil {
			return nil, err
		} else {
			inputs = append(inputs, input)
		}
	}
	return inputs, nil
}

func (m MultiTransactionWrite) Execute(ctx context.Context,
	writer TransactionWriter, options ...func(*dynamodb.Options)) ([]*dynamodb.TransactWriteItemsOutput, error) {
	outputs := make([]*dynamodb.TransactWriteItemsOutput, 0)
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

type MultiTransactionWriteResult struct {
	awaiter *sync.WaitGroup
	outputs []*dynamodb.TransactWriteItemsOutput
	errors  []error
}

func (m *MultiTransactionWriteResult) Wait() ([]*dynamodb.TransactWriteItemsOutput, error) {
	m.awaiter.Wait()
	if len(m.errors) > 0 {
		return nil, errors.Join(m.errors...)
	}
	return m.outputs, nil
}

func (m MultiTransactionWrite) ExecuteAsync(ctx context.Context,
	writer TransactionWriter, options ...func(*dynamodb.Options)) *MultiTransactionWriteResult {

	runner := func(proc TransactionWrite, result *MultiTransactionWriteResult) {
		defer result.awaiter.Done()
		output, err := proc.Execute(ctx, writer, options...)
		if err != nil {
			result.errors = append(result.errors, err)
		} else {
			result.outputs = append(result.outputs, output)
		}
	}

	result := &MultiTransactionWriteResult{}

	for _, proc := range m {
		result.awaiter.Add(1)
		go runner(proc, result)
	}

	return result
}
