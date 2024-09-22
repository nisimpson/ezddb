package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Scanner implements the dynamodb Scan API.
type Scanner interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// ScanProcedure functions generate dynamodb scan input data given some context.
type ScanProcedure func(context.Context) (*dynamodb.ScanInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (q ScanProcedure) Invoke(ctx context.Context) (*dynamodb.ScanInput, error) {
	return q(ctx)
}

// ScanModifier makes modifications to the scan input before the procedure is executed.
type ScanModifier interface {
	// ModifyScanInput is invoked when this modifier is applied to the provided input.
	ModifyScanInput(context.Context, *dynamodb.ScanInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (p ScanProcedure) Modify(modifiers ...ScanModifier) ScanProcedure {
	mapper := func(ctx context.Context, input *dynamodb.ScanInput, mod ScanModifier) error {
		return mod.ModifyScanInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.ScanInput, error) {
		return modify[dynamodb.ScanInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (p ScanProcedure) Execute(ctx context.Context,
	Scanner Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return Scanner.Scan(ctx, input, options...)
	}
}

// PageScanCallback is invoked each time the stored procedure is executed. The result
// of the execution is provided for further processing; to halt further page calls,
// return false.
type PageScanCallback = func(context.Context, *dynamodb.ScanOutput) bool

// WithPagination creates a new procedure that exhastively retrieves items from the
// database using the initial ezddb. Use the callback to access data from each
// response.
func (p ScanProcedure) WithPagination(callback PageScanCallback) ScanExecutor {
	return func(ctx context.Context, scanner Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
		input, err := p.Invoke(ctx)
		if err != nil {
			return nil, err
		}
		for {
			if output, err := scanner.Scan(ctx, input, options...); err != nil {
				return nil, err
			} else if ok := callback(ctx, output); !ok {
				return output, nil
			} else if output.LastEvaluatedKey == nil {
				return output, nil
			} else {
				input.ExclusiveStartKey = output.LastEvaluatedKey
			}
		}
	}
}

// ScanExecutor functions execute the dynamoDB scan items API.
type ScanExecutor func(context.Context, Scanner, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)

// Execute invokes the scan items API using the provided scanner and options.
func (s ScanExecutor) Execute(ctx context.Context,
	scanner Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return s(ctx, scanner, options...)
}
