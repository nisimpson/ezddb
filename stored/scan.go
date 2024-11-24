package stored

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
)

// Scan is a function that generates a [dynamodb.ScanInput] given a context.
// It represents a scan operation that can be modified and executed.
type Scan func(context.Context) (*dynamodb.ScanInput, error)

// Invoke generates a [dynamodb.ScanInput] given the provided context.
func (q Scan) Invoke(ctx context.Context) (*dynamodb.ScanInput, error) {
	return q(ctx)
}

// ScanModifier makes modifications to the scan input before the operation is executed.
type ScanModifier interface {
	// ModifyScanInput is invoked when this modifier is applied to the provided input.
	ModifyScanInput(context.Context, *dynamodb.ScanInput) error
}

// Modify adds modifying functions to the operation, transforming the input
// before it is executed.
func (p Scan) Modify(modifiers ...ScanModifier) Scan {
	mapper := func(ctx context.Context, input *dynamodb.ScanInput, mod ScanModifier) error {
		return mod.ModifyScanInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.ScanInput, error) {
		return modify(ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the operation, returning the API result.
func (p Scan) Execute(ctx context.Context,
	Scanner ezddb.Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return Scanner.Scan(ctx, input, options...)
	}
}

// PageScanCallback is invoked each time the stored Operation is executed. The result
// of the execution is provided for further processing; to halt further page calls,
// return false.
type PageScanCallback = func(context.Context, *dynamodb.ScanOutput) bool

// WithPagination creates a [ScanExecutor] that exhastively retrieves items from the
// database using the initial stored. Use the callback to access data from each
// response.
func (p Scan) WithPagination(callback PageScanCallback) ScanExecutor {
	return func(ctx context.Context, scanner ezddb.Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
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

// ScanExecutor is a function type that executes a scan operation with pagination support.
// It handles the execution of the scan and processing of paginated results.
type ScanExecutor func(context.Context, ezddb.Scanner, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)

// Execute invokes the scan items API using the provided scanner and options.
func (s ScanExecutor) Execute(ctx context.Context,
	scanner ezddb.Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return s(ctx, scanner, options...)
}
