package procedures

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Scanner interface {
	Scan(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

type ScanProcedure func(context.Context) (*dynamodb.ScanInput, error)

func (q ScanProcedure) Invoke(ctx context.Context) (*dynamodb.ScanInput, error) {
	return q(ctx)
}

type ScanModifier interface {
	ModifyScanInput(context.Context, *dynamodb.ScanInput) error
}

func (p ScanProcedure) Modify(modifiers ...ScanModifier) ScanProcedure {
	mapper := func(ctx context.Context, input *dynamodb.ScanInput, mod ScanModifier) error {
		return mod.ModifyScanInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.ScanInput, error) {
		return modify[dynamodb.ScanInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

func (p ScanProcedure) Execute(ctx context.Context,
	Scanner Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return Scanner.Scan(ctx, input, options...)
	}
}

type PageScanCallback = func(context.Context, *dynamodb.ScanOutput) bool

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

type ScanExecutor func(context.Context, Scanner, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)

func (s ScanExecutor) Execute(ctx context.Context,
	scanner Scanner, options ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return s(ctx, scanner, options...)
}
