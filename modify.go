package ezddb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type withLimit int32

// ModifyQueryInput implements QueryModifier.
func (w withLimit) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	input.Limit = w.value()
	return nil
}

// ModifyScanInput implements ScanModifier.
func (w withLimit) ModifyScanInput(ctx context.Context, input *dynamodb.ScanInput) error {
	input.Limit = w.value()
	return nil
}

func (w withLimit) value() *int32 {
	value := int32(w)
	if value <= 0 {
		return nil
	}
	return aws.Int32(value)
}

// WithLimit provides an input modifier for adjusting the number of items returned on a scan or query.
// Non-positive values are ignored.
func WithLimit(value int) withLimit {
	return withLimit(value)
}

type withLastToken struct {
	provider StartKeyProvider
	token    string
}

// ModifyQueryInput implements QueryModifier.
func (w withLastToken) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	if key, err := w.provider.GetStartKey(ctx, w.token); err != nil {
		return err
	} else {
		input.ExclusiveStartKey = key
		return nil
	}
}

// ModifyScanInput implements ScanModifier.
func (w withLastToken) ModifyScanInput(ctx context.Context, input *dynamodb.ScanInput) error {
	if key, err := w.provider.GetStartKey(ctx, w.token); err != nil {
		return err
	} else {
		input.ExclusiveStartKey = key
		return nil
	}
}

// WithLastToken creates a new input modifier for adding pagination tokens to scan or query
// ezddb.
func WithLastToken(token string, provider StartKeyProvider) withLastToken {
	return withLastToken{token: token, provider: provider}
}

type invoker[T any] interface {
	Invoke(context.Context) (*T, error)
}

type modifier[T any] func(context.Context, *T) error

type modifierGroup[T any] []modifier[T]

func (m modifierGroup[T]) Join() modifier[T] {
	return func(ctx context.Context, t *T) error {
		for _, mod := range m {
			if err := mod(ctx, t); err != nil {
				return err
			}
		}
		return nil
	}
}

func newModiferGroup[T any, U any](items []U, mapper func(context.Context, *T, U) error) modifierGroup[T] {
	group := make(modifierGroup[T], 0, len(items))
	for _, item := range items {
		group = append(group, func(ctx context.Context, t *T) error {
			return mapper(ctx, t, item)
		})
	}
	return group
}

func modify[T any](ctx context.Context, invoker invoker[T], modifier modifier[T]) (*T, error) {
	if input, err := invoker.Invoke(ctx); err != nil {
		return nil, err
	} else if err := modifier(ctx, input); err != nil {
		return nil, err
	} else {
		return input, nil
	}
}
