package procedure

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

type limitModifier int32

// Limit provides an input modifier for adjusting the number of items returned on a scan or query.
// Non-positive values are ignored.
func Limit(value int) limitModifier {
	return limitModifier(value)
}

// ModifyQueryInput implements QueryModifier.
func (l limitModifier) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	input.Limit = l.value()
	return nil
}

// ModifyScanInput implements ScanModifier.
func (l limitModifier) ModifyScanInput(ctx context.Context, input *dynamodb.ScanInput) error {
	input.Limit = l.value()
	return nil
}

func (l limitModifier) value() *int32 {
	value := int32(l)
	if value <= 0 {
		return nil
	}
	return aws.Int32(value)
}

type startTokenModifier struct {
	provider StartKeyProvider
	token    string
}

// ModifyQueryInput implements QueryModifier.
func (s startTokenModifier) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	if key, err := s.provider.GetStartKey(ctx, s.token); err != nil {
		return err
	} else {
		input.ExclusiveStartKey = key
		return nil
	}
}

// ModifyScanInput implements ScanModifier.
func (s startTokenModifier) ModifyScanInput(ctx context.Context, input *dynamodb.ScanInput) error {
	if key, err := s.provider.GetStartKey(ctx, s.token); err != nil {
		return err
	} else {
		input.ExclusiveStartKey = key
		return nil
	}
}

// StartToken creates a new input modifier for adding pagination tokens to scan or query
// procedure.
func StartToken(token string, provider StartKeyProvider) startTokenModifier {
	return startTokenModifier{token: token, provider: provider}
}

type startKeyModifier map[string]types.AttributeValue

// StartKey creates a new input modifier for adding pagination tokens to scan or query
// procedure.
func StartKey(item ezddb.Item) startKeyModifier {
	return startKeyModifier(item)
}

func (s startKeyModifier) ModifyScanInput(ctx context.Context, input *dynamodb.ScanInput) error {
	input.ExclusiveStartKey = s
	return nil
}

func (s startKeyModifier) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	input.ExclusiveStartKey = s
	return nil
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
