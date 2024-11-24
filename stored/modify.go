package stored

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nisimpson/ezddb"
)

// withLimit is a modifier type that sets the limit for query and scan operations.
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
// WithLimit creates a new modifier that sets the limit for query and scan operations.
func WithLimit(value int) withLimit {
	return withLimit(value)
}

// withLastToken is a modifier that sets the start token for pagination in query and scan operations.
type withLastToken struct {
	provider ezddb.StartKeyProvider
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
// stored.
// WithLastToken creates a new modifier that sets the start token for pagination.
func WithLastToken(token string, provider ezddb.StartKeyProvider) withLastToken {
	return withLastToken{token: token, provider: provider}
}

type BuildExpressionFunc = func(expression.Builder) (expression.Expression, error)

// BuildExpression creates a DynamoDB expression from the provided builder.
// It's a utility function used by expression modifiers.
func BuildExpression(builder expression.Builder) (expression.Expression, error) {
	return builder.Build()
}

// withExpressionBuilder is a modifier that applies DynamoDB expressions to various operations.
type withExpressionBuilder struct {
	build   BuildExpressionFunc
	builder expression.Builder
}

func WithExpressionBuilderFunc(builder expression.Builder, f BuildExpressionFunc) withExpressionBuilder {
	return withExpressionBuilder{build: f, builder: builder}
}

func WithExpressionBuilder(builder expression.Builder) withExpressionBuilder {
	return WithExpressionBuilderFunc(builder, BuildExpression)
}

func (w withExpressionBuilder) ModifyQueryInput(ctx context.Context, input *dynamodb.QueryInput) error {
	expr, err := w.build(w.builder)
	if err != nil {
		return err
	}
	input.FilterExpression = expr.Filter()
	input.KeyConditionExpression = expr.KeyCondition()
	input.ProjectionExpression = expr.Projection()
	input.ExpressionAttributeNames = expr.Names()
	input.ExpressionAttributeValues = expr.Values()
	return nil
}

func (w withExpressionBuilder) ModifyScanInput(ctx context.Context, input *dynamodb.QueryInput) error {
	expr, err := w.build(w.builder)
	if err != nil {
		return err
	}
	input.FilterExpression = expr.Filter()
	input.KeyConditionExpression = expr.KeyCondition()
	input.ProjectionExpression = expr.Projection()
	input.ExpressionAttributeNames = expr.Names()
	input.ExpressionAttributeValues = expr.Values()
	return nil
}

func (w withExpressionBuilder) ModifyUpdateItemInput(ctx context.Context, input *dynamodb.UpdateItemInput) error {
	expr, err := w.build(w.builder)
	if err != nil {
		return err
	}
	input.UpdateExpression = expr.Update()
	input.ExpressionAttributeNames = expr.Names()
	input.ExpressionAttributeValues = expr.Values()
	return nil
}

// invoker represents a type that can generate input for DynamoDB operations.
type invoker[T any] interface {
	Invoke(context.Context) (*T, error)
}

// modifier is a generic function type that modifies DynamoDB input types.
type modifier[T any] func(context.Context, *T) error

// modifierGroup is a collection of modifiers that can be joined together.
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
		i := item
		group = append(group, func(ctx context.Context, t *T) error {
			return mapper(ctx, t, i)
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
