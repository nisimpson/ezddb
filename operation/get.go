package operation

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// GetOperation functions generate dynamodb put input data given some context.
type GetOperation func(context.Context) (*dynamodb.GetItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g GetOperation) Invoke(ctx context.Context) (*dynamodb.GetItemInput, error) {
	return g(ctx)
}

// GetModifier makes modifications to the input before the Operation is executed.
type GetModifier interface {
	// ModifyGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyGetItemInput(context.Context, *dynamodb.GetItemInput) error
}

// GetModifierFunc is a function that implements GetModifier.
type GetModifierFunc modifier[dynamodb.GetItemInput]

func (g GetModifierFunc) ModifyGetItemInput(ctx context.Context, input *dynamodb.GetItemInput) error {
	return g(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (p GetOperation) Modify(modifiers ...GetModifier) GetOperation {
	mapper := func(ctx context.Context, input *dynamodb.GetItemInput, mod GetModifier) error {
		return mod.ModifyGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return modify[dynamodb.GetItemInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (g GetOperation) Execute(ctx context.Context,
	getter ezddb.Getter, options ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if input, err := g.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.GetItem(ctx, input, options...)
	}
}

// ModifyBatchWriteItemInput implements the BatchWriteModifier interface.
func (g GetOperation) ModifyBatchGetItemInput(ctx context.Context, input *dynamodb.BatchGetItemInput) error {
	if input.RequestItems == nil {
		input.RequestItems = map[string]types.KeysAndAttributes{}
	}
	if get, err := g.Invoke(ctx); err != nil {
		return err
	} else if get.TableName == nil {
		return fmt.Errorf("Operation missing table name; cannot modify batch get item input")
	} else if requests, ok := input.RequestItems[*get.TableName]; !ok {
		input.RequestItems[*get.TableName] = types.KeysAndAttributes{
			Keys: []map[string]types.AttributeValue{get.Key},
		}
		return nil
	} else {
		requests.Keys = append(requests.Keys, get.Key)
		input.RequestItems[*get.TableName] = requests
		return nil
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
func (g GetOperation) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
	if get, err := g.Invoke(ctx); err != nil {
		return err
	} else {
		input.TransactItems = append(input.TransactItems, types.TransactGetItem{
			Get: &types.Get{
				TableName:                get.TableName,
				Key:                      get.Key,
				ProjectionExpression:     get.ProjectionExpression,
				ExpressionAttributeNames: get.ExpressionAttributeNames,
			},
		})
		return nil
	}
}
