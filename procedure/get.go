package procedure

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Getter implements the dynamodb Get API.
type Getter interface {
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
}

// GetProcedure functions generate dynamodb put input data given some context.
type GetProcedure func(context.Context) (*dynamodb.GetItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (g GetProcedure) Invoke(ctx context.Context) (*dynamodb.GetItemInput, error) {
	return g(ctx)
}

// GetModifier makes modifications to the input before the procedure is executed.
type GetModifier interface {
	// ModifyGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyGetItemInput(context.Context, *dynamodb.GetItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (p GetProcedure) Modify(modifiers ...GetModifier) GetProcedure {
	mapper := func(ctx context.Context, input *dynamodb.GetItemInput, mod GetModifier) error {
		return mod.ModifyGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return modify[dynamodb.GetItemInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (g GetProcedure) Execute(ctx context.Context,
	getter Getter, options ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if input, err := g.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.GetItem(ctx, input, options...)
	}
}

// ModifyBatchWriteItemInput implements the BatchWriteModifier interface.
func (g GetProcedure) ModifyBatchGetItemInput(ctx context.Context, input *dynamodb.BatchGetItemInput) error {
	if get, err := g.Invoke(ctx); err != nil {
		return err
	} else if get.TableName == nil {
		return fmt.Errorf("procedure missing table name; cannot modify batch get item input")
	} else if requests, ok := input.RequestItems[*get.TableName]; !ok {
		input.RequestItems[*get.TableName] = types.KeysAndAttributes{
			Keys: []map[string]types.AttributeValue{get.Key},
		}
		return nil
	} else {
		requests.Keys = append(requests.Keys, get.Key)
		return nil
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
func (g GetProcedure) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
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
