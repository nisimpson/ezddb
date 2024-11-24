package stored

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// Get functions generate dynamodb put input data given some context.
// Get is a function that generates a DynamoDB GetItemInput given a context.
// It represents a get operation that can be modified and executed.
type Get func(context.Context) (*dynamodb.GetItemInput, error)

// Invoke is a wrapper around the function invocation for semantic purposes.
// Invoke executes the Get function with the provided context to generate a GetItemInput.
func (g Get) Invoke(ctx context.Context) (*dynamodb.GetItemInput, error) {
	return g(ctx)
}

// GetModifier makes modifications to the input before the Operation is executed.
// GetModifier defines the interface for types that can modify GetItemInput operations.
type GetModifier interface {
	// ModifyGetItemInput is invoked when this modifier is applied to the provided input.
	ModifyGetItemInput(context.Context, *dynamodb.GetItemInput) error
}

// GetModifierFunc is a function that implements GetModifier.
// GetModifierFunc is a function type that implements the GetModifier interface.
// It provides a convenient way to create GetModifiers from simple functions.
type GetModifierFunc modifier[dynamodb.GetItemInput]

func (g GetModifierFunc) ModifyGetItemInput(ctx context.Context, input *dynamodb.GetItemInput) error {
	return g(ctx, input)
}

// Modify applies the provided modifiers to this Get operation and returns a new Get operation.
func (p Get) Modify(modifiers ...GetModifier) Get {
	mapper := func(ctx context.Context, input *dynamodb.GetItemInput, mod GetModifier) error {
		return mod.ModifyGetItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return modify(ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the operation, returning the API result.
func (g Get) Execute(ctx context.Context,
	getter ezddb.Getter, options ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if input, err := g.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return getter.GetItem(ctx, input, options...)
	}
}

// ModifyBatchGetItemInput implements the [BatchGetItemModifier] interface.
// It modifies a [dynamodb.BatchGetItemInput] to include this get operation.
func (g Get) ModifyBatchGetItemInput(ctx context.Context, input *dynamodb.BatchGetItemInput) error {
	if input.RequestItems == nil {
		input.RequestItems = map[string]types.KeysAndAttributes{}
	}
	if get, err := g.Invoke(ctx); err != nil {
		return err
	} else if get.TableName == nil {
		return fmt.Errorf("operation missing table name; cannot modify batch get item input")
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

// ModifyTransactWriteItemsInput implements the [TransactGetItemsModifier] interface.
// It modifies a [dynamodb.TransactGetItemsInput] to include this get operation.
func (g Get) ModifyTransactGetItemsInput(ctx context.Context, input *dynamodb.TransactGetItemsInput) error {
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
