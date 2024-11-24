package stored

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// Delete is a function that generates a DynamoDB DeleteItemInput given a context.
// It represents a delete operation that can be modified and executed.
type Delete func(context.Context) (*dynamodb.DeleteItemInput, error)

// Invoke is a wrapper around the function invocation for semantic purposes.
func (d Delete) Invoke(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
	return d(ctx)
}

// DeleteModifier defines the interface for types that can modify [dynamodb.DeleteItemInput] request
// before execution.
type DeleteModifier interface {
	// ModifyDeleteItemInput is invoked when this modifier is applied to the provided input.
	ModifyDeleteItemInput(context.Context, *dynamodb.DeleteItemInput) error
}

// DeleteModifierFunc is a function type that implements the [DeleteModifier] interface.
// It provides a convenient way to create DeleteModifiers from simple functions.
type DeleteModifierFunc modifier[dynamodb.DeleteItemInput]

func (d DeleteModifierFunc) ModifyDeleteItemInput(ctx context.Context, input *dynamodb.DeleteItemInput) error {
	return d(ctx, input)
}

// Modify applies the provided modifiers to this Delete operation and returns a new Delete operation.
func (d Delete) Modify(modifiers ...DeleteModifier) Delete {
	mapper := func(ctx context.Context, input *dynamodb.DeleteItemInput, mod DeleteModifier) error {
		return mod.ModifyDeleteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return modify(ctx, d, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the operation, returning the API result.
func (d Delete) Execute(ctx context.Context,
	deleter ezddb.Deleter, options ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if input, err := d.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return deleter.DeleteItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput modifies a [dynamodb.TransactWriteItemsInput] to include this delete operation.
func (d Delete) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	if deletes, err := d.Invoke(ctx); err != nil {
		return err
	} else {
		input.TransactItems = append(input.TransactItems, types.TransactWriteItem{
			Delete: &types.Delete{
				TableName:                 deletes.TableName,
				Key:                       deletes.Key,
				ConditionExpression:       deletes.ConditionExpression,
				ExpressionAttributeNames:  deletes.ExpressionAttributeNames,
				ExpressionAttributeValues: deletes.ExpressionAttributeValues,
			},
		})
		return nil
	}
}

// ModifyBatchWriteItemInput implements the [BatchWriteItemModifier] interface.
// It modifies a [dynamodb.BatchWriteItemInput] to include this delete operation.
func (d Delete) ModifyBatchWriteItemInput(ctx context.Context, input *dynamodb.BatchWriteItemInput) error {
	if input.RequestItems == nil {
		input.RequestItems = make(map[string][]types.WriteRequest)
	}

	if deletes, err := d.Invoke(ctx); err != nil {
		return err
	} else if deletes.TableName == nil {
		return fmt.Errorf("delete Operation has empty table name; cannot creat batch Operation")
	} else if requests, ok := input.RequestItems[*deletes.TableName]; !ok {
		input.RequestItems[*deletes.TableName] = []types.WriteRequest{
			{
				DeleteRequest: &types.DeleteRequest{Key: deletes.Key},
			},
		}
	} else {
		requests = append(requests, types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{Key: deletes.Key},
		})
		input.RequestItems[*deletes.TableName] = requests
	}
	return nil
}
