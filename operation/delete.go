package operation

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// Delete functions generate dynamodb put input data given some context.
type Delete func(context.Context) (*dynamodb.DeleteItemInput, error)

// Invoke is a wrapper around the function invocation for semantic purposes.
func (d Delete) Invoke(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
	return d(ctx)
}

// DeleteModifier makes modifications to the input before the Operation is executed.
type DeleteModifier interface {
	// ModifyDeleteItemInput is invoked when this modifier is applied to the provided input.
	ModifyDeleteItemInput(context.Context, *dynamodb.DeleteItemInput) error
}

// DeleteModifierFunc is a function that implements DeleteModifier.
type DeleteModifierFunc modifier[dynamodb.DeleteItemInput]

func (d DeleteModifierFunc) ModifyDeleteItemInput(ctx context.Context, input *dynamodb.DeleteItemInput) error {
	return d(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (d Delete) Modify(modifiers ...DeleteModifier) Delete {
	mapper := func(ctx context.Context, input *dynamodb.DeleteItemInput, mod DeleteModifier) error {
		return mod.ModifyDeleteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return modify(ctx, d, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (d Delete) Execute(ctx context.Context,
	deleter ezddb.Deleter, options ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if input, err := d.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return deleter.DeleteItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
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

// ModifyBatchWriteItemInput implements the BatchWriteModifier interface.
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
