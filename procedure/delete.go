package procedure

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)


// Deleter implements the dynamodb Delete API.
type Deleter interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// DeleteProcedure functions generate dynamodb put input data given some context.
type DeleteProcedure func(context.Context) (*dynamodb.DeleteItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (d DeleteProcedure) Invoke(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
	return d(ctx)
}

// DeleteModifier makes modifications to the input before the procedure is executed.
type DeleteModifier interface {
	// ModifyDeleteItemInput is invoked when this modifier is applied to the provided input.
	ModifyDeleteItemInput(context.Context, *dynamodb.DeleteItemInput) error
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (d DeleteProcedure) Modify(modifiers ...DeleteModifier) DeleteProcedure {
	mapper := func(ctx context.Context, input *dynamodb.DeleteItemInput, mod DeleteModifier) error {
		return mod.ModifyDeleteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return modify[dynamodb.DeleteItemInput](ctx, d, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (d DeleteProcedure) Execute(ctx context.Context,
	deleter Deleter, options ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if input, err := d.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return deleter.DeleteItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
func (d DeleteProcedure) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
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
func (d DeleteProcedure) ModifyBatchWriteItemInput(ctx context.Context, input *dynamodb.BatchWriteItemInput) error {
	if deletes, err := d.Invoke(ctx); err != nil {
		return err
	} else if deletes.TableName == nil {
		return fmt.Errorf("delete procedure has empty table name; cannot creat batch procedure")
	} else if requests, ok := input.RequestItems[*deletes.TableName]; !ok {
		input.RequestItems[*deletes.TableName] = []types.WriteRequest{
			{
				DeleteRequest: &types.DeleteRequest{Key: deletes.Key},
			},
		}
	} else {
		requests = append(requests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: deletes.Key},
		})
		input.RequestItems[*deletes.TableName] = requests
	}
	return nil
}
