package procedures

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Deleter interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type DeleteProcedure func(context.Context) (*dynamodb.DeleteItemInput, error)

func (d DeleteProcedure) Invoke(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
	return d(ctx)
}

type DeleteModifier interface {
	ModifyDeleteItemInput(context.Context, *dynamodb.DeleteItemInput) error
}

func (u DeleteProcedure) Modify(modifiers ...DeleteModifier) DeleteProcedure {
	mapper := func(ctx context.Context, input *dynamodb.DeleteItemInput, mod DeleteModifier) error {
		return mod.ModifyDeleteItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return modify[dynamodb.DeleteItemInput](ctx, u, newModiferGroup(modifiers, mapper).Join())
	}
}

func (d DeleteProcedure) Execute(ctx context.Context,
	deleter Deleter, options ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	if input, err := d.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return deleter.DeleteItem(ctx, input, options...)
	}
}

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
