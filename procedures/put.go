package procedures

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Putter interface {
	PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

type PutProcedure func(context.Context) (*dynamodb.PutItemInput, error)

func (p PutProcedure) Invoke(ctx context.Context) (*dynamodb.PutItemInput, error) {
	return p(ctx)
}

type PutModifier interface {
	ModifyPutItemInput(context.Context, *dynamodb.PutItemInput) error
}

func (p PutProcedure) Modify(modifiers ...PutModifier) PutProcedure {
	mapper := func(ctx context.Context, input *dynamodb.PutItemInput, mod PutModifier) error {
		return mod.ModifyPutItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		return modify[dynamodb.PutItemInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

func (p PutProcedure) Execute(ctx context.Context, putter Putter, options ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return putter.PutItem(ctx, input, options...)
	}
}

func (p PutProcedure) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	if puts, err := p.Invoke(ctx); err != nil {
		return err
	} else {
		input.TransactItems = append(input.TransactItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName:                 puts.TableName,
				Item:                      puts.Item,
				ConditionExpression:       puts.ConditionExpression,
				ExpressionAttributeNames:  puts.ExpressionAttributeNames,
				ExpressionAttributeValues: puts.ExpressionAttributeValues,
			},
		})
		return nil
	}
}

func (p PutProcedure) ModifyBatchWriteItemInput(ctx context.Context, input *dynamodb.BatchWriteItemInput) error {
	if put, err := p.Invoke(ctx); err != nil {
		return err
	} else if put.TableName == nil {
		return fmt.Errorf("put procedure has empty table name; cannot creat batch procedure")
	} else if requests, ok := input.RequestItems[*put.TableName]; !ok {
		input.RequestItems[*put.TableName] = []types.WriteRequest{
			{
				PutRequest: &types.PutRequest{
					Item: put.Item,
				},
			},
		}
	} else {
		requests = append(requests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: put.Item},
		})
		input.RequestItems[*put.TableName] = requests
	}
	return nil
}
