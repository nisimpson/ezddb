package operation

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// PutOperation functions generate dynamodb put input data given some context.
type PutOperation func(context.Context) (*dynamodb.PutItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (p PutOperation) Invoke(ctx context.Context) (*dynamodb.PutItemInput, error) {
	return p(ctx)
}

// PutModifier makes modifications to the input before the Operation is executed.
type PutModifier interface {
	// ModifyPutItemInput is invoked when this modifier is applied to the provided input.
	ModifyPutItemInput(context.Context, *dynamodb.PutItemInput) error
}

// PutModifierFunc is a function that implements PutModifier.
type PutModifierFunc modifier[dynamodb.PutItemInput]

func (p PutModifierFunc) ModifyPutItemInput(ctx context.Context, input *dynamodb.PutItemInput) error {
	return p(ctx, input)
}

// Modify adds modifying functions to the Operation, transforming the input
// before it is executed.
func (p PutOperation) Modify(modifiers ...PutModifier) PutOperation {
	mapper := func(ctx context.Context, input *dynamodb.PutItemInput, mod PutModifier) error {
		return mod.ModifyPutItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		return modify[dynamodb.PutItemInput](ctx, p, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the Operation, returning the API result.
func (p PutOperation) Execute(ctx context.Context, putter ezddb.Putter, options ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if input, err := p.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return putter.PutItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
func (p PutOperation) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
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

// ModifyBatchWriteItemInput implements the BatchWriteModifier interface.
func (p PutOperation) ModifyBatchWriteItemInput(ctx context.Context, input *dynamodb.BatchWriteItemInput) error {
	if input.RequestItems == nil {
		input.RequestItems = make(map[string][]types.WriteRequest)
	}

	if put, err := p.Invoke(ctx); err != nil {
		return err
	} else if put.TableName == nil {
		return fmt.Errorf("put Operation has empty table name; cannot creat batch Operation")
	} else if requests, ok := input.RequestItems[*put.TableName]; !ok {
		input.RequestItems[*put.TableName] = []types.WriteRequest{
			{
				PutRequest: &types.PutRequest{Item: put.Item},
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