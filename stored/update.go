package stored

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
)

// UpdateItem is a function that generates a [dynamodb.UpdateItemInput] given a context.
// It represents an update operation that can be modified and executed.
type UpdateItem func(context.Context) (*dynamodb.UpdateItemInput, error)

// Invoke executes the UpdateItem function with the provided context to generate an UpdateItemInput.
func (u UpdateItem) Invoke(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
	return u(ctx)
}

// UpdateItemModifier makes modifications to the input before the ojperation is executed.
type UpdateItemModifier interface {
	// ModifyPutItemInput is invoked when this modifier is applied to the provided input.
	ModifyUpdateItemInput(context.Context, *dynamodb.UpdateItemInput) error
}

// UpdateItemModifierFunc is a function type that implements the [UpdateItemModifier] interface.
// It provides a convenient way to create UpdateItemModifiers from simple functions.
type UpdateItemModifierFunc modifier[dynamodb.UpdateItemInput]

func (u UpdateItemModifierFunc) ModifyUpdateItemInput(ctx context.Context, input *dynamodb.UpdateItemInput) error {
	return u(ctx, input)
}

// Modify adds modifying functions to the operation, transforming the input
// before it is executed.
func (u UpdateItem) Modify(modifiers ...UpdateItemModifier) UpdateItem {
	mapper := func(ctx context.Context, input *dynamodb.UpdateItemInput, mod UpdateItemModifier) error {
		return mod.ModifyUpdateItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		return modify(ctx, u, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the operation, returning the API result.
func (u UpdateItem) Execute(ctx context.Context,
	Updater ezddb.Updater, options ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if input, err := u.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return Updater.UpdateItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput implements the [TransactWriteItemsModifier] interface.
// It modifies a [dynamodb.TransactWriteItemsInput] to include this update operation.
func (u UpdateItem) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
	if update, err := u.Invoke(ctx); err != nil {
		return err
	} else {
		input.TransactItems = append(input.TransactItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName:                 update.TableName,
				Key:                       update.Key,
				UpdateExpression:          update.UpdateExpression,
				ExpressionAttributeNames:  update.ExpressionAttributeNames,
				ExpressionAttributeValues: update.ExpressionAttributeValues,
				ConditionExpression:       update.ConditionExpression,
			},
		})
		return nil
	}
}
