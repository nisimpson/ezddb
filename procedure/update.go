package procedure

import (
	"context"

	"github.com/nisimpson/ezddb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Update functions generate dynamodb input data given some context.
type Update func(context.Context) (*dynamodb.UpdateItemInput, error)

// Invoke is a wrapper around the function invocation for stylistic purposes.
func (u Update) Invoke(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
	return u(ctx)
}

// UpdateModifier makes modifications to the input before the procedure is executed.
type UpdateModifier interface {
	// ModifyPutItemInput is invoked when this modifier is applied to the provided input.
	ModifyUpdateItemInput(context.Context, *dynamodb.UpdateItemInput) error
}

// UpdateModifierFunc is a function that implements UpdateModifier.
type UpdateModifierFunc modifier[dynamodb.UpdateItemInput]

func (u UpdateModifierFunc) ModifyUpdateItemInput(ctx context.Context, input *dynamodb.UpdateItemInput) error {
	return u(ctx, input)
}

// Modify adds modifying functions to the procedure, transforming the input
// before it is executed.
func (u Update) Modify(modifiers ...UpdateModifier) Update {
	mapper := func(ctx context.Context, input *dynamodb.UpdateItemInput, mod UpdateModifier) error {
		return mod.ModifyUpdateItemInput(ctx, input)
	}
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		return modify[dynamodb.UpdateItemInput](ctx, u, newModiferGroup(modifiers, mapper).Join())
	}
}

// Execute executes the procedure, returning the API result.
func (u Update) Execute(ctx context.Context,
	Updater ezddb.Updater, options ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if input, err := u.Invoke(ctx); err != nil {
		return nil, err
	} else {
		return Updater.UpdateItem(ctx, input, options...)
	}
}

// ModifyTransactWriteItemsInput implements the TransactWriteModifier interface.
func (u Update) ModifyTransactWriteItemsInput(ctx context.Context, input *dynamodb.TransactWriteItemsInput) error {
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
