package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/operation"
)

type Graph2[T Data] struct {
	options Options
}

func (g Graph2[T]) Put(data T, opts ...OptionsFunc) operation.PutOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		node := NewItem(data)
		item, err := node.Marshal(g.options.MarshalItem)
		if err != nil {
			return nil, fmt.Errorf("put failed: %s", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.options.TableName,
			Item:      item,
		}, nil
	}
}

func (g Graph2[T]) Get(id Identifier, opts ...OptionsFunc) operation.GetOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		pk, sk := id.DynamoPrimaryKey()
		return &dynamodb.GetItemInput{
			TableName: &g.options.TableName,
			Key:       newItemKey(pk, sk),
		}, nil
	}
}

func (g Graph2[T]) Delete(id Identifier, opts ...OptionsFunc) operation.DeleteOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		pk, sk := id.DynamoPrimaryKey()
		return &dynamodb.DeleteItemInput{
			TableName: &g.options.TableName,
			Key:       newItemKey(pk, sk),
		}, nil
	}
}

type updater[T any] interface {
	update(expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder)
}

func (g Graph2[T]) Update(updater updater[T], opts ...OptionsFunc) operation.UpdateOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
		ts := g.options.Tick().UTC().Format(time.RFC3339)
		update := expression.Set(expression.Name(AttributeUpdatedAt), expression.Value(ts))
		key, update := updater.update(update)
		expr, err := g.options.BuildExpression(expression.NewBuilder().WithUpdate(update))
		if err != nil {
			return nil, fmt.Errorf("update failed: build expression: %w", err)
		}

		return &dynamodb.UpdateItemInput{
			TableName:                 &g.options.TableName,
			Key:                       key,
			UpdateExpression:          expr.Update(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ReturnValues:              types.ReturnValueAllNew,
		}, nil
	}
}

type searcher[T any] interface {
	search(context.Context, *Options) (*dynamodb.QueryInput, error)
}

func (g Graph2[T]) Search(searcher searcher[T], opts ...OptionsFunc) operation.QueryOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return searcher.search(ctx, &g.options)
	}
}
