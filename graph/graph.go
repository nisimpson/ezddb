package graph

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/operation"
)

const (
	DefaultCollectionQueryIndexName = "collection-query-index"
	DefaultReverseLookupIndexName   = "reverse-lookup-index"
)

type Graph[T Node] struct {
	options Options
}

type Options struct {
	ezddb.StartKeyTokenProvider
	ezddb.StartKeyProvider
	TableName                string
	CollectionQueryIndexName string
	ReverseLookupIndexName   string
	BuildExpression          operation.BuildExpressionFunc
	MarshalItem              ezddb.ItemMarshaler
	UnmarshalItem            ezddb.ItemUnmarshaler
	Tick                     Clock
}

type OptionsFunc = func(*Options)

func (o *Options) apply(opts []OptionsFunc) {
	for _, opt := range opts {
		opt(o)
	}
}

func Of[T Node](tableName string, opts ...OptionsFunc) Graph[T] {
	options := Options{
		CollectionQueryIndexName: "collection-query-index",
		ReverseLookupIndexName:   "reverse-lookup-index",
		BuildExpression:          operation.BuildExpression,
		MarshalItem:              attributevalue.MarshalMap,
		UnmarshalItem:            attributevalue.UnmarshalMap,
		Tick:                     time.Now,
	}
	options.apply(opts)
	return Graph[T]{options: options}
}

func (g Graph[T]) PutEdges(data T, opts ...OptionsFunc) operation.BatchWriteCollection {
	g.options.apply(opts)
	node := NewEdge(data)
	refs := node.refs()
	mods := make(operation.BatchWriteCollection, 0, len(refs)+1)
	for _, ref := range refs {
		mods = append(mods, g.put(ref))
	}
	return mods
}

func (g Graph[T]) Put(data T, opts ...OptionsFunc) operation.PutOperation {
	g.options.apply(opts)
	node := NewEdge(data)
	return g.put(node)
}

type puttable interface {
	marshal(ezddb.ItemMarshaler) (ezddb.Item, error)
}

func (g Graph[T]) put(puttable puttable) operation.PutOperation {
	item, err := puttable.marshal(g.options.MarshalItem)
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		if err != nil {
			return nil, fmt.Errorf("put failed: %s", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.options.TableName,
			Item:      item,
		}, nil
	}
}

func (g Graph[T]) Get(id T, opts ...OptionsFunc) operation.GetOperation {
	g.options.apply(opts)
	key := NewEdge(id).Key()
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		return &dynamodb.GetItemInput{
			TableName: &g.options.TableName,
			Key:       key,
		}, nil
	}
}

func (g Graph[T]) Delete(id T, opts ...OptionsFunc) operation.DeleteOperation {
	g.options.apply(opts)
	key := NewEdge(id).Key()
	return func(ctx context.Context) (*dynamodb.DeleteItemInput, error) {
		return &dynamodb.DeleteItemInput{
			TableName: &g.options.TableName,
			Key:       key,
		}, nil
	}
}

type updater[T any] interface {
	update(expression.UpdateBuilder) (ezddb.Item, expression.UpdateBuilder)
}

func (g Graph[T]) Update(updater updater[T], opts ...OptionsFunc) operation.UpdateOperation {
	g.options.apply(opts)
	ts := g.options.Tick().UTC().Format(time.RFC3339)
	update := expression.Set(expression.Name(AttributeUpdatedAt), expression.Value(ts))
	key, update := updater.update(update)
	return func(ctx context.Context) (*dynamodb.UpdateItemInput, error) {
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

func (g Graph[T]) UnmarshalNode(item ezddb.Item, opts ...OptionsFunc) (node T, err error) {
	if item == nil {
		return node, ErrNotFound
	}
	g.options.apply(opts)
	edge := Edge[T]{}
	err = edge.unmarshal(item, g.options.UnmarshalItem)
	if err != nil {
		err = fmt.Errorf("failed to unmarshal edge (%v): %w", item, err)
		return node, err
	}
	err = edge.Data.DynamoUnmarshal(edge.CreatedAt, edge.UpdatedAt)
	return edge.Data, err
}

type refmap[T Node] map[string][]Node

func (m refmap[T]) Apply(node T) {
	for relation, refs := range m {
		for _, ref := range refs {
			node.DynamoUnmarshalRef(relation, ref.DynamoID())
		}
	}
}

func (g Graph[T]) UnmarshalEdges(node T, items []ezddb.Item, opts ...OptionsFunc) (out T, refs refmap[T], err error) {
	g.options.apply(opts)
	refGraph := Graph[nodeRef](g)
	refs = make(refmap[T])
	out = node
	for _, item := range items {
		if ok := itemIsTypeOf(node, item); ok {
			out, err = g.UnmarshalNode(item)
			if err != nil {
				return
			}
			continue
		}
		ref, referr := refGraph.UnmarshalNode(item)
		if referr != nil {
			err = referr
			return
		}
		refs[ref.Relation] = append(refs[ref.Relation], ref)
	}
	return
}

func (g Graph[T]) UnmarshalList(items []ezddb.Item, opts ...OptionsFunc) (out []T, err error) {
	g.options.apply(opts)
	out = make([]T, len(items))
	for i, item := range items {
		node, nerr := g.UnmarshalNode(item)
		if nerr != nil {
			err = nerr
			return
		}
		out[i] = node
	}
	return
}

func (g Graph[T]) UnmarshalPageCursor(lastEvaluatedKey ezddb.Item, opts ...OptionsFunc) (token string, err error) {
	g.options.apply(opts)
	if lastEvaluatedKey == nil {
		return "", nil
	}
	token, err = g.options.StartKeyTokenProvider.GetStartKeyToken(context.TODO(), lastEvaluatedKey)
	if err != nil {
		err = fmt.Errorf("failed to get token: %w", err)
		return
	}
	return
}

type QueryBuilder[T any] interface {
	queryInput(context.Context, *Options) (*dynamodb.QueryInput, error)
}

func (g Graph[T]) Search(criteria QueryBuilder[T], opts ...OptionsFunc) operation.QueryOperation {
	g.options.apply(opts)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		return criteria.queryInput(ctx, &g.options)
	}
}
