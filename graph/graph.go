package graph

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nisimpson/ezddb"
	"github.com/nisimpson/ezddb/operation"
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	AttributePartitionKey           = "pk"
	AttributeSortKey                = "sk"
	AttributeItemType               = "item_type"
	AttributeCollectionQuerySortKey = "gsi2_sk"
	AttributeReverseLookupSortKey   = "gsi1_sk"
	AttributeUpdatedAt              = "updated_at"
)

type Data interface {
	DynamoHashKey() string
	DynamoSortKey() string
	DynamoID() string
	DynamoPrefix() string
	DynamoItemType() string
	DynamoMarshal(createdAt, updatedAt time.Time) error
	DynamoUnmarshal(createdAt, updatedAt time.Time) error
	DynamoUnmarshalRef(relation string, refID string) error
	DynamoRelationships() map[string][]Data
}

type Item[T Data] struct {
	PK        string    `dynamodbav:"pk"`
	SK        string    `dynamodbav:"sk"`
	ItemType  string    `dynamodbav:"item_type"`
	GSI1SK    string    `dynamodbav:"gsi1_sk"`
	GSI2SK    string    `dynamodbav:"gsi2_sk"`
	CreatedAt time.Time `dynamodbav:"created_at"`
	UpdatedAt time.Time `dynamodbav:"updated_at"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      T         `dynamodbav:"data"`
}

type Clock func() time.Time

type ItemOptions struct {
	HashID                     string
	HashPrefix                 string
	SortID                     string
	SortPrefix                 string
	ItemType                   string
	SupportsReverseLookupIndex bool
	SupportsCollectionIndex    bool
	Tick                       Clock
	ExpirationDate             time.Time
}

func NewItem[T Data](data T, opts ...func(*ItemOptions)) Item[T] {
	options := ItemOptions{
		Tick:                       time.Now,
		HashID:                     data.DynamoID(),
		HashPrefix:                 data.DynamoPrefix(),
		SortID:                     data.DynamoID(),
		SortPrefix:                 data.DynamoPrefix(),
		ItemType:                   data.DynamoItemType(),
		SupportsReverseLookupIndex: true,
		SupportsCollectionIndex:    true,
	}

	for _, opt := range opts {
		opt(&options)
	}

	now := options.Tick().UTC()

	edge := Item[T]{
		PK:        options.HashPrefix + ":" + options.HashID,
		SK:        options.SortPrefix + ":" + options.SortID,
		ItemType:  options.ItemType,
		CreatedAt: now,
		UpdatedAt: now,
		Expires:   options.ExpirationDate,
	}

	if options.SupportsCollectionIndex {
		edge.GSI2SK = edge.CreatedAt.Format(time.RFC3339)
	}

	if options.SupportsReverseLookupIndex {
		edge.GSI1SK = edge.PK
	}

	return edge
}

type ItemRef struct {
	HashID     string
	HashPrefix string
	SortID     string
	SortPrefix string
	Relation   string
}

func (n *ItemRef) DynamoID() string                                   { return n.HashID }
func (n *ItemRef) DynamoPrefix() string                               { return n.HashPrefix }
func (n *ItemRef) DynamoItemType() string                             { return n.Relation }
func (*ItemRef) DynamoMarshal(createdAt, updatedAt time.Time) error   { return nil }
func (*ItemRef) DynamoUnmarshal(createdAt, updatedAt time.Time) error { return nil }
func (*ItemRef) DynamoRelationships() map[string][]Data               { return nil }
func (*ItemRef) DynamoUnmarshalRef(string, string) error              { return nil }

func newItemRef(src, tgt Data, relation string) Item[*ItemRef] {
	return NewItem(&ItemRef{
		HashID:     tgt.DynamoID(),
		SortID:     src.DynamoID(),
		HashPrefix: tgt.DynamoPrefix(),
		SortPrefix: src.DynamoPrefix(),
		Relation:   relation,
	}, func(io *ItemOptions) {
		io.ItemType = fmt.Sprintf("ref:%s", relation)
		io.SupportsCollectionIndex = false
		io.SupportsReverseLookupIndex = true
	})
}

func (e Item[T]) Refs() []Item[*ItemRef] {
	relationships := e.Data.DynamoRelationships()
	if len(relationships) == 0 {
		return nil
	}

	refs := make([]Item[*ItemRef], 0, len(relationships))
	for name, nodes := range relationships {
		for _, node := range nodes {
			refs = append(refs, newItemRef(e.Data, node, name))
		}
	}
	return refs
}

func (e Item[T]) IsNode() bool {
	return e.PK == e.SK
}

func (e Item[T]) TableRowKey() ezddb.Item {
	return newItemKey(e.PK, e.SK)
}

func (e *Item[T]) Marshal(m ezddb.ItemMarshaler) (ezddb.Item, error) {
	err := e.Data.DynamoMarshal(e.CreatedAt, e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return m(e)
}

func (e *Item[T]) Unmarshal(r ezddb.Item, u ezddb.ItemUnmarshaler) error {
	err := u(r, e)
	if err != nil {
		return err
	}
	return e.Data.DynamoUnmarshal(e.CreatedAt, e.UpdatedAt)
}

type ConditionFilterProvider interface {
	FilterCondition(ctx context.Context, base expression.ConditionBuilder) expression.ConditionBuilder
}

type Options struct {
	ezddb.StartKeyTokenProvider
	ezddb.StartKeyProvider
	TableName                string
	CollectionQueryIndexName string
	ReverseLookupIndexName   string
	BuildExpression          ExpressionBuilder
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

type Graph struct {
	Options
}

func New(tableName string, opts ...OptionsFunc) *Graph {
	options := Options{
		CollectionQueryIndexName: "collection-query-index",
		ReverseLookupIndexName:   "reverse-lookup-index",
		BuildExpression:          buildExpression,
		MarshalItem:              attributevalue.MarshalMap,
		UnmarshalItem:            attributevalue.UnmarshalMap,
		Tick:                     time.Now,
	}
	options.apply(opts)
	return &Graph{Options: options}
}

type NodeCollectionIndexQuery struct {
	CollectionItemType string
	PageCursor         string
	Limit              Limit
	CreatedAfterDate   time.Time
	CreatedBeforeDate  time.Time
	Filter             ConditionFilterProvider
	SortAscending      bool
	graph              Graph
}

func (g Graph) NodeCollectionIndexQuery(itemType string, opts ...func(*NodeCollectionIndexQuery)) NodeCollectionIndexQuery {
	q := NodeCollectionIndexQuery{CollectionItemType: itemType, graph: g}
	q.apply(opts)
	return q
}

func (q *NodeCollectionIndexQuery) apply(opts []func(*NodeCollectionIndexQuery)) {
	for _, opt := range opts {
		opt(q)
	}
}

func (q NodeCollectionIndexQuery) Operation(opts ...OptionsFunc) operation.QueryOperation {
	q.graph.apply(opts)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		expressionBuilder := expression.NewBuilder().
			WithKeyCondition(edgesCreatedBetween(q.CollectionItemType, q.CreatedAfterDate, q.CreatedBeforeDate))

		condition := expression.ConditionBuilder{}
		if q.Filter != nil {
			condition = q.Filter.FilterCondition(ctx, condition)
		}

		if condition.IsSet() {
			expressionBuilder = expressionBuilder.WithFilter(condition)
		}

		expr, err := q.graph.BuildExpression(expressionBuilder)
		if err != nil {
			return nil, fmt.Errorf("build expression failed: %w", err)
		}

		startKey, err := ezddb.GetStartKey(ctx, q.graph, q.PageCursor)
		if err != nil {
			return nil, fmt.Errorf("start key failed: %w", err)
		}

		return &dynamodb.QueryInput{
			TableName:                 &q.graph.TableName,
			IndexName:                 &q.graph.CollectionQueryIndexName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         startKey,
			Limit:                     q.Limit.Value(),
			ScanIndexForward:          aws.Bool(q.SortAscending),
		}, nil
	}
}

func UnmarshalCollection[T Data](ctx context.Context, items []ezddb.Item, opts ...OptionsFunc) ([]T, error) {
	options := Options{UnmarshalItem: attributevalue.UnmarshalMap}
	options.apply(opts)

	nodes := make([]T, 0, len(items))
	for _, item := range items {
		node := Item[T]{}
		err := node.Unmarshal(item, options.UnmarshalItem)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal node: %w", err)
		}
		nodes = append(nodes, node.Data)
	}

	return nodes, nil
}

type NodePartitionQuery struct {
	NodeID        string
	NodePrefix    string
	ReverseLookup bool
	Limit         Limit
	PageCursor    string
	Filter        ConditionFilterProvider
	graph         Graph
}

func (g Graph) NodePartitionQuery(hashPrefix, hashID string, opts ...func(*NodePartitionQuery)) NodePartitionQuery {
	q := NodePartitionQuery{NodeID: hashID, NodePrefix: hashPrefix, graph: g}
	q.apply(opts)
	return q
}

func (p *NodePartitionQuery) apply(opts []func(*NodePartitionQuery)) {
	for _, opt := range opts {
		opt(p)
	}
}

func (q NodePartitionQuery) Operation(opts ...OptionsFunc) operation.QueryOperation {
	q.graph.apply(opts)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		expressionBuilder := expression.NewBuilder().
			WithKeyCondition(nodeEdgesWithPrefix(q.NodeID, q.NodePrefix))

		var indexName *string
		if q.ReverseLookup {
			indexName = &q.graph.ReverseLookupIndexName
			expressionBuilder = expressionBuilder.WithKeyCondition(
				reverseNodeEdgesWithPrefix(q.NodeID, q.NodePrefix))
		}

		condition := expression.ConditionBuilder{}
		if q.Filter != nil {
			condition = q.Filter.FilterCondition(ctx, condition)
		}

		if condition.IsSet() {
			expressionBuilder = expressionBuilder.WithFilter(condition)
		}

		expr, err := q.graph.BuildExpression(expressionBuilder)
		if err != nil {
			return nil, fmt.Errorf("build expression failed: %w", err)
		}

		startKey, err := ezddb.GetStartKey(ctx, q.graph, q.PageCursor)
		if err != nil {
			return nil, fmt.Errorf("start key failed: %w", err)
		}

		return &dynamodb.QueryInput{
			TableName:                 &q.graph.TableName,
			IndexName:                 indexName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         startKey,
			Limit:                     q.Limit.Value(),
		}, nil
	}
}

func UnmarshalPartition[T Data](ctx context.Context, data T, items []ezddb.Item, opts ...OptionsFunc) (T, error) {
	options := Options{UnmarshalItem: attributevalue.UnmarshalMap}
	options.apply(opts)

	node := NewItem(data)
	errs := make([]error, 0, len(items))
	for _, item := range items {
		itemType := item[AttributeItemType].(*types.AttributeValueMemberS).Value
		if itemType == node.ItemType {
			errs = append(errs, node.Unmarshal(item, options.UnmarshalItem))
			continue
		}
		ref := Item[*ItemRef]{}
		if err := ref.Unmarshal(item, options.UnmarshalItem); err != nil {
			errs = append(errs, err)
			continue
		}
		relation := ref.Data.Relation
		refID := ref.Data.HashID
		err := node.Data.DynamoUnmarshalRef(relation, refID)
		errs = append(errs, err)
	}
	return node.Data, errors.Join(errs...)
}

type PuttableItem interface {
	Marshal(ezddb.ItemMarshaler) (ezddb.Item, error)
}

func (g Graph) PutItemOperation(item PuttableItem, opts ...OptionsFunc) operation.PutOperation {
	g.apply(opts)
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		item, err := item.Marshal(g.MarshalItem)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal edge: %w", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.TableName,
			Item:      item,
		}, nil
	}
}

type Identifier interface {
	DynamoPrimaryKey() (pk string, sk string)
}

func (g Graph) GetItemOperation(id Identifier, opts ...OptionsFunc) operation.GetOperation {
	g.apply(opts)
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		pk, sk := id.DynamoPrimaryKey()
		return &dynamodb.GetItemInput{
			TableName: &g.TableName,
			Key:       newItemKey(pk, sk),
		}, nil
	}
}

type ExpressionBuilder = func(expression.Builder) (expression.Expression, error)

func buildExpression(builder expression.Builder) (expression.Expression, error) {
	return builder.Build()
}

type Limit int

func (l Limit) Value() *int32 {
	if l > 0 {
		return aws.Int32(int32(l))
	}
	return nil
}

func newItemKey(pk, sk string) ezddb.Item {
	return ezddb.Item{
		AttributePartitionKey: &types.AttributeValueMemberS{Value: pk},
		AttributeSortKey:      &types.AttributeValueMemberS{Value: sk},
	}
}

func edgesWithPrefix(primary, sort, nodeid, edgeprefix string) expression.KeyConditionBuilder {
	condition := expression.Key(primary).Equal(expression.Value(nodeid))
	if edgeprefix != "" {
		condition = condition.And(expression.KeyBeginsWith(
			expression.Key(sort),
			edgeprefix,
		))
	}
	return condition
}

func nodeEdgesWithPrefix(nodeid, edgeprefix string) expression.KeyConditionBuilder {
	return edgesWithPrefix(AttributePartitionKey, AttributeSortKey, nodeid, edgeprefix)
}

func reverseNodeEdgesWithPrefix(nodeid, edgeprefix string) expression.KeyConditionBuilder {
	return edgesWithPrefix(AttributeSortKey, AttributeReverseLookupSortKey, nodeid, edgeprefix)
}

func edgesCreatedBetween(edgetype string, start, end time.Time) expression.KeyConditionBuilder {
	condition := expression.Key(AttributePartitionKey).Equal(expression.Value(edgetype))
	if start.IsZero() || end.IsZero() {
		return condition
	}
	condition = expression.KeyBetween(
		expression.Key(AttributeCollectionQuerySortKey),
		expression.Value(start.Format(time.RFC3339)),
		expression.Value(start.Format(time.RFC3339)),
	)

	return condition
}
