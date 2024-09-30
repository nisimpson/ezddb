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
)

var (
	ErrNotFound = errors.New("not found")
)

const (
	AttributePartitionKey           = "pk"
	AttributeSortKey                = "sk"
	AttributeRelation               = "relation"
	AttributeCollectionQuerySortKey = "gsi2_sk"
	AttributeReverseLookupSortKey   = "gsi1_sk"
)

type Node interface {
	DynamoGraphNodeID() string
	DynamoGraphNodePrefix() string
	DynamoGraphRelation() string
	DynamoGraphMarshal(createdAt, updatedAt time.Time) error
	DynamoGraphUnmarshal(createdAt, updatedAt time.Time) error
	DynamoGraphRelationships() map[string][]Node
}

type EdgeIdentifier interface {
	EdgeID() (pk string, sk string)
}

type Item[T Node] struct {
	PK        string    `dynamodbav:"pk"`
	SK        string    `dynamodbav:"sk"`
	Relation  string    `dynamodbav:"relation"`
	GSI1SK    string    `dynamodbav:"gsi1_sk"`
	GSI2SK    string    `dynamodbav:"gsi2_sk"`
	CreatedAt time.Time `dynamodbav:"created_at"`
	UpdatedAt time.Time `dynamodbav:"updated_at"`
	Expires   time.Time `dynamodbav:"expires,unixtime"`
	Data      T         `dynamodbav:"data"`
}

type Clock func() time.Time

type EdgeOptions struct {
	HashID                     string
	HashPrefix                 string
	SortID                     string
	SortPrefix                 string
	Relation                   string
	SupportsReverseLookupIndex bool
	SupportsCollectionIndex    bool
	Tick                       Clock
	ExpirationDate             time.Time
}

func NewItem[T Node](node T, opts ...func(*EdgeOptions)) Item[T] {
	options := EdgeOptions{
		Tick:                       time.Now,
		HashID:                     node.DynamoGraphNodeID(),
		HashPrefix:                 node.DynamoGraphNodePrefix(),
		SortID:                     node.DynamoGraphNodeID(),
		SortPrefix:                 node.DynamoGraphNodePrefix(),
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
		Relation:  options.Relation,
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
	NodeID     string
	NodePrefix string
	EdgeID     string
	Relation   string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func (n *ItemRef) DynamoGraphNodeID() string {
	return n.NodeID
}

func (n *ItemRef) DynamoGraphNodePrefix() string {
	return n.NodePrefix
}

func (n *ItemRef) DynamoGraphRelation() string {
	return n.Relation
}

func (n *ItemRef) DynamoGraphMarshal(createdAt, updatedAt time.Time) error {
	n.CreatedAt = createdAt
	n.UpdatedAt = updatedAt
	return nil
}

func (n *ItemRef) DynamoGraphUnmarshal(createdAt, updatedAt time.Time) error {
	n.CreatedAt = createdAt
	n.UpdatedAt = updatedAt
	return nil
}

func (n *ItemRef) DynamoGraphRelationships() map[string][]Node {
	return nil
}

func (e Item[T]) ref(node Node, relation string) Item[*ItemRef] {
	ref := &ItemRef{
		NodeID:     e.Data.DynamoGraphNodeID(),
		NodePrefix: e.Data.DynamoGraphNodePrefix(),
		Relation:   relation,
	}
	return NewItem(ref, func(eo *EdgeOptions) {
		eo.SortID = node.DynamoGraphNodeID()
		eo.SortPrefix = node.DynamoGraphNodePrefix()
		eo.SupportsReverseLookupIndex = true
		eo.SupportsCollectionIndex = false
	})
}

func (e Item[T]) Refs() []Item[*ItemRef] {
	relationships := e.Data.DynamoGraphRelationships()
	if len(relationships) == 0 {
		return nil
	}

	refs := make([]Item[*ItemRef], 0, len(relationships))
	for name, nodes := range relationships {
		for _, node := range nodes {
			refs = append(refs, e.ref(node, name))
		}
	}
	return refs
}

func (e Item[T]) IsNode() bool {
	return e.PK == e.SK
}

func (e Item[T]) EdgeID() (string, string) {
	return e.PK, e.SK
}

func (e Item[T]) TableRowKey() ezddb.TableRow {
	return tableRowKey(e.PK, e.SK)
}

func (e *Item[T]) Marshal(m ezddb.RowMarshaler) (ezddb.TableRow, error) {
	err := e.Data.DynamoGraphMarshal(e.CreatedAt, e.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return m(e)
}

func (e *Item[T]) Unmarshal(r ezddb.TableRow, u ezddb.RowUnmarshaler) error {
	err := u(r, e)
	if err != nil {
		return err
	}
	return e.Data.DynamoGraphUnmarshal(e.CreatedAt, e.UpdatedAt)
}

type EdgeVisitor interface {
	VisitEdge(ctx context.Context, row ezddb.TableRow) error
}

type EdgeVisitorFunc func(ctx context.Context, row ezddb.TableRow) error

func (f EdgeVisitorFunc) VisitEdge(ctx context.Context, row ezddb.TableRow) error {
	return f(ctx, row)
}

type NodeVisitor struct {
	mux map[string]EdgeVisitor
}

func NewNodeVisitor() *NodeVisitor {
	return &NodeVisitor{
		mux: make(map[string]EdgeVisitor),
	}
}

func (v *NodeVisitor) AddRelation(relation string, visitor EdgeVisitor) *NodeVisitor {
	v.mux[relation] = visitor
	return v
}

func (v NodeVisitor) VisitEdge(ctx context.Context, row ezddb.TableRow) error {
	relation := row[AttributeRelation].(*types.AttributeValueMemberS).Value
	visitor, ok := v.mux[relation]
	if !ok {
		return fmt.Errorf("no visitor found for relation %s", relation)
	}
	return visitor.VisitEdge(ctx, row)
}

type ConditionFilterProvider interface {
	FilterCondition(ctx context.Context, base expression.ConditionBuilder) expression.ConditionBuilder
}

type DynamoClient interface {
	ezddb.Querier
	ezddb.Putter
	ezddb.Getter
	ezddb.BatchWriter
	ezddb.BatchGetter
}

type Options struct {
	ezddb.StartKeyTokenProvider
	ezddb.StartKeyProvider
	TableName                string
	CollectionQueryIndexName string
	ReverseLookupIndexName   string
	BuildExpression          ExpressionBuilder
	MarshalEdge              ezddb.RowMarshaler
	UnmarshalEdge            ezddb.RowUnmarshaler
}

type OptionsFunc = func(*Options)

func (o *Options) apply(opts ...OptionsFunc) {
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
		MarshalEdge:              attributevalue.MarshalMap,
		UnmarshalEdge:            attributevalue.UnmarshalMap,
	}
	options.apply(opts...)
	return &Graph{Options: options}
}

type QueryNodesInput struct {
	relation          string
	PageCursor        string
	Limit             Limit
	CreatedAfterDate  time.Time
	CreatedBeforeDate time.Time
	Filter            ConditionFilterProvider
	SortAscending     bool
}

func QueryNodes(ctx context.Context, g Graph, input QueryNodesInput, opts ...OptionsFunc) ezddb.QueryOperation {
	g.Options.apply(opts...)

	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		expressionBuilder := expression.NewBuilder().
			WithKeyCondition(edgesCreatedBetween(input.relation, input.CreatedAfterDate, input.CreatedBeforeDate))

		condition := expression.ConditionBuilder{}
		if input.Filter != nil {
			condition = input.Filter.FilterCondition(ctx, condition)
		}

		if condition.IsSet() {
			expressionBuilder = expressionBuilder.WithFilter(condition)
		}

		expr, err := g.BuildExpression(expressionBuilder)
		if err != nil {
			return nil, fmt.Errorf("build expression failed: %w", err)
		}

		startKey, err := ezddb.GetStartKey(ctx, g, input.PageCursor)
		if err != nil {
			return nil, fmt.Errorf("start key failed: %w", err)
		}

		return &dynamodb.QueryInput{
			TableName:                 &g.TableName,
			IndexName:                 &g.CollectionQueryIndexName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         startKey,
			Limit:                     input.Limit.Value(),
			ScanIndexForward:          aws.Bool(input.SortAscending),
		}, nil
	}
}

func VisitNodes[T Node](ctx context.Context, g Graph, output *dynamodb.QueryOutput) ([]T, string, error) {
	nextToken, err := ezddb.GetStartKeyToken(ctx, g, output.LastEvaluatedKey)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get start key token: %w", err)
	}

	nodes := make([]T, 0, len(output.Items))
	for _, item := range output.Items {
		node := Item[T]{}
		err := node.Unmarshal(item, g.UnmarshalEdge)
		if err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal node: %w", err)
		}
		nodes = append(nodes, node.Data)
	}

	return nodes, nextToken, nil
}

type QueryEdgesInput struct {
	NodeID        string
	EdgePrefix    string
	ReverseLookup bool
	Limit         Limit
	PageCursor    string
	Filter        ConditionFilterProvider
}

func QueryNode(ctx context.Context, g Graph, input QueryEdgesInput, opts ...OptionsFunc) ezddb.QueryOperation {
	g.Options.apply(opts...)
	return func(ctx context.Context) (*dynamodb.QueryInput, error) {
		expressionBuilder := expression.NewBuilder().
			WithKeyCondition(nodeEdgesWithPrefix(input.NodeID, input.EdgePrefix))

		if input.ReverseLookup {
			expressionBuilder = expressionBuilder.WithKeyCondition(
				reverseNodeEdgesWithPrefix(input.NodeID, input.EdgePrefix))
		}

		condition := expression.ConditionBuilder{}
		if input.Filter != nil {
			condition = input.Filter.FilterCondition(ctx, condition)
		}

		if condition.IsSet() {
			expressionBuilder = expressionBuilder.WithFilter(condition)
		}

		expr, err := g.BuildExpression(expressionBuilder)
		if err != nil {
			return nil, fmt.Errorf("build expression failed: %w", err)
		}

		startKey, err := ezddb.GetStartKey(ctx, g, input.PageCursor)
		if err != nil {
			return nil, fmt.Errorf("start key failed: %w", err)
		}

		return &dynamodb.QueryInput{
			TableName:                 &g.TableName,
			KeyConditionExpression:    expr.KeyCondition(),
			FilterExpression:          expr.Filter(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			ExclusiveStartKey:         startKey,
			Limit:                     input.Limit.Value(),
		}, nil
	}
}

func VisitNode(ctx context.Context, g Graph, visitor EdgeVisitor, output *dynamodb.QueryOutput) (string, error) {
	nextToken, err := ezddb.GetStartKeyToken(ctx, g, output.LastEvaluatedKey)
	if err != nil {
		return "", fmt.Errorf("failed to get start key token: %w", err)
	}
	for _, item := range output.Items {
		err := visitor.VisitEdge(ctx, item)
		if err != nil {
			return "", fmt.Errorf("visit edge failed: %w", err)
		}
	}
	return nextToken, nil
}

type PuttableEdge interface {
	Marshal(ezddb.RowMarshaler) (ezddb.TableRow, error)
}

func PutEdge(g Graph, edge PuttableEdge) ezddb.PutOperation {
	return func(ctx context.Context) (*dynamodb.PutItemInput, error) {
		item, err := edge.Marshal(g.MarshalEdge)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal edge: %w", err)
		}
		return &dynamodb.PutItemInput{
			TableName: &g.TableName,
			Item:      item,
		}, nil
	}
}

func GetEdge(g Graph, id EdgeIdentifier) ezddb.GetOperation {
	return func(ctx context.Context) (*dynamodb.GetItemInput, error) {
		pk, sk := id.EdgeID()
		return &dynamodb.GetItemInput{
			TableName: &g.TableName,
			Key:       tableRowKey(pk, sk),
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

func tableRowKey(pk, sk string) ezddb.TableRow {
	return ezddb.TableRow{
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
